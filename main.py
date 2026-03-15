from fastapi import FastAPI, Query, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from typing import Dict, Any
import os
import json
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo
import math
import httpx
import asyncio
import time
from urllib.parse import parse_qs
from email.utils import formatdate, parsedate_to_datetime

# IMPORTACIONES DEL BRIDGE (Inyectadas en el droplet)
try:
    from bridge import persist, _series_from_sqlite, _iter_archive_chunks, _ungzip_to_cache
except ImportError:
    # Fallback para desarrollo local
    def persist(channel, device, data): return True
    def _series_from_sqlite(*args, **kwargs): return ([], 0, 0)
    def _iter_archive_chunks(s, e): return []
    def _ungzip_to_cache(p): return p

try:
    from invernIA import register_invernia_routes
except ImportError:
    def register_invernia_routes(app):
        return None

app = FastAPI(title="ADTEC Bridge API (Desktop+Mobile Unified)")

# Registrar rutas de InvernIA si está disponible
register_invernia_routes(app)

DATA_DIR = os.environ.get("DATA_DIR", "data")
DB_FILE = os.path.join(DATA_DIR, "telemetry.db")
ARCHIVE_DIR = os.environ.get("ARCHIVE_DIR", "archive")
DEFAULT_DEVICE = "TEST-EMU"
DEFAULT_CHANNEL = "ingreso"
PY_TZ = ZoneInfo("America/Asuncion")  # UTC-3
LIVE_RETENTION_DAYS = 30

# === UPSTREAM (Bridge HTTP) ===
# App Platform -> Bridge (droplet) por HTTP (SIM800L no acepta https)
BRIDGE_HTTP_BASE = os.getenv("BRIDGE_HTTP_BASE", "http://161.35.129.132")

# --- cache_api snapshot (uploaded from Bridge) ---
CACHE_API_DIR = os.environ.get("CACHE_API_DIR", os.path.join(DATA_DIR, "cache_api"))
CACHE_API_SNAPSHOT_GZ = os.path.join(CACHE_API_DIR, "snapshot.json.gz")
CACHE_API_TMP_GZ = CACHE_API_SNAPSHOT_GZ + ".tmp"
CACHE_API_TOKEN = os.environ.get("CACHE_API_TOKEN", "")
os.makedirs(CACHE_API_DIR, exist_ok=True)

@app.post("/cache_api/upload")
async def cache_api_upload(request: Request):
    if CACHE_API_TOKEN:
        if request.headers.get("X-Cache-Token") != CACHE_API_TOKEN:
            raise HTTPException(status_code=401, detail="bad token")
    body = await request.body()
    if not body or len(body) < 20:
        raise HTTPException(status_code=400, detail="empty payload")

    with open(CACHE_API_TMP_GZ, "wb") as f:
        f.write(body)
        f.flush()
        os.fsync(f.fileno())
    os.replace(CACHE_API_TMP_GZ, CACHE_API_SNAPSHOT_GZ)
    return {"status": "ok", "bytes": len(body)}

@app.get("/cache_api/snapshot.json.gz")
def cache_api_snapshot(request: Request):
    if not os.path.exists(CACHE_API_SNAPSHOT_GZ):
        raise HTTPException(status_code=404, detail="snapshot not found")

    st = os.stat(CACHE_API_SNAPSHOT_GZ)
    etag = f'"{st.st_mtime_ns:x}-{st.st_size:x}"'
    last_mod = formatdate(st.st_mtime, usegmt=True)

    inm = request.headers.get("if-none-match")
    ims = request.headers.get("if-modified-since")

    if inm and inm == etag:
        return Response(status_code=304, headers={"ETag": etag, "Last-Modified": last_mod})

    if ims:
        try:
            dt = parsedate_to_datetime(ims)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if st.st_mtime <= dt.timestamp():
                return Response(status_code=304, headers={"ETag": etag, "Last-Modified": last_mod})
        except Exception:
            pass

    return FileResponse(
        CACHE_API_SNAPSHOT_GZ,
        media_type="application/gzip",
        filename="snapshot.json.gz",
        headers={
            "ETag": etag,
            "Last-Modified": last_mod,
            "Cache-Control": "no-cache",
        },
    )

# Memoria volátil para respuesta instantánea
LAST_CACHE = {}

# Cache volátil para series (evita hits repetidos al Bridge al cambiar de variables/intervalos)
# key -> (t_mono, ttl_sec, status_code, content_bytes, headers_dict, media_type)
SERIES_CACHE = {}
SERIES_CACHE_MAX = int(os.environ.get('SERIES_CACHE_MAX', '500'))
SERIES_TTL_DEFAULT = float(os.environ.get('SERIES_TTL_DEFAULT', '60'))

def _cache_prune():
    if len(SERIES_CACHE) <= SERIES_CACHE_MAX:
        return
    items = sorted(SERIES_CACHE.items(), key=lambda kv: kv[1][0])
    for k, _v in items[: max(1, len(items) - SERIES_CACHE_MAX)]:
        SERIES_CACHE.pop(k, None)

def _cache_get(key: str):
    it = SERIES_CACHE.get(key)
    if not it:
        return None
    t_mono, ttl, status_code, content, headers, media_type = it
    if (time.monotonic() - t_mono) > ttl:
        SERIES_CACHE.pop(key, None)
        return None
    return status_code, content, headers, media_type

def _cache_set(key: str, status_code: int, content: bytes, headers: dict, media_type: str, ttl: float):
    SERIES_CACHE[key] = (time.monotonic(), float(ttl), int(status_code), content, headers, media_type)
    _cache_prune()

def _series_ttl_from_qs(qs: str) -> float:
    try:
        q = parse_qs(qs or '')
        from_ts = int(q.get('from_ts', [0])[0]) if 'from_ts' in q else None
        to_ts = int(q.get('to_ts', [0])[0]) if 'to_ts' in q else None
        if from_ts is not None and to_ts is not None and to_ts >= from_ts:
            span = to_ts - from_ts
            if span >= 7 * 86400: return 180.0
            if span >= 86400: return 120.0
            if span >= 6 * 3600: return 90.0
            if span >= 3600: return 60.0
            return 15.0
    except Exception:
        pass
    return SERIES_TTL_DEFAULT

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Servir archivos estáticos (si existen)
if os.path.exists("libs"):
    app.mount("/libs", StaticFiles(directory="libs"), name="libs")

# === Proxy robusto (stream) ===
HOP_BY_HOP = {
    "connection","keep-alive","proxy-authenticate","proxy-authorization",
    "te","trailers","transfer-encoding","upgrade"
}

async def proxy_stream(request: Request, url: str, inject_mobile: bool = False):
    # forward headers (sin hop-by-hop, sin host/content-length)
    headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in HOP_BY_HOP and k.lower() not in ("host", "content-length")
    }

    # si querés forzar marca desde server (opcional)
    if inject_mobile and "x-client" not in {k.lower() for k in headers.keys()}:
        headers["X-Client"] = "mobile"

    body = await request.body() if request.method in ("POST", "PUT", "PATCH") else None

    async with httpx.AsyncClient(timeout=None, follow_redirects=True) as client:
        upstream_stream = client.stream(request.method, url, headers=headers, content=body)
        resp = await upstream_stream.__aenter__()

        resp_headers = {
            k: v for k, v in resp.headers.items()
            if k.lower() not in HOP_BY_HOP and k.lower() != "content-length"
        }

        async def content_generator():
            try:
                async for chunk in resp.aiter_raw():
                    yield chunk
            finally:
                await upstream_stream.__aexit__(None, None, None)

        return StreamingResponse(
            content_generator(),
            status_code=resp.status_code,
            headers=resp_headers,
            media_type=resp.headers.get("content-type"),
        )

# === UI routing (Desktop + Mobile) ===
def _is_mobile_ua(ua: str) -> bool:
    u = (ua or "").lower()
    # heurística simple (suficiente para routing)
    return any(x in u for x in ["mobile", "android", "iphone", "ipad", "ipod", "opera mini", "iemobile"])

async def _serve_html(path: str, title: str):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content=f"<h1>{title}</h1><p>No existe: {path}</p>", status_code=404)

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    # Auto: si es mobile -> mobile.html, si no -> index.html
    ua = request.headers.get("user-agent", "")
    if _is_mobile_ua(ua):
        return await _serve_html("mobile.html", "ADTEC Mobile UI")
    return await _serve_html("index.html", "ADTEC Desktop UI")

@app.get("/mobile", response_class=HTMLResponse)
async def mobile(request: Request):
    return await _serve_html("mobile.html", "ADTEC Mobile UI")

@app.get("/desktop", response_class=HTMLResponse)
async def desktop(request: Request):
    return await _serve_html("index.html", "ADTEC Desktop UI")

# 1) ESTADO DE CONTROL (placeholder tuyo)
CONTROL_STATE = {
    "manual": False,
        "relay_1": False,
    "relay_2": False,
    "relay_3": False
}

# 2) /api/ingreso (Normalizado según especificación)
@app.post("/api/ingreso")
async def api_ingreso(payload: Dict[str, Any]):
    global LAST_CACHE
    device = payload.get("device", DEFAULT_DEVICE)
    data_obj = payload.get("data", {})
    if not isinstance(data_obj, dict):
        data_obj = {}

    ts_raw = data_obj.get("timestamp", "AUTO")
    if ts_raw == "AUTO":
        dt_utc = datetime.now(timezone.utc)
    else:
        try:
            dt = datetime.fromisoformat(ts_raw.replace("Z", ""))
            dt_utc = dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=PY_TZ).astimezone(timezone.utc)
        except Exception:
            dt_utc = datetime.now(timezone.utc)

    bridge_payload = {
        "ts": int(dt_utc.timestamp()),
        "ts_iso": dt_utc.isoformat().replace("+00:00", "Z"),
        "kv": {}
    }

    kv_data = data_obj.get("kv", {})
    if not isinstance(kv_data, dict):
        kv_data = {}

    for k, v in kv_data.items():
        key = k if k.startswith("kv.") else f"kv.{k}"
        try:
            val = float(v)
            if math.isnan(val) or math.isinf(val):
                val = 0.0
        except Exception:
            val = str(v)
        bridge_payload["kv"][key] = val

    success = persist(DEFAULT_CHANNEL, device, bridge_payload)
    if not success:
        raise HTTPException(status_code=500, detail="Error al persistir en bridge")

    cache_data = {"timestamp": bridge_payload["ts_iso"]}
    cache_data.update(bridge_payload["kv"])
    LAST_CACHE[DEFAULT_CHANNEL] = cache_data

    return {"status": "ok"}

# === Proxies API (stream) ===
@app.api_route("/api/last", methods=["GET", "HEAD"])
async def api_last(request: Request):
    qs = str(request.url.query)
    url = f"{BRIDGE_HTTP_BASE}/api/last"
    full_url = f"{url}?{qs}" if qs else url
    return await proxy_stream(request, full_url)

@app.api_route("/api/data", methods=["GET", "HEAD"])
async def api_data(request: Request):
    qs = str(request.url.query)
    url = f"{BRIDGE_HTTP_BASE}/api/data"
    full_url = f"{url}?{qs}" if qs else url
    return await proxy_stream(request, full_url)

# /api/series mantiene cache existente (no streaming)
@app.get("/api/series")
async def api_series(request: Request):
    url = f"{BRIDGE_HTTP_BASE}/api/series"
    qs = str(request.url.query)
    cache_key = f"/api/series?{qs}" if qs else "/api/series"
    ttl = _series_ttl_from_qs(qs)

    hit = _cache_get(cache_key)
    if hit is not None:
        status_code, content, headers, media_type = hit
        return Response(content=content, status_code=status_code, headers=headers, media_type=media_type)

    async with httpx.AsyncClient(timeout=None, follow_redirects=True) as client:
        r = await client.get(f"{url}?{qs}" if qs else url)

    headers = {k: v for k, v in r.headers.items() if k.lower() not in HOP_BY_HOP and k.lower() != "content-length"}
    media_type = r.headers.get("content-type")
    _cache_set(cache_key, r.status_code, r.content, headers, media_type, ttl)
    return Response(content=r.content, status_code=r.status_code, headers=headers, media_type=media_type)

# Sparkline series (multi) - igual que antes (mantengo tu lógica)
@app.get("/api/sparkline_series")
async def api_sparkline_series(
    keys: str = Query(..., description="Lista de keys separadas por coma"),
    hours: int = Query(24, ge=1, le=168),
    max_points: int = Query(120, ge=10, le=1000),
    channel: str = Query(DEFAULT_CHANNEL),
    device: str = Query(DEFAULT_DEVICE),
):
    key_list = [k.strip() for k in (keys or "").split(",") if k.strip()]
    if not key_list:
        return JSONResponse(content={"channel": channel, "device": device, "hours": hours, "max_points": max_points, "series": {}})

    now = int(datetime.now(timezone.utc).timestamp())
    from_ts = now - int(hours) * 3600
    ttl = 60.0 if hours >= 24 else 20.0

    async def fetch_one(k: str):
        qs = f"channel={channel}&device={device}&key={k}&from_ts={from_ts}&to_ts={now}&max_points={max_points}"
        cache_key = f"/api/series?{qs}"
        hit = _cache_get(cache_key)
        if hit is not None:
            _status, content, _headers, _media = hit
            try:
                js = json.loads(content.decode("utf-8"))
                pts = js.get("points") if isinstance(js, dict) else None
                return k, (pts if isinstance(pts, list) else [])
            except Exception:
                pass

        url = f"{BRIDGE_HTTP_BASE}/api/series?{qs}"
        async with httpx.AsyncClient(timeout=None, follow_redirects=True) as client:
            r = await client.get(url)

        try:
            js = r.json()
            pts = js.get("points") if isinstance(js, dict) else None
            pts = pts if isinstance(pts, list) else []
        except Exception:
            pts = []

        headers = {k: v for k, v in r.headers.items() if k.lower() not in HOP_BY_HOP and k.lower() != "content-length"}
        media_type = r.headers.get("content-type")
        _cache_set(cache_key, r.status_code, r.content, headers, media_type, ttl)
        return k, pts

    results = await asyncio.gather(*[fetch_one(k) for k in key_list])
    series = {k: pts for (k, pts) in results}
    return JSONResponse(content={"channel": channel, "device": device, "hours": hours, "max_points": max_points, "series": series})

# Downloads (stream para no romper archivos grandes)
@app.api_route("/api/download/xlsx", methods=["GET", "HEAD"])
async def proxy_download_xlsx(request: Request):
    qs = str(request.url.query)
    url = f"{BRIDGE_HTTP_BASE}/api/download/xlsx"
    full_url = f"{url}?{qs}" if qs else url
    return await proxy_stream(request, full_url)

@app.api_route("/api/download/xlsx_range", methods=["GET", "HEAD"])
async def proxy_download_xlsx_range(request: Request):
    qs = str(request.url.query)
    url = f"{BRIDGE_HTTP_BASE}/api/download/xlsx_range"
    full_url = f"{url}?{qs}" if qs else url
    return await proxy_stream(request, full_url)

# Alerts (GET/POST) streaming
@app.api_route("/api/alerts", methods=["GET", "POST", "PUT", "HEAD"])
async def api_alerts_any(request: Request):
    qs = str(request.url.query)
    url = f"{BRIDGE_HTTP_BASE}/api/alerts"
    full_url = f"{url}?{qs}" if qs else url
    return await proxy_stream(request, full_url)

# Control state (GET/POST) streaming
@app.api_route("/api/control_state", methods=["GET", "POST", "PUT", "HEAD"])
async def api_control_state_any(request: Request):
    qs = str(request.url.query)
    url = f"{BRIDGE_HTTP_BASE}/api/control_state"
    full_url = f"{url}?{qs}" if qs else url

    # Si viene desde mobile, idealmente el JS manda X-Client: mobile.
    # Igual dejamos la opción de inyectar si el request ya es POST/PUT.
    inject_mobile = request.method in ("POST", "PUT")
    return await proxy_stream(request, full_url, inject_mobile=inject_mobile)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
