from fastapi import FastAPI, Query, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import sqlite3
import pandas as pd
import os
import json
from datetime import datetime, timedelta, timezone
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo
import math
import httpx
import asyncio
import time
from urllib.parse import parse_qs


# IMPORTACIONES DEL BRIDGE (Inyectadas en el droplet)
try:
    from bridge import persist, _series_from_sqlite, _iter_archive_chunks, _ungzip_to_cache
except ImportError:
    # Fallback para desarrollo local
    def persist(channel, device, data): return True
    def _series_from_sqlite(*args, **kwargs): return ([], 0, 0)
    def _iter_archive_chunks(s, e): return []
    def _ungzip_to_cache(p): return p

app = FastAPI(title="ADTEC Cloud Bridge API")

DATA_DIR = os.environ.get("DATA_DIR", "data")
DB_FILE = os.path.join(DATA_DIR, "telemetry.db")
ARCHIVE_DIR = os.environ.get("ARCHIVE_DIR", "archive")
DEFAULT_DEVICE = "TEST-EMU"
DEFAULT_CHANNEL = "ingreso"
PY_TZ = ZoneInfo("America/Asuncion")
LIVE_RETENTION_DAYS = 30


# --- cache_api snapshot (uploaded from Bridge) ---
CACHE_API_DIR = os.environ.get("CACHE_API_DIR", os.path.join(DATA_DIR, "cache_api"))
CACHE_API_SNAPSHOT_GZ = os.path.join(CACHE_API_DIR, "snapshot.json.gz")
CACHE_API_TMP_GZ = CACHE_API_SNAPSHOT_GZ + ".tmp"
CACHE_API_TOKEN = os.environ.get("CACHE_API_TOKEN", "")
os.makedirs(CACHE_API_DIR, exist_ok=True)

@app.post("/cache_api/upload")
async def cache_api_upload(request: Request):
    # Optional auth: set CACHE_API_TOKEN in env and send X-Cache-Token header from Bridge
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
def cache_api_snapshot():
    if not os.path.exists(CACHE_API_SNAPSHOT_GZ):
        raise HTTPException(status_code=404, detail="snapshot not found")
    return FileResponse(
        CACHE_API_SNAPSHOT_GZ,
        media_type="application/gzip",
        filename="snapshot.json.gz",
    )
# Memoria volátil para respuesta instantánea
LAST_CACHE = {}

# Cache volátil para series (evita hits repetidos al Bridge al cambiar de variables/intervalos)
# key -> (t_mono, ttl_sec, status_code, content_bytes, headers_dict, media_type)
SERIES_CACHE = {}
SERIES_CACHE_MAX = int(os.environ.get('SERIES_CACHE_MAX', '500'))
SERIES_TTL_DEFAULT = float(os.environ.get('SERIES_TTL_DEFAULT', '60'))

def _cache_prune():
    # Mantener tamaño acotado (drop de los más viejos)
    if len(SERIES_CACHE) <= SERIES_CACHE_MAX:
        return
    items = sorted(SERIES_CACHE.items(), key=lambda kv: kv[1][0])  # por t_mono
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
    # TTL más largo para rangos amplios; más corto para rangos pequeños.
    try:
        q = parse_qs(qs or '')
        from_ts = int(q.get('from_ts', [0])[0]) if 'from_ts' in q else None
        to_ts = int(q.get('to_ts', [0])[0]) if 'to_ts' in q else None
        if from_ts is not None and to_ts is not None and to_ts >= from_ts:
            span = to_ts - from_ts
            if span >= 7 * 86400:
                return 180.0
            if span >= 86400:
                return 120.0
            if span >= 6 * 3600:
                return 90.0
            if span >= 3600:
                return 60.0
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

def clean_for_json(obj):
    if isinstance(obj, list): return [clean_for_json(x) for x in obj]
    if isinstance(obj, dict): return {k: clean_for_json(v) for k, v in obj.items()}
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj): return 0.0
    return obj

# HELPERS: filtrar headers hop-by-hop y no reenviar content-length
HOP_BY_HOP = {"connection","keep-alive","proxy-authenticate","proxy-authorization","te","trailers","transfer-encoding","upgrade"}

def _filter_upstream_headers(hdrs):
    return {k: v for k, v in hdrs.items() if k.lower() not in HOP_BY_HOP and k.lower() != "content-length"}

# 1) ESTADO DE CONTROL Y ALERTAS
CONTROL_STATE = {
    "manual": False,
        "relay_1": False,
    "relay_2": False,
    "vfd_1": 0.0,
    "vfd_1_state": False,
    "relay_3": False
}

# Inicialización por compatibilidad
if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)

# Servir archivos estáticos (librerías)
if os.path.exists("libs"):
    app.mount("/libs", StaticFiles(directory="libs"), name="libs")

@app.get("/", response_class=HTMLResponse)
async def index():
    if os.path.exists("index.html"):
        with open("index.html", "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>ADTEC Bridge API</h1>")

# 2) /api/ingreso (Normalizado según especificación Natasha)
@app.post("/api/ingreso")
async def api_ingreso(payload: Dict[str, Any]):
    global LAST_CACHE
    device = payload.get("device", DEFAULT_DEVICE)
    data_obj = payload.get("data", {})
    if not isinstance(data_obj, dict): data_obj = {}

    # Normalizar Timestamp a UTC
    ts_raw = data_obj.get("timestamp", "AUTO")
    if ts_raw == "AUTO":
        dt_utc = datetime.now(timezone.utc)
    else:
        try:
            dt = datetime.fromisoformat(ts_raw.replace("Z", ""))
            dt_utc = dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=PY_TZ).astimezone(timezone.utc)
        except:
            dt_utc = datetime.now(timezone.utc)

    # Construir Payload Final para el Bridge (ts, ts_iso, kv)
    bridge_payload = {
        "ts": int(dt_utc.timestamp()),
        "ts_iso": dt_utc.isoformat().replace("+00:00", "Z"),
        "kv": {}
    }
    
    # El ESP32 ya envía las keys con kv., las pasamos directo al objeto kv
    kv_data = data_obj.get("kv", {})
    if not isinstance(kv_data, dict): kv_data = {}
    
    for k, v in kv_data.items():
        # Validar prefijo kv. (por seguridad)
        key = k if k.startswith("kv.") else f"kv.{k}"
        try:
            val = float(v)
            if math.isnan(val) or math.isinf(val): val = 0.0
        except:
            val = str(v)
        bridge_payload["kv"][key] = val

    # Delegar al Bridge
    success = persist(DEFAULT_CHANNEL, device, bridge_payload)
    if success:
        # Actualizar caché con el formato que espera el Dashboard
        # El Dashboard prefiere ver las claves en la raíz del objeto para compatibilidad
        cache_data = {"timestamp": bridge_payload["ts_iso"]}
        cache_data.update(bridge_payload["kv"])
        LAST_CACHE[DEFAULT_CHANNEL] = cache_data
    else:
        raise HTTPException(status_code=500, detail="Error al persistir en bridge")
        
    return {"status": "ok"}

@app.get("/api/last")
async def api_last(request: Request):
    url = f"{BRIDGE_HTTP_BASE}/api/last"
    qs = str(request.url.query)
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(f"{url}?{qs}" if qs else url)
    headers = _filter_upstream_headers(r.headers)
    media_type = r.headers.get("content-type")
    return Response(content=r.content, status_code=r.status_code, headers=headers, media_type=media_type)

# 2b) /api/data (PROXY: Dashboard HTTPS -> API HTTPS -> BRIDGE HTTP)
# Inserted bridge host at generation time to avoid undefined Python variable
BRIDGE_HTTP_BASE = os.getenv("BRIDGE_HTTP_BASE", "http://161.35.129.132")

@app.get("/api/data")
async def api_data(request: Request):
    url = f"{BRIDGE_HTTP_BASE}/api/data"
    qs = str(request.url.query)
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(f"{url}?{qs}" if qs else url)
    headers = _filter_upstream_headers(r.headers)
    media_type = r.headers.get("content-type")
    return Response(content=r.content, status_code=r.status_code, headers=headers, media_type=media_type)


# 3) /api/series
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

    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(f"{url}?{qs}" if qs else url)

    headers = _filter_upstream_headers(r.headers)
    media_type = r.headers.get("content-type")
    _cache_set(cache_key, r.status_code, r.content, headers, media_type, ttl)
    return Response(content=r.content, status_code=r.status_code, headers=headers, media_type=media_type)


@app.get("/api/sparkline_series")
async def api_sparkline_series(
    keys: str = Query(..., description="Lista de keys separadas por coma"),
    hours: int = Query(24, ge=1, le=168),
    max_points: int = Query(120, ge=10, le=1000),
    channel: str = Query(DEFAULT_CHANNEL),
    device: str = Query(DEFAULT_DEVICE),
):
    """Devuelve series (downsample) para múltiples keys en un solo request.

    Pensado para precargar sparklines y evitar N requests al Bridge.
    """
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
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.get(url)

        try:
            js = r.json()
            pts = js.get("points") if isinstance(js, dict) else None
            pts = pts if isinstance(pts, list) else []
        except Exception:
            pts = []

        headers = _filter_upstream_headers(r.headers)
        media_type = r.headers.get("content-type")
        _cache_set(cache_key, r.status_code, r.content, headers, media_type, ttl)
        return k, pts

    results = await asyncio.gather(*[fetch_one(k) for k in key_list])
    series = {k: pts for (k, pts) in results}
    return JSONResponse(content={"channel": channel, "device": device, "hours": hours, "max_points": max_points, "series": series})



# 4) /api/download/xlsx_range
@app.get("/api/download/xlsx")
async def proxy_download_xlsx(request: Request):
    url = f"{BRIDGE_HTTP_BASE}/api/download/xlsx"
    qs = str(request.url.query)
    async with httpx.AsyncClient(timeout=None) as client:
        r = await client.get(f"{url}?{qs}" if qs else url)

    headers = _filter_upstream_headers(r.headers)
    if r.headers.get("content-disposition"):
        headers["content-disposition"] = r.headers.get("content-disposition")
    media_type = r.headers.get("content-type")
    return StreamingResponse(r.aiter_bytes(), status_code=r.status_code, headers=headers, media_type=media_type or "application/octet-stream")

@app.get("/api/download/xlsx_range")
async def proxy_download_xlsx_range(request: Request):
    url = f"{BRIDGE_HTTP_BASE}/api/download/xlsx_range"
    qs = str(request.url.query)
    async with httpx.AsyncClient(timeout=None) as client:
        r = await client.get(f"{url}?{qs}" if qs else url)

    headers = _filter_upstream_headers(r.headers)
    if r.headers.get("content-disposition"):
        headers["content-disposition"] = r.headers.get("content-disposition")
    media_type = r.headers.get("content-type")
    return StreamingResponse(r.aiter_bytes(), status_code=r.status_code, headers=headers, media_type=media_type or "application/octet-stream")


@app.post("/api/alerts")
async def api_alerts(request: Request):
    url = f"{BRIDGE_HTTP_BASE}/api/alerts"
    qs = str(request.url.query)
    body = await request.body()

    # reenviar content-type + headers de auditoría si vienen
    fwd_headers = {}
    if "content-type" in request.headers:
        fwd_headers["content-type"] = request.headers["content-type"]
    if "x-actor-id" in request.headers:
        fwd_headers["x-actor-id"] = request.headers["x-actor-id"]
    if "x-session-id" in request.headers:
        fwd_headers["x-session-id"] = request.headers["x-session-id"]
    if "x-forwarded-for" in request.headers:
        fwd_headers["x-forwarded-for"] = request.headers["x-forwarded-for"]

    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(f"{url}?{qs}" if qs else url, content=body, headers=fwd_headers)

    headers = _filter_upstream_headers(r.headers)
    media_type = r.headers.get("content-type")
    return Response(content=r.content, status_code=r.status_code, headers=headers, media_type=media_type)


@app.get("/api/alerts")
async def get_alerts(request: Request):
    url = f"{BRIDGE_HTTP_BASE}/api/alerts"
    qs = str(request.url.query)

    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(f"{url}?{qs}" if qs else url)

    headers = _filter_upstream_headers(r.headers)
    media_type = r.headers.get("content-type")
    return Response(content=r.content, status_code=r.status_code, headers=headers, media_type=media_type)



@app.get("/api/control_state")
async def get_control_state(request: Request):
    url = f"{BRIDGE_HTTP_BASE}/api/control_state"
    qs = str(request.url.query)

    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(f"{url}?{qs}" if qs else url)

    headers = _filter_upstream_headers(r.headers)
    media_type = r.headers.get("content-type")
    return Response(content=r.content, status_code=r.status_code, headers=headers, media_type=media_type)


@app.post("/api/control_state")
async def update_control_state(request: Request):
    url = f"{BRIDGE_HTTP_BASE}/api/control_state"
    qs = str(request.url.query)
    body = await request.body()

    # reenviar content-type + headers de auditoría si vienen
    fwd_headers = {}
    if "content-type" in request.headers:
        fwd_headers["content-type"] = request.headers["content-type"]
    if "x-actor-id" in request.headers:
        fwd_headers["x-actor-id"] = request.headers["x-actor-id"]
    if "x-session-id" in request.headers:
        fwd_headers["x-session-id"] = request.headers["x-session-id"]
    if "x-forwarded-for" in request.headers:
        fwd_headers["x-forwarded-for"] = request.headers["x-forwarded-for"]

    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(f"{url}?{qs}" if qs else url, content=body, headers=fwd_headers)

    headers = _filter_upstream_headers(r.headers)
    media_type = r.headers.get("content-type")
    return Response(content=r.content, status_code=r.status_code, headers=headers, media_type=media_type)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
