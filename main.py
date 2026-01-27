from fastapi import FastAPI, Query, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
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

# Memoria volátil para respuesta instantánea
LAST_CACHE = {}

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
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(f"{url}?{qs}" if qs else url)
    headers = _filter_upstream_headers(r.headers)
    media_type = r.headers.get("content-type")
    return Response(content=r.content, status_code=r.status_code, headers=headers, media_type=media_type)

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
