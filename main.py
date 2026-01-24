from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
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
async def api_last():
    if DEFAULT_CHANNEL in LAST_CACHE:
        return clean_for_json(LAST_CACHE[DEFAULT_CHANNEL])
    
    # Fallback a DB
    conn = sqlite3.connect(DB_FILE)
    try:
        c = conn.cursor()
        c.execute("SELECT ts_iso, payload_json FROM events WHERE channel=? ORDER BY ts DESC LIMIT 1", (DEFAULT_CHANNEL,))
        res = c.fetchone()
        if res:
            raw_payload = json.loads(res[1])
            # Normalizar para el Dashboard
            data = {"timestamp": res[0]}
            if "kv" in raw_payload:
                data.update(raw_payload["kv"])
            else:
                data.update(raw_payload)
            LAST_CACHE[DEFAULT_CHANNEL] = data
            return clean_for_json(data)
    except: pass
    finally: conn.close()
    return {}

# 2b) /api/data (PROXY: Dashboard HTTPS -> API HTTPS -> BRIDGE HTTP)
BRIDGE_HTTP_BASE = os.getenv("BRIDGE_HTTP_BASE", f"http://{bridgeHost}")

@app.get("/api/data")
async def api_data(request: Request):
    url = f"{BRIDGE_HTTP_BASE}/api/data"
    qs = str(request.url.query)

    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(f"{url}?{qs}" if qs else url)

    # Si el bridge responde error/no-json, no romper
    try:
        data = r.json()
    except Exception:
        data = {"detail": r.text}

    return JSONResponse(content=data, status_code=r.status_code)


# 3) /api/series
@app.get("/api/series")
async def api_series(
    channel: str = DEFAULT_CHANNEL,
    key: str = Query(...),
    from_ts: Optional[int] = None,
    to_ts: Optional[int] = None,
    from_iso: Optional[str] = None,
    to_iso: Optional[str] = None,
    source: str = "auto",
    max_points: int = Query(1000, ge=1000, le=25000)
):
    if not key.startswith("kv."): key = f"kv.{key}"

    now_ts = int(datetime.now(timezone.utc).timestamp())
    if to_iso: 
        try: to_ts = int(datetime.fromisoformat(to_iso.replace("Z", "").split("+")[0]).replace(tzinfo=timezone.utc).timestamp())
        except: pass
    if from_iso:
        try: from_ts = int(datetime.fromisoformat(from_iso.replace("Z", "").split("+")[0]).replace(tzinfo=timezone.utc).timestamp())
        except: pass
    
    if to_ts is None: to_ts = now_ts
    if from_ts is None: from_ts = to_ts - 86400

    if from_ts >= to_ts: raise HTTPException(status_code=400, detail="Rango inválido")

    # Decidir origen
    if source == "auto":
        source = "live" if (to_ts >= (now_ts - LIVE_RETENTION_DAYS * 86400)) else "archive"

    # Llamada al Bridge (Devuelve tupla: pts, I, n_in)
    pts, I, n_in = _series_from_sqlite(DB_FILE if source == "live" else "AUTO", channel, key, from_ts, to_ts, max_points)
    
    # Adaptar para el Dashboard (inyectar key en los puntos)
    for p in pts:
        p[key] = p.get("avg")

    return {
        "source": source,
        "channel": channel,
        "key": key,
        "from_ts": from_ts,
        "to_ts": to_ts,
        "bucket_I_sec": I,
        "points_in": n_in,
        "points_out": len(pts),
        "points": pts
    }

# 4) /api/download/xlsx_range
@app.get("/api/download/xlsx_range")
async def download_xlsx_range(
    channel: str = DEFAULT_CHANNEL,
    from_ts: int = Query(...),
    to_ts: int = Query(...)
):
    chunks = _iter_archive_chunks(from_ts, to_ts)
    rows = []
    
    def process_rows(cursor_rows):
        for r in cursor_rows:
            try:
                d = json.loads(r[2])
                payload = d.get("kv", d)
                payload["timestamp"] = r[0]
                payload["_device"] = r[1]
                rows.append(payload)
            except: pass

    # Cargar de Live
    conn = sqlite3.connect(DB_FILE)
    try:
        cur = conn.execute(f"SELECT ts_iso, device, payload_json FROM events WHERE channel='{channel}' AND ts BETWEEN {from_ts} AND {to_ts}")
        process_rows(cur.fetchall())
    except: pass
    finally: conn.close()
    
    # Cargar de Archive
    for path in chunks:
        tmp_db = _ungzip_to_cache(path)
        if tmp_db:
            try:
                conn_tmp = sqlite3.connect(tmp_db)
                cur = conn_tmp.execute(f"SELECT ts_iso, device, payload_json FROM events WHERE channel='{channel}' AND ts BETWEEN {from_ts} AND {to_ts}")
                process_rows(cur.fetchall())
                conn_tmp.close()
            except: pass
            
    if not rows: return JSONResponse({"status": "empty", "message": "Sin datos"}, status_code=404)

    df_total = pd.DataFrame(rows)
    df_total['timestamp'] = pd.to_datetime(df_total['timestamp']).dt.tz_localize('UTC').dt.tz_convert(PY_TZ)
    
    output = "reporte_rango.xlsx"
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_total['month'] = df_total['timestamp'].dt.strftime('%Y-%m')
        for month, group in df_total.groupby('month'):
            group.drop(columns=['month']).to_excel(writer, sheet_name=month, index=False)
            
    return FileResponse(output, filename=output)

@app.get("/api/download/xlsx")
async def download_xlsx(channel: str = DEFAULT_CHANNEL, limit: int = 5000):
    conn = sqlite3.connect(DB_FILE)
    try:
        query = f"SELECT ts_iso, device, payload_json FROM events WHERE channel='{channel}' ORDER BY ts DESC LIMIT {limit}"
        df_raw = pd.read_sql_query(query, conn)
        if df_raw.empty: return JSONResponse({"status": "empty", "message": "Sin datos"}, status_code=404)
        
        rows = []
        for _, r in df_raw.iterrows():
            d = json.loads(r['payload_json'])
            payload = d.get("kv", d)
            payload["timestamp"] = r['ts_iso']
            payload["_device"] = r['device']
            rows.append(payload)
            
        df = pd.DataFrame(rows)
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize('UTC').dt.tz_convert(PY_TZ)
        
        output = f"reporte_reciente_{channel}.xlsx"
        df.to_excel(output, index=False)
        return FileResponse(output, filename=output)
    finally: conn.close()

@app.post("/api/alerts")
async def api_alerts(payload: Dict[str, Any]):
    device = payload.get("device", DEFAULT_DEVICE)
    alert_data = {
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "msg": payload.get("msg") or payload.get("mensaje", ""),
        "type": payload.get("type") or "CRITICAL",
        "value": payload.get("value") or 0,
        "signal": payload.get("signal", ""),
        "extra": payload.get("extra", {})
    }
    success = persist("alerts", device, alert_data)
    return {"status": "ok" if success else "error"}

@app.get("/api/alerts")
async def get_alerts(limit: int = 100):
    conn = sqlite3.connect(DB_FILE)
    try:
        df = pd.read_sql_query(f"SELECT payload_json FROM events WHERE channel='alerts' ORDER BY ts DESC LIMIT {limit}", conn)
        alerts = [json.loads(r['payload_json']) for _, r in df.iterrows()]
        return clean_for_json(alerts)
    except: return []
    finally: conn.close()

@app.get("/api/control_state")
async def get_control_state(format: Optional[str] = None):
    conn = sqlite3.connect(DB_FILE)
    try:
        c = conn.cursor()
        c.execute("SELECT state_json FROM control_state WHERE id=1")
        res = c.fetchone()
        state = json.loads(res[0]) if res else {"manual": False}
        if format == "esp32":
            parts = [f"MANUAL:{'ON' if state.get('manual') else 'OFF'}"]
            for k, v in state.items():
                if k == "manual": continue
                val = "ON" if v is True else "OFF" if v is False else str(v)
                parts.append(f"{k}:{val}")
            return HTMLResponse(",".join(parts))
        return state
    except: return {"manual": False}
    finally: conn.close()

@app.post("/api/control_state")
async def update_control_state(payload: Dict[str, Any]):
    conn = sqlite3.connect(DB_FILE)
    try:
        c = conn.cursor()
        c.execute("SELECT state_json FROM control_state WHERE id=1")
        res = c.fetchone()
        state = json.loads(res[0]) if res else {"manual": False}
        state.update(payload)
        c.execute("UPDATE control_state SET state_json=?, updated_ts=?, updated_iso=? WHERE id=1", 
                 (json.dumps(state), int(datetime.now(timezone.utc).timestamp()), datetime.now(timezone.utc).isoformat()))
        conn.commit()
        return state
    finally: conn.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
