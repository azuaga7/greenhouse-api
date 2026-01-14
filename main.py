from fastapi import FastAPI, UploadFile, File, Query
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import pandas as pd
import os
import json
from datetime import datetime
import math

app = FastAPI(title="ADTEC Cloud Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

CSV_FILE = "telemetria.csv"
HISTORY = []
LAST_DATA = {}
FIELD_LABELS = {"dht22_1_HUM_OUT":"Humedad del Invernadero","dht22_1_TEMP_OUT":"Temperatura del Invernadero","ds18b20_2_TEMP_OUT":"Temperatura Exterior","relay_3_STATE_OUT":"Estado Bomba de Agua","relay_1_STATE_OUT":"Estado Vent. 1","relay_2_STATE_OUT":"Estado Vent. 2","vfd_1_FREQ_OUT":"Frecuencia Ventiladores Pared","relay_3_RUNTIME_OUT":"Uso Bomba de Agua","relay_1_RUNTIME_OUT":"Uso Ventiladores 1 - 2","relay_2_RUNTIME_OUT":"Uso Ventiladores 3 - 4","vfd_1_STATE_OUT":"Estado de Ventiladores Axiales","vfd_1_RUNTIME_OUT":"Uso Ventiladores Axiales","ds18b20_1_TEMP_OUT":"Temperatura de Pozo","tsl2561_1_LUX_OUT":"Luxes","gsm_1_STATE_OUT":"State Output"}

# Servir archivos estáticos (librerías)
if os.path.exists("libs"):
    app.mount("/libs", StaticFiles(directory="libs"), name="libs")

def clean_for_json(obj):
    """Limpia recursivamente NaN de un objeto para que sea JSON compliant"""
    if isinstance(obj, list):
        return [clean_for_json(x) for x in obj]
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return 0
    return obj

# Cargar historial existente al iniciar
if os.path.exists(CSV_FILE):
    try:
        df_init = pd.read_csv(CSV_FILE)
        # Reemplazar NaN por vacio o 0 antes de convertir a dict
        HISTORY = clean_for_json(df_init.to_dict(orient="records"))
        if len(HISTORY) > 1000: HISTORY = HISTORY[-1000:]
        if HISTORY: LAST_DATA = HISTORY[-1]
    except Exception as e:
        print(f"Error al cargar historial: {e}")

CONTROL_STATE = {
    "manual": False,
    "relay_1": False,
    "relay_2": False,
    "vfd_1": 0.0,
    "vfd_1_state": False,
    "relay_3": False
}

class ControlUpdate(BaseModel):
    manual: Optional[bool] = None
    class Config:
        extra = "allow"

@app.get("/", response_class=HTMLResponse)
async def index():
    if os.path.exists("index.html"):
        with open("index.html", "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>ADTEC Cloud Dashboard</h1><p>Archivo index.html no encontrado.</p>")

@app.post("/api/ingreso")
async def api_ingreso(payload: Dict[str, Any]):
    global HISTORY, LAST_DATA
    
    # SOPORTE PARA EL "IDIOMA" UNIFICADO:
    # Si viene con {"device": "...", "data": {...}}, extraemos "data"
    data = payload.get("data", payload)
    
    if not isinstance(data, dict):
        # Fallback por si data no es un dict
        data = payload

    if "timestamp" not in data:
        data["timestamp"] = datetime.now().isoformat()
    
    # Limpiar datos entrantes de posibles NaN/Inf
    data = clean_for_json(data)
    
    LAST_DATA = data
    HISTORY.append(data)
    if len(HISTORY) > 1000: HISTORY.pop(0)
    
    try:
        df = pd.DataFrame([data])
        file_exists = os.path.exists(CSV_FILE)
        df.to_csv(CSV_FILE, mode='a', header=not file_exists, index=False)
    except Exception as e:
        print(f"Error en CSV: {e}")
        
    return {"status": "ok"}

@app.get("/api/last")
async def api_last():
    return JSONResponse(content=clean_for_json(LAST_DATA))

@app.get("/api/data")
async def api_data():
    return JSONResponse(content=clean_for_json(HISTORY))

@app.get("/api/download/csv")
async def download_csv():
    if os.path.exists(CSV_FILE):
        return FileResponse(CSV_FILE, media_type='text/csv', filename=CSV_FILE)
    return JSONResponse(content={"error": "Archivo no encontrado"}, status_code=404)

@app.get("/api/control_state")
async def get_control_state(format: Optional[str] = None):
    if format == "esp32":
        parts = [f"MANUAL:{'ON' if CONTROL_STATE['manual'] else 'OFF'}"]
        for k, v in CONTROL_STATE.items():
            if k == "manual": continue
            val = "ON" if v is True else "OFF" if v is False else str(v)
            parts.append(f"{k}:{val}")
        return HTMLResponse(",".join(parts))
    return CONTROL_STATE

@app.post("/api/control_state")
async def update_control_state(update: ControlUpdate):
    data = update.dict(exclude_unset=True)
    for k, v in data.items():
        CONTROL_STATE[k] = v
    return CONTROL_STATE

if __name__ == "__main__":
    import uvicorn
    import os
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
