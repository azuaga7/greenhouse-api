from fastapi import FastAPI, UploadFile, File, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Any
import pandas as pd
import os
import json
from datetime import datetime

app = FastAPI(title="ADTEC Cloud Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

CSV_FILE = "telemetria.csv"
HISTORY = []
FIELD_LABELS = {"dht22_1_HUM_OUT":"Humedad del Invernadero","dht22_1_TEMP_OUT":"Temperatura del Invernadero","ds18b20_2_TEMP_OUT":"Temperatura Exterior","relay_3_STATE_OUT":"Estado Bomba de Agua","relay_1_STATE_OUT":"Estado Vent. 1","relay_2_STATE_OUT":"Estado Vent. 2","vfd_1_FREQ_OUT":"Frecuencia Ventiladores Pared","relay_3_RUNTIME_OUT":"Uso Bomba de Agua","relay_1_RUNTIME_OUT":"Uso Ventiladores 1 - 2","relay_2_RUNTIME_OUT":"Uso Ventiladores 3 - 4","vfd_1_STATE_OUT":"Estado de Ventiladores Axiales","vfd_1_RUNTIME_OUT":"Uso Ventiladores Axiales","ds18b20_1_TEMP_OUT":"Temperatura de Pozo","tsl2561_1_LUX_OUT":"Luxes"}

class Lectura(BaseModel):
    class Config:
        extra = "allow"

if os.path.exists(CSV_FILE):
    try:
        df_init = pd.read_csv(CSV_FILE)
        HISTORY = df_init.to_dict(orient="records")
        if len(HISTORY) > 500: HISTORY = HISTORY[-500:]
    except Exception as e:
        print(f"Error al cargar CSV: {e}")

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
    with open("index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

@app.post("/api/ingreso")
async def api_ingreso(lectura: Lectura):
    global HISTORY
    data = lectura.dict()
    if "timestamp" not in data:
        data["timestamp"] = datetime.now().isoformat()
        
    HISTORY.append(data)
    if len(HISTORY) > 500: HISTORY.pop(0)
    
    try:
        final_data = {FIELD_LABELS.get(k, k): v for k, v in data.items()}
        df = pd.DataFrame([final_data])
        df.to_csv(CSV_FILE, mode='a', header=not os.path.exists(CSV_FILE), index=False)
    except Exception as e:
        print(f"Error en CSV: {e}")
    return {"status": "ok"}

@app.get("/api/last")
async def api_last():
    return HISTORY[-1] if HISTORY else {}

@app.get("/api/data")
async def api_data():
    return HISTORY

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
        # Aceptar nuevas claves dinámicamente para estados de VFD (_state)
        CONTROL_STATE[k] = v
    return CONTROL_STATE

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
