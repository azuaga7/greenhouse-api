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
import httpx

app = FastAPI(title="ADTEC Cloud Dashboard")

# Memoria caché para no saturar OpenStreetMap
LOCATION_CACHE = {}

async def get_city_country(location_str: str):
    """Convierte 'lat,lon' en 'Ciudad, Pais' usando Nominatim (OpenStreetMap)"""
    if not location_str or location_str == "0.0,0.0" or "," not in location_str:
        return "Ubicación Desconocida"
    
    if location_str in LOCATION_CACHE:
        return LOCATION_CACHE[location_str]
    
    try:
        async with httpx.AsyncClient() as client:
            lat, lon = location_str.split(",")
            url = f"https://nominatim.openstreetmap.org/reverse?lat={lat.strip()}&lon={lon.strip()}&format=json&accept-language=es"
            headers = {"User-Agent": "GreenhouseConfigStudio/1.0"}
            resp = await client.get(url, headers=headers, timeout=5.0)
            if resp.status_code == 200:
                geo = resp.json()
                address = geo.get("address", {})
                city = address.get("city") or address.get("town") or address.get("village") or address.get("county") or "Desconocido"
                country = address.get("country", "Desconocido")
                res = f"{city}, {country}"
                LOCATION_CACHE[location_str] = res
                return res
    except Exception as e:
        print(f"Error Geocoding: {e}")
    return "Ubicación Pro"

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

CSV_FILE = "telemetria.csv"
HISTORY = []
LAST_DATA = {}
FIELD_LABELS = {"dht22_1_HUM_OUT":"Humedad del Invernadero","dht22_1_TEMP_OUT":"Temperatura del Invernadero","ds18b20_2_TEMP_OUT":"Temperatura Exterior","relay_3_STATE_OUT":"Estado Bomba de Agua","relay_1_STATE_OUT":"Estado Vent. 1","relay_2_STATE_OUT":"Estado Vent. 2","vfd_1_FREQ_OUT":"Frecuencia Ventiladores Pared","relay_3_RUNTIME_OUT":"Uso Bomba de Agua","relay_1_RUNTIME_OUT":"Uso Ventiladores 1 - 2","relay_2_RUNTIME_OUT":"Uso Ventiladores 3 - 4","vfd_1_STATE_OUT":"Estado de Ventiladores Axiales","vfd_1_RUNTIME_OUT":"Uso Ventiladores Axiales","ds18b20_1_TEMP_OUT":"Temperatura de Pozo","tsl2561_1_LUX_OUT":"Luxes","gsm_1_SIGNAL":"Signal GSM","gsm_1_STATE":"Estado GSM","gsm_1_LOCATION":"GPS"}

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
    
    # Motor de Geocodificación Inteligente (Normaliza y procesa múltiples módulos GSM)
    for k, v in list(data.items()):
        k_upper = k.upper()
        if "GSM" in k_upper and "LOCATION" in k_upper and "_STAT" not in k_upper and "_CITY" not in k_upper:
            if v and v != "0.0,0.0" and "," in str(v):
                # Generar clave de ciudad específica para este módulo (Ej: gsm_1_LOCATION_CITY)
                city_key = k + "_CITY"
                data[city_key] = await get_city_country(v)
                # Mantener compatibilidad con motor original y widgets estándar
                if k == "gsm_location" or k == "gsm_1_LOCATION":
                    data["gsm_location"] = v
                    data["gsm_city_country"] = data[city_key]

    # ORDENAR COLUMNAS PARA EL CSV (Identidad Pro)
    # 1. Extraer todas las keys disponibles de los field labels (que vienen del editor)
    ordered_keys = ["timestamp"]
    if isinstance(FIELD_LABELS, dict):
        # Añadir las variables configuradas en el orden del editor
        ordered_keys.extend([k for k in FIELD_LABELS.keys() if k != "timestamp"])
    
    # 2. Añadir keys adicionales que puedan venir en la telemetría pero no estén en labels
    for k in data.keys():
        if k not in ordered_keys:
            ordered_keys.append(k)

    # 3. Crear un diccionario ordenado para el CSV
    ordered_data = {k: data.get(k, "") for k in ordered_keys}

    LAST_DATA = data
    HISTORY.append(data)
    if len(HISTORY) > 1000: HISTORY.pop(0)
    
    try:
        # Usar ordered_data para asegurar el orden de las columnas en el CSV
        df = pd.DataFrame([ordered_data])
        file_exists = os.path.exists(CSV_FILE)
        
        if not file_exists:
            df.to_csv(CSV_FILE, mode='w', header=True, index=False)
        else:
            # Leer las columnas existentes del CSV para mantener consistencia
            df_existing = pd.read_csv(CSV_FILE, nrows=0)
            # Reordenar el nuevo dataframe para que coincida con el CSV existente
            df = df.reindex(columns=df_existing.columns, fill_value="")
            df.to_csv(CSV_FILE, mode='a', header=False, index=False)
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
