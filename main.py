from fastapi import FastAPI, UploadFile, File, Query, Depends, HTTPException, status
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import pandas as pd
import os
import json
import base64
import hashlib
import secrets
from datetime import datetime, timedelta
from jose import JWTError, jwt
import math
import httpx

# --- CONFIGURACIÓN DE SEGURIDAD ---
SECRET_KEY = secrets.token_hex(32)
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 # 1 día por defecto, ajustable por rol

app = FastAPI(title="ADTEC Cloud Dashboard")

# Memoria caché para no saturar OpenStreetMap
LOCATION_CACHE = {}

# --- CARGAR USUARIOS DESDE DB SEGURA ---
USERS_DB = []
USERS_FILE = "users_db.gcs.b64"
GUEST_START_TIMES = {} # { "username": datetime }

def load_users():
    global USERS_DB
    if os.path.exists(USERS_FILE):
        try:
            with open(USERS_FILE, "r") as f:
                encoded = f.read()
                decoded = base64.b64decode(encoded).decode('utf-8')
                USERS_DB = json.loads(decoded)
                print(f"✅ Cargados {len(USERS_DB)} usuarios de la base de datos segura.")
        except Exception as e:
            print(f"❌ Error al cargar usuarios: {e}")
            USERS_DB = []
    else:
        print("⚠️ No se encontró users_db.gcs.b64. Acceso público deshabilitado.")

def save_users():
    """Persiste la lista de usuarios de vuelta al archivo b64"""
    try:
        json_str = json.dumps(USERS_DB)
        encoded = base64.b64encode(json_str.encode('utf-8')).decode('utf-8')
        with open(USERS_FILE, "w") as f:
            f.write(encoded)
        print("💾 Base de datos de usuarios actualizada.")
    except Exception as e:
        print(f"❌ Error al guardar usuarios: {e}")

load_users()

# --- UTILIDADES DE CONTRASEÑA (PBKDF2) ---
def verify_password(plain_password, salt_hex, stored_hash_hex):
    try:
        salt = bytes.fromhex(salt_hex)
        # PBKDF2 con 100,000 iteraciones (mismo que el software)
        new_hash = hashlib.pbkdf2_hmac(
            'sha256', 
            plain_password.encode('utf-8'), 
            salt, 
            100000
        )
        return new_hash.hex() == stored_hash_hex
    except:
        return False

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/token")

# --- ESTADO DE SESIONES Y CONTROL ---
ACTIVE_SESSIONS = {}  # { "username": { "role": "...", "last_active": datetime, "notifications": [] } }
CONTROL_HOLDER = {"username": None, "role": None, "last_action": None}

def get_role_priority(role: str) -> int:
    priorities = {"adtec": 3, "operacion": 2, "user": 1, "guest": 0}
    return priorities.get(role, 0)

def update_session(username: str, role: str):
    now = datetime.now()
    if username not in ACTIVE_SESSIONS:
        ACTIVE_SESSIONS[username] = {
            "role": role,
            "last_active": now,
            "notifications": []
        }
    else:
        ACTIVE_SESSIONS[username]["last_active"] = now
    
    # Limpiar sesiones inactivas (> 30s)
    to_delete = []
    for uname, data in ACTIVE_SESSIONS.items():
        if (now - data["last_active"]).total_seconds() > 30:
            to_delete.append(uname)
    for uname in to_delete:
        if CONTROL_HOLDER["username"] == uname:
            CONTROL_HOLDER["username"] = None
            CONTROL_HOLDER["role"] = None
        del ACTIVE_SESSIONS[uname]

async def get_current_user(token: str = Depends(oauth2_scheme)):
    global USERS_DB
    # Si no hay usuarios cargados, el acceso es libre (modo editor)
    if not USERS_DB:
        return {"username": "Invitado", "mode": "adtec", "permissions": {"canControl": True, "canDownload": True, "accessDocs": True, "accessData": True}}

    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="No se pudo validar la sesión",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        
        user = next((u for u in USERS_DB if u["username"] == username), None)
        if user is None:
            raise credentials_exception
        
        # Lógica de expiración y auto-eliminación para Invitados
        if user.get("mode") == "guest":
            start_time = GUEST_START_TIMES.get(username)
            if start_time:
                ttl = user.get("ttl", 10)
                elapsed = (datetime.now() - start_time).total_seconds() / 60
                if elapsed > ttl:
                    # ELIMINAR DE LA BASE DE DATOS DEFINITIVAMENTE
                    USERS_DB = [u for u in USERS_DB if u["username"] != username]
                    save_users()
                    print(f"🗑️ Usuario invitado '{username}' ha sido eliminado por expiración de TTL ({ttl} min).")
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail="Tu acceso de invitado ha expirado y ha sido eliminado.",
                        headers={"WWW-Authenticate": "Bearer"},
                    )

        # Actualizar actividad de sesión
        update_session(username, user["mode"])
        
        return user
    except JWTError:
        raise credentials_exception

# Dependency para verificar permisos específicos
def check_permission(perm_name: str):
    async def permission_dependency(current_user: Dict = Depends(get_current_user)):
        perms = current_user.get("permissions", {})
        if current_user.get("mode") == "adtec":
            return current_user
        if not perms.get(perm_name, False):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"No tiene permiso para realizar esta acción ({perm_name})"
            )
        return current_user
    return permission_dependency

# --- RESTO DE LA LÓGICA ---

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
ALERTS = []
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

# --- ENDPOINTS DE AUTENTICACIÓN ---

@app.post("/api/token")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = next((u for u in USERS_DB if u["username"] == form_data.username), None)
    if not user or not verify_password(form_data.password, user["salt"], user["password_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuario o contraseña incorrectos",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Iniciar cronómetro para invitados si es su primer login
    if user.get("mode") == "guest" and user["username"] not in GUEST_START_TIMES:
        GUEST_START_TIMES[user["username"]] = datetime.now()
        print(f"⏱️ Iniciado tiempo de vida para invitado: {user['username']} ({user.get('ttl', 10)} min)")

    # TTL dinámico según el rol
    expire_min = ACCESS_TOKEN_EXPIRE_MINUTES
    if user.get("mode") == "guest":
        expire_min = user.get("ttl", 10) # 10 min por defecto para invitados
    
    access_token_expires = timedelta(minutes=expire_min)
    access_token = create_access_token(
        data={"sub": user["username"], "mode": user["mode"], "permissions": user["permissions"]},
        expires_delta=access_token_expires
    )
    return {
        "access_token": access_token, 
        "token_type": "bearer",
        "user": {
            "username": user["username"],
            "mode": user["mode"],
            "permissions": user["permissions"]
        }
    }

@app.get("/api/me")
async def read_users_me(current_user: Dict = Depends(get_current_user)):
    return {
        "username": current_user["username"],
        "mode": current_user["mode"],
        "permissions": current_user["permissions"]
    }

# --- ENDPOINTS DEL DASHBOARD (PROTEGIDOS) ---

@app.get("/", response_class=HTMLResponse)
async def index():
    if os.path.exists("index.html"):
        with open("index.html", "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>ADTEC Cloud Dashboard</h1><p>Archivo index.html no encontrado.</p>")

@app.post("/api/ingreso")
async def api_ingreso(payload: Dict[str, Any]):
    # El ingreso de datos (del hardware) sigue siendo abierto o con API KEY si lo deseas
    global HISTORY, LAST_DATA
    data = payload.get("data", payload)
    if not isinstance(data, dict): data = payload
    if "timestamp" not in data: data["timestamp"] = datetime.now().isoformat()
    data = clean_for_json(data)
    
    for k, v in list(data.items()):
        k_upper = k.upper()
        if "GSM" in k_upper and "LOCATION" in k_upper and "_STAT" not in k_upper and "_CITY" not in k_upper:
            if v and v != "0.0,0.0" and "," in str(v):
                city_key = k + "_CITY"
                data[city_key] = await get_city_country(v)
                if k == "gsm_location" or k == "gsm_1_LOCATION":
                    data["gsm_location"] = v
                    data["gsm_city_country"] = data[city_key]

    ordered_keys = ["timestamp"]
    if isinstance(FIELD_LABELS, dict):
        ordered_keys.extend([k for k in FIELD_LABELS.keys() if k != "timestamp"])
    for k in data.keys():
        if k not in ordered_keys: ordered_keys.append(k)
    ordered_data = {k: data.get(k, "") for k in ordered_keys}

    LAST_DATA = data
    HISTORY.append(data)
    if len(HISTORY) > 1000: HISTORY.pop(0)
    
    try:
        df = pd.DataFrame([ordered_data])
        file_exists = os.path.exists(CSV_FILE)
        if not file_exists:
            df.to_csv(CSV_FILE, mode='w', header=True, index=False)
        else:
            df_existing = pd.read_csv(CSV_FILE, nrows=0)
            existing_cols = list(df_existing.columns)

            # columnas nuevas que vienen en el firmware nuevo
            new_cols = [c for c in df.columns if c not in existing_cols]

            if new_cols:
                # 1) Expandir el CSV: reescribir encabezado con columnas nuevas
                updated_cols = existing_cols + new_cols

                # leer todo el CSV viejo, agregar columnas nuevas vacías, y reescribir
                df_all = pd.read_csv(CSV_FILE)
                df_all = df_all.reindex(columns=updated_cols, fill_value="")
                df_all.to_csv(CSV_FILE, mode="w", header=True, index=False)

                existing_cols = updated_cols  # actualizar para el append

            # 2) Alinear el registro nuevo al orden final y append
            df = df.reindex(columns=existing_cols, fill_value="")
            df.to_csv(CSV_FILE, mode='a', header=False, index=False)

    except Exception as e:
        print(f"Error en CSV: {e}")
        
    return {"status": "ok"}

@app.get("/api/last")
async def api_last(user: Dict = Depends(get_current_user)):
    return JSONResponse(content=clean_for_json(LAST_DATA))

@app.get("/api/data")
async def api_data(user: Dict = Depends(get_current_user)):
    return JSONResponse(content=clean_for_json(HISTORY))

@app.post("/api/alerts")
async def api_alerts(payload: Dict[str, Any]):
    global ALERTS
    alert = payload.copy()
    if "timestamp" not in alert: alert["timestamp"] = datetime.now().isoformat()
    ALERTS.append(alert)
    if len(ALERTS) > 100: ALERTS.pop(0)
    return {"status": "ok"}

@app.get("/api/alerts")
async def get_alerts(user: Dict = Depends(get_current_user)):
    return JSONResponse(content=clean_for_json(ALERTS))

@app.post("/api/alerts/clear")
async def clear_alerts(user: Dict = Depends(check_permission("canControl"))):
    global ALERTS
    ALERTS = []
    return {"status": "ok"}

@app.get("/api/download/csv")
async def download_csv(user: Dict = Depends(check_permission("canDownload"))):
    if os.path.exists(CSV_FILE):
        return FileResponse(CSV_FILE, media_type='text/csv', filename=CSV_FILE)
    return JSONResponse(content={"error": "Archivo no encontrado"}, status_code=404)

@app.get("/api/control_state")
async def get_control_state(format: Optional[str] = None):
    # El Hardware pide estado sin token por ahora (secreto compartido en config.h)
    if format == "esp32":
        parts = [f"MANUAL:{'ON' if CONTROL_STATE['manual'] else 'OFF'}"]
        for k, v in CONTROL_STATE.items():
            if k == "manual": continue
            val = "ON" if v is True else "OFF" if v is False else str(v)
            parts.append(f"{k}:{val}")
        return HTMLResponse(",".join(parts))
    
    # Enriquecer con info del poseedor del control
    return {
        "state": CONTROL_STATE,
        "holder": CONTROL_HOLDER["username"],
        "holder_role": CONTROL_HOLDER["role"]
    }

@app.post("/api/control_state")
async def update_control_state(update: ControlUpdate, user: Dict = Depends(check_permission("canControl"))):
    global CONTROL_HOLDER
    username = user["username"]
    role = user["mode"]
    priority = get_role_priority(role)
    
    current_holder = CONTROL_HOLDER["username"]
    current_role = CONTROL_HOLDER["role"]
    current_priority = get_role_priority(current_role) if current_role else -1
    
    # Lógica de jerarquía avanzada
    if current_holder and current_holder != username:
        if priority > current_priority:
            # El nuevo usuario tiene más prioridad (ej: adtec sobre operacion)
            
            # 1. Notificar al que pierde el control (Anónimo)
            msg_for_kicked = "Tu control ha sido revocado por un administrador."
            if current_holder in ACTIVE_SESSIONS:
                ACTIVE_SESSIONS[current_holder]["notifications"].append({"type": "error", "text": msg_for_kicked})
            
            # 2. Notificar al que toma el control sobre quiénes lo tenían o estaban presentes
            overridden_users = []
            for uname, data in ACTIVE_SESSIONS.items():
                if uname != username and get_role_priority(data["role"]) < priority:
                    overridden_users.append(f"{uname} ({data['role'].capitalize()})")
            
            if overridden_users:
                msg_for_taker = "Has tomado el control por sobre:\n- " + "\n- ".join(overridden_users)
                if username in ACTIVE_SESSIONS:
                    ACTIVE_SESSIONS[username]["notifications"].append({"type": "info", "text": msg_for_taker})
                
            CONTROL_HOLDER = {"username": username, "role": role, "last_action": datetime.now()}
        elif priority == current_priority:
            # Mismo rango, no puede quitar el control si el otro está activo (30s)
            raise HTTPException(status_code=403, detail=f"El control ya está siendo usado por {current_holder} ({current_role})")
        else:
            # Menor rango
            raise HTTPException(status_code=403, detail="No tienes prioridad suficiente para tomar el control.")
    else:
        # Nadie tiene el control o es el mismo usuario
        if not current_holder:
            # Notificar de todas formas quiénes están mirando si es un Adtec tomando control
            if role == "adtec":
                others = [f"{u} ({d['role'].capitalize()})" for u, d in ACTIVE_SESSIONS.items() if u != username]
                if others:
                    msg = "Has tomado el control. Usuarios presentes:\n- " + "\n- ".join(others)
                    if username in ACTIVE_SESSIONS:
                        ACTIVE_SESSIONS[username]["notifications"].append({"type": "info", "text": msg})
        
        CONTROL_HOLDER = {"username": username, "role": role, "last_action": datetime.now()}

    data = update.dict(exclude_unset=True)
    for k, v in data.items():
        CONTROL_STATE[k] = v
    return CONTROL_STATE

@app.get("/api/session_status")
async def get_session_status(user: Dict = Depends(get_current_user)):
    username = user["username"]
    notifications = []
    if username in ACTIVE_SESSIONS:
        notifications = ACTIVE_SESSIONS[username]["notifications"]
        ACTIVE_SESSIONS[username]["notifications"] = [] # Limpiar tras leer
    
    return {
        "active_users": [
            {"username": u, "role": d["role"], "is_me": u == username} 
            for u, d in ACTIVE_SESSIONS.items()
        ],
        "control_holder": CONTROL_HOLDER["username"],
        "notifications": notifications
    }

if __name__ == "__main__":
    import uvicorn
    import os
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
