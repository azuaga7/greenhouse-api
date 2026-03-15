from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
import os
import io
import re
import csv
import json
import math
import uuid
import gzip
import time
import hashlib
from pathlib import Path

import httpx

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

try:
    import openpyxl
    from openpyxl import Workbook
except ImportError:
    openpyxl = None
    Workbook = None

PY_TZ = ZoneInfo("America/Asuncion")
DEFAULT_TIMEOUT_MS = int(os.getenv("INVERNIA_AI_TIMEOUT_MS", "45000"))
DEFAULT_AI_BASE_URL = os.getenv("INVERNIA_AI_BASE_URL", "https://generativelanguage.googleapis.com")
DEFAULT_AI_API_KEY = os.getenv("INVERNIA_AI_API_KEY", "")
DEFAULT_EXPORT_DIR = os.getenv("INVERNIA_AI_EXPORT_DIR", "/opt/invernia/exports")
BRIDGE_HTTP_BASE = os.getenv("INVERNIA_AI_BRIDGE_BASE_URL", "http://161.35.129.132")
DEFAULT_PLATFORM = os.getenv("INVERNIA_AI_DEFAULT_PLATFORM", "web")
DEFAULT_MAX_ROWS = int(os.getenv("INVERNIA_AI_MAX_ROWS", "1000"))
EXPORT_TOKEN_TTL_SEC = int(os.getenv("INVERNIA_AI_EXPORT_TOKEN_TTL_SEC", "3600"))
CACHE_API_DIR = os.environ.get("CACHE_API_DIR", os.path.join("data", "cache_api"))
CACHE_API_SNAPSHOT_GZ = os.path.join(CACHE_API_DIR, "snapshot.json.gz")
EXPORT_MANIFEST = os.path.join(DEFAULT_EXPORT_DIR, ".manifest.json")

PREFERRED_EXPORT_COLUMNS = [
    "ts",
    "ts_iso",
    "device",
    "channel",
    "data",
    "dht22_1_TEMP_OUT",
    "dht22_1_HUM_OUT",
    "ds18b20_1_TEMP_OUT",
    "ds18b20_2_TEMP_OUT",
    "tsl2561_1_LUX_OUT",
    "relay_1_STATE_OUT",
    "relay_2_STATE_OUT",
    "relay_3_STATE_OUT",
    "vfd_1_STATE_OUT",
    "vfd_1_FREQ_OUT",
    "relay_1_RUNTIME_OUT",
    "relay_2_RUNTIME_OUT",
    "relay_3_RUNTIME_OUT",
    "vfd_1_RUNTIME_OUT",
    "gsm_1_SIGNAL",
    "gsm_1_STATE",
    "gsm_1_LOCATION",
    "device_id",
]

os.makedirs(DEFAULT_EXPORT_DIR, exist_ok=True)

router = APIRouter(prefix="/api/invernIA", tags=["InvernIA"])


class WindowItem(BaseModel):
    label: str
    from_: str = Field(alias="from")
    to: str


class QueryOptions(BaseModel):
    mode: str = "auto"
    max_points: int = 500
    prefer_export_format: str = "xlsx"


class QueryContext(BaseModel):
    platform: str = "web"
    view: str = "assistant"


class QueryPayload(BaseModel):
    intent: Optional[str] = None
    dataset: Optional[str] = None
    metrics: Optional[List[str]] = None
    columns: Optional[List[str]] = None
    filters: Optional[Dict[str, Any]] = None
    windows: Optional[List[WindowItem]] = None
    limit: Optional[int] = None
    export: Optional[bool] = None
    export_format: Optional[str] = None
    title: Optional[str] = None


class InvernIAQueryRequest(BaseModel):
    prompt: str
    context: QueryContext = Field(default_factory=QueryContext)
    options: QueryOptions = Field(default_factory=QueryOptions)
    payload: Optional[QueryPayload] = None


class InvernIAConfigRequest(BaseModel):
    prompt: str
    context: QueryContext = Field(default_factory=QueryContext)
    payload: Optional[Dict[str, Any]] = None


def _iso_now_py() -> str:
    return datetime.now(PY_TZ).isoformat()


def _safe_json_loads(text: str, fallback: Any) -> Any:
    try:
        return json.loads(text)
    except Exception:
        return fallback


def _to_ts(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=PY_TZ)
        return int(dt.timestamp())
    except Exception:
        return None


def _format_py_iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(PY_TZ).isoformat()


def _normalize_metric_name(name: str) -> str:
    return (name or "").strip()


def _ordered_columns(rows: List[Dict[str, Any]], requested: Optional[List[str]] = None) -> List[str]:
    if requested:
        existing = set()
        for row in rows:
            existing.update(row.keys())
        preferred = [c for c in requested if c in existing]
        rest = sorted([c for c in existing if c not in preferred])
        return preferred + rest

    all_cols = set()
    for row in rows:
        all_cols.update(row.keys())
    preferred = [c for c in PREFERRED_EXPORT_COLUMNS if c in all_cols]
    rest = sorted([c for c in all_cols if c not in preferred])
    return preferred + rest


def _load_manifest() -> Dict[str, Any]:
    path = Path(EXPORT_MANIFEST)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_manifest(data: Dict[str, Any]) -> None:
    Path(EXPORT_MANIFEST).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _purge_manifest_expired() -> Dict[str, Any]:
    manifest = _load_manifest()
    now_ts = int(time.time())
    changed = False
    cleaned = {}
    for token, item in manifest.items():
        expires_ts = item.get("expires_ts")
        if isinstance(expires_ts, int) and expires_ts < now_ts:
            changed = True
            continue
        cleaned[token] = item
    if changed:
        _save_manifest(cleaned)
    return cleaned


def _register_export(file_path: str, media_type: str, download_name: str) -> str:
    token = hashlib.sha256(f"{file_path}|{download_name}|{time.time()}|{uuid.uuid4().hex}".encode("utf-8")).hexdigest()[:32]
    manifest = _load_manifest()
    now_ts = int(time.time())
    manifest[token] = {
        "file_path": file_path,
        "media_type": media_type,
        "download_name": download_name,
        "created_at": _iso_now_py(),
        "created_ts": now_ts,
        "expires_ts": now_ts + EXPORT_TOKEN_TTL_SEC,
    }
    _save_manifest(manifest)
    return token


def _make_export_file(rows: List[Dict[str, Any]], fmt: str, title: str, requested_columns: Optional[List[str]] = None) -> Dict[str, Any]:
    fmt = (fmt or "xlsx").lower()
    if fmt not in ("xlsx", "csv"):
        fmt = "xlsx"

    columns = _ordered_columns(rows, requested_columns)
    safe_title = re.sub(r"[^A-Za-z0-9._-]+", "_", title or "invernia_export").strip("_") or "invernia_export"
    ts_slug = datetime.now(PY_TZ).strftime("%Y%m%d_%H%M%S")

    if fmt == "csv":
        file_path = os.path.join(DEFAULT_EXPORT_DIR, f"{safe_title}_{ts_slug}.csv")
        with open(file_path, "w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=columns)
            writer.writeheader()
            for row in rows:
                writer.writerow({c: row.get(c) for c in columns})
        token = _register_export(file_path, "text/csv", os.path.basename(file_path))
        return {
            "token": token,
            "format": "csv",
            "filename": os.path.basename(file_path),
            "columns": columns,
            "rows": len(rows),
            "downloadUrl": f"/api/invernIA/export/{token}",
        }

    if Workbook is None:
        raise HTTPException(status_code=500, detail="openpyxl no está disponible para generar XLSX")

    file_path = os.path.join(DEFAULT_EXPORT_DIR, f"{safe_title}_{ts_slug}.xlsx")
    wb = Workbook()
    ws = wb.active
    ws.title = "data"
    ws.append(columns)
    for row in rows:
        ws.append([row.get(c) for c in columns])
    wb.save(file_path)
    token = _register_export(file_path, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", os.path.basename(file_path))
    return {
        "token": token,
        "format": "xlsx",
        "filename": os.path.basename(file_path),
        "columns": columns,
        "rows": len(rows),
        "downloadUrl": f"/api/invernIA/export/{token}",
    }


async def _bridge_get(path: str, params: Optional[Dict[str, Any]] = None, timeout_ms: Optional[int] = None) -> Any:
    timeout = httpx.Timeout((timeout_ms or DEFAULT_TIMEOUT_MS) / 1000.0)
    url = f"{BRIDGE_HTTP_BASE}{path}"
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        ctype = resp.headers.get("content-type", "")
        if "application/json" in ctype or "text/json" in ctype:
            return resp.json()
        return resp.content


async def _bridge_post(path: str, payload: Dict[str, Any], timeout_ms: Optional[int] = None) -> Any:
    timeout = httpx.Timeout((timeout_ms or DEFAULT_TIMEOUT_MS) / 1000.0)
    url = f"{BRIDGE_HTTP_BASE}{path}"
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        resp = await client.post(url, json=payload)
        resp.raise_for_status()
        ctype = resp.headers.get("content-type", "")
        if "application/json" in ctype or "text/json" in ctype:
            return resp.json()
        return resp.content


async def _load_snapshot() -> Dict[str, Any]:
    if os.path.exists(CACHE_API_SNAPSHOT_GZ):
        try:
            with gzip.open(CACHE_API_SNAPSHOT_GZ, "rt", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            pass
    try:
        raw = await _bridge_get("/cache_api/snapshot.json.gz")
        if isinstance(raw, (bytes, bytearray)):
            return json.loads(gzip.decompress(raw).decode("utf-8"))
    except Exception:
        return {}
    return {}


async def _fetch_last() -> Dict[str, Any]:
    data = await _bridge_get("/api/last")
    return data if isinstance(data, dict) else {}


async def _fetch_alerts(limit: int = 100) -> List[Dict[str, Any]]:
    data = await _bridge_get("/api/alerts", params={"limit": limit})
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        if isinstance(data.get("items"), list):
            return data.get("items")
        if isinstance(data.get("rows"), list):
            return data.get("rows")
        if isinstance(data.get("alerts"), list):
            return data.get("alerts")
    return []


async def _fetch_control_state() -> Dict[str, Any]:
    data = await _bridge_get("/api/control_state")
    return data if isinstance(data, dict) else {}


async def _fetch_table_rows(limit: int = 500) -> List[Dict[str, Any]]:
    safe_limit = min(max(int(limit or 100), 1), DEFAULT_MAX_ROWS)
    data = await _bridge_get("/api/data", params={"limit": safe_limit})
    return data if isinstance(data, list) else []


async def _fetch_metric_series(metric: str, from_ts: Optional[int], to_ts: Optional[int], max_points: int = 500) -> List[List[Any]]:
    params = {"key": metric, "max_points": max_points}
    if from_ts is not None:
        params["from_ts"] = from_ts
    if to_ts is not None:
        params["to_ts"] = to_ts
    data = await _bridge_get("/api/series", params=params)
    if isinstance(data, dict):
        points = data.get("points")
        if isinstance(points, list):
            return points
    return []


async def _fetch_window_dataset(metrics: List[str], windows: List[Dict[str, Any]], max_points: int) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    for window in windows:
        label = window["label"]
        from_ts = _to_ts(window.get("from"))
        to_ts = _to_ts(window.get("to"))
        merged: Dict[int, Dict[str, Any]] = {}
        for metric in metrics:
            points = await _fetch_metric_series(metric, from_ts, to_ts, max_points=max_points)
            for item in points:
                if not isinstance(item, list) or len(item) < 2:
                    continue
                ts = int(item[0])
                val = item[1]
                row = merged.setdefault(ts, {
                    "ts": ts,
                    "ts_iso": _format_py_iso(ts),
                    "window": label,
                })
                row[metric] = val
        out[label] = [merged[k] for k in sorted(merged.keys())]
    return out


async def _call_external_ai(prompt: str, context: Dict[str, Any], structured_data: Dict[str, Any]) -> Optional[str]:
    if not DEFAULT_AI_BASE_URL or not DEFAULT_AI_API_KEY:
        return None

    payload = {
        "prompt": prompt,
        "context": context,
        "data": structured_data,
    }
    headers = {
        "Authorization": f"Bearer {DEFAULT_AI_API_KEY}",
        "Content-Type": "application/json",
    }
    timeout = httpx.Timeout(DEFAULT_TIMEOUT_MS / 1000.0)
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            resp = await client.post(DEFAULT_AI_BASE_URL, json=payload, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict):
                return data.get("text") or data.get("message") or data.get("response")
    except Exception:
        return None
    return None


def _infer_intent(prompt: str, payload: Optional[QueryPayload]) -> str:
    if payload and payload.intent:
        return payload.intent.lower()
    p = (prompt or "").lower()
    if any(x in p for x in ["excel", "xlsx", "csv", "exporta", "exportame"]):
        return "export"
    if any(x in p for x in ["gráfico", "grafico", "chart", "curva", "comparame", "compara"]):
        return "chart"
    if any(x in p for x in ["tabla", "table", "alertas críticas", "alertas criticas"]):
        return "table"
    if any(x in p for x in ["máximo", "maximo", "mínimo", "minimo", "promedio", "avg", "resum", "stats"]):
        return "stats"
    if any(x in p for x in ["config", "propon", "bajar temperatura", "subir humedad"]):
        return "config"
    return "text"


def _infer_metrics(prompt: str, payload: Optional[QueryPayload]) -> List[str]:
    if payload and payload.metrics:
        return [_normalize_metric_name(x) for x in payload.metrics if x]
    p = (prompt or "").lower()
    metrics = []
    if "temper" in p:
        metrics.append("dht22_1_TEMP_OUT")
    if "hum" in p:
        metrics.append("dht22_1_HUM_OUT")
    if "lux" in p or "luz" in p:
        metrics.append("tsl2561_1_LUX_OUT")
    if "vfd" in p or "frecuencia" in p:
        metrics.append("vfd_1_FREQ_OUT")
    if not metrics:
        metrics.append("dht22_1_TEMP_OUT")
    return metrics


def _default_windows() -> List[Dict[str, Any]]:
    now = datetime.now(PY_TZ)
    start_today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_yesterday = start_today.timestamp() - 86400
    end_yesterday = start_today.timestamp() - 1
    return [
        {
            "label": "actual",
            "from": start_today.isoformat(),
            "to": now.isoformat(),
        },
        {
            "label": "anterior",
            "from": datetime.fromtimestamp(start_yesterday, PY_TZ).isoformat(),
            "to": datetime.fromtimestamp(end_yesterday, PY_TZ).isoformat(),
        },
    ]


def _normalize_windows(payload: Optional[QueryPayload]) -> List[Dict[str, Any]]:
    if payload and payload.windows:
        return [
            {"label": w.label, "from": w.from_, "to": w.to}
            for w in payload.windows
        ]
    return _default_windows()


def _calc_metric_stats(rows: List[Dict[str, Any]], metric: str) -> Dict[str, Any]:
    vals = []
    for row in rows:
        v = row.get(metric)
        if isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v):
            vals.append(float(v))
    if not vals:
        return {"metric": metric, "count": 0, "min": None, "max": None, "avg": None, "delta": None}
    return {
        "metric": metric,
        "count": len(vals),
        "min": min(vals),
        "max": max(vals),
        "avg": sum(vals) / len(vals),
        "delta": vals[-1] - vals[0] if len(vals) >= 2 else 0,
    }


def _table_from_alerts(alerts: List[Dict[str, Any]]) -> Dict[str, Any]:
    rows = []
    for a in alerts:
        rows.append({
            "ts_iso": a.get("ts_iso") or a.get("timestamp"),
            "type": a.get("type"),
            "msg": a.get("msg"),
            "value": a.get("value"),
            "signal": a.get("signal"),
            "ref": a.get("ref"),
        })
    return {
        "columns": ["ts_iso", "type", "msg", "value", "signal", "ref"],
        "rows": rows,
        "meta": {"count": len(rows), "source": "api_alerts"},
    }


def _table_from_rows(rows: List[Dict[str, Any]], requested_columns: Optional[List[str]] = None) -> Dict[str, Any]:
    columns = _ordered_columns(rows, requested_columns)
    return {
        "columns": columns,
        "rows": [{c: row.get(c) for c in columns} for row in rows],
        "meta": {"count": len(rows)},
    }


def _build_chart_payload(window_data: Dict[str, List[Dict[str, Any]]], metrics: List[str], context: QueryContext) -> Dict[str, Any]:
    rows = []
    for label, items in window_data.items():
        for row in items:
            merged = dict(row)
            merged["window"] = label
            rows.append(merged)
    columns = _ordered_columns(rows, ["window", "ts", "ts_iso"] + metrics)
    return {
        "recommendedType": "line",
        "columns": columns,
        "rows": [{c: row.get(c) for c in columns} for row in rows],
        "spec": {
            "x": "ts_iso",
            "series": metrics,
            "groupBy": "window",
            "platform": context.platform,
            "compact": context.platform == "mobile",
        },
    }


def _response(ok: bool, kind: str, message: str, *, text=None, table=None, chart=None, stats=None, export=None, config_proposal=None, error=None):
    return {
        "ok": ok,
        "kind": kind,
        "message": message,
        "data": {
            "text": text,
            "table": table,
            "chart": chart,
            "stats": stats,
            "export": export,
            "configProposal": config_proposal,
        },
        "error": error,
    }


@router.get("/health")
async def invernia_health():
    export_dir_ok = os.path.isdir(DEFAULT_EXPORT_DIR)
    bridge_ok = bool(BRIDGE_HTTP_BASE)
    configured = bool(DEFAULT_AI_BASE_URL and DEFAULT_AI_API_KEY)

    return {
        "ok": bool(bridge_ok),
        "configured": configured,
        "baseUrl": "present" if DEFAULT_AI_BASE_URL else "missing",
        "apiKey": "present" if DEFAULT_AI_API_KEY else "missing",
        "bridgeBase": "present" if BRIDGE_HTTP_BASE else "missing",
        "exportDir": DEFAULT_EXPORT_DIR,
        "exportDirStatus": "ok" if export_dir_ok else "missing",
        "defaultPlatform": DEFAULT_PLATFORM,
        "maxRows": DEFAULT_MAX_ROWS,
        "tokenTtlSec": EXPORT_TOKEN_TTL_SEC,
        "time": _iso_now_py(),
    }


@router.get("/export/{token}")
async def invernia_export(token: str):
    manifest = _purge_manifest_expired()
    item = manifest.get(token)
    if not item:
        raise HTTPException(status_code=404, detail="export token no encontrado")

    expires_ts = item.get("expires_ts")
    if isinstance(expires_ts, int) and expires_ts < int(time.time()):
        raise HTTPException(status_code=404, detail="export token expirado")

    file_path = item.get("file_path")
    if not file_path:
        raise HTTPException(status_code=404, detail="export path inválido")

    resolved = str(Path(file_path).resolve())
    export_root = str(Path(DEFAULT_EXPORT_DIR).resolve())
    if not resolved.startswith(export_root):
        raise HTTPException(status_code=403, detail="export path no permitido")
    if not os.path.exists(resolved):
        raise HTTPException(status_code=404, detail="archivo exportado no existe")

    return FileResponse(
        resolved,
        media_type=item.get("media_type") or "application/octet-stream",
        filename=item.get("download_name") or os.path.basename(resolved),
    )


@router.post("/config/propose")
async def invernia_config_propose(req: InvernIAConfigRequest):
    last_row = await _fetch_last()
    control_state = await _fetch_control_state()
    temp = last_row.get("dht22_1_TEMP_OUT")
    hum = last_row.get("dht22_1_HUM_OUT")

    proposal = {
        "mode": "suggestion-only",
        "reason": "Propuesta generada sin aplicar cambios reales",
        "current": {
            "temperature": temp,
            "humidity": hum,
            "controlState": control_state,
        },
        "suggested": {
            "relay_1": True if isinstance(temp, (int, float)) and temp > 30 else control_state.get("relay_1"),
            "relay_2": True if isinstance(hum, (int, float)) and hum < 55 else control_state.get("relay_2"),
            "vfd_1_state": True if isinstance(temp, (int, float)) and temp > 31 else control_state.get("vfd_1_state"),
            "vfd_1": 35 if isinstance(temp, (int, float)) and temp > 31 else control_state.get("vfd_1"),
        },
    }

    text = await _call_external_ai(req.prompt, req.context.model_dump(), {"proposal": proposal})
    if not text:
        text = "Propuesta orientativa generada a partir de temperatura, humedad y estado actual de control."

    return _response(True, "mixed", "Propuesta de configuración generada", text=text, config_proposal=proposal)


@router.post("/query")
async def invernia_query(req: InvernIAQueryRequest):
    intent = _infer_intent(req.prompt, req.payload)
    metrics = _infer_metrics(req.prompt, req.payload)
    windows = _normalize_windows(req.payload)
    max_points = min(max(int(req.options.max_points or 500), 50), 2000)

    if intent == "table":
        if "alert" in (req.prompt or "").lower() or (req.payload and req.payload.dataset == "alerts"):
            alerts = await _fetch_alerts(limit=req.payload.limit if req.payload and req.payload.limit else 100)
            table = _table_from_alerts(alerts)
            return _response(True, "table", "Tabla de alertas generada", table=table)

        rows = await _fetch_table_rows(limit=req.payload.limit if req.payload and req.payload.limit else DEFAULT_MAX_ROWS)
        table = _table_from_rows(rows, req.payload.columns if req.payload else None)
        return _response(True, "table", "Tabla de telemetría generada", table=table)

    if intent == "chart":
        window_data = await _fetch_window_dataset(metrics, windows, max_points=max_points)
        chart = _build_chart_payload(window_data, metrics, req.context)
        stats = []
        for label, rows in window_data.items():
            for metric in metrics:
                stat = _calc_metric_stats(rows, metric)
                stat["window"] = label
                stats.append(stat)
        text = await _call_external_ai(req.prompt, req.context.model_dump(), {"chart": chart, "stats": stats})
        if not text:
            text = f"Se generó un gráfico comparativo para {', '.join(metrics)} en {len(windows)} ventana(s)."
        return _response(True, "mixed", "Gráfico generado", text=text, chart=chart, stats=stats)

    if intent == "stats":
        window_data = await _fetch_window_dataset(metrics, windows, max_points=max_points)
        stats = []
        for label, rows in window_data.items():
            for metric in metrics:
                stat = _calc_metric_stats(rows, metric)
                stat["window"] = label
                stats.append(stat)
        snapshot = await _load_snapshot()
        text = await _call_external_ai(req.prompt, req.context.model_dump(), {"stats": stats, "snapshot": snapshot})
        if not text:
            text = "Resumen estadístico generado a partir de las ventanas solicitadas y snapshot reciente."
        return _response(True, "stats", "Estadísticas generadas", text=text, stats=stats)

    if intent == "export":
        rows = await _fetch_table_rows(limit=req.payload.limit if req.payload and req.payload.limit else DEFAULT_MAX_ROWS)
        export_fmt = (req.payload.export_format if req.payload and req.payload.export_format else req.options.prefer_export_format) or "xlsx"
        export = _make_export_file(rows, export_fmt, req.payload.title if req.payload and req.payload.title else "invernia_export", req.payload.columns if req.payload else None)
        text = f"Export listo en formato {export['format'].upper()} con {export['rows']} fila(s)."
        return _response(True, "mixed", "Export generado", text=text, export=export, table={"columns": export["columns"], "rows": rows[:20], "meta": {"preview": True}})

    if intent == "config":
        return await invernia_config_propose(InvernIAConfigRequest(prompt=req.prompt, context=req.context, payload=req.payload.model_dump() if req.payload else None))

    last_row = await _fetch_last()
    control_state = await _fetch_control_state()
    alerts = await _fetch_alerts(limit=20)
    snapshot = await _load_snapshot()
    system_context = {
        "last": last_row,
        "control": control_state,
        "alerts": alerts,
        "snapshot": snapshot,
    }
    text = await _call_external_ai(req.prompt, req.context.model_dump(), system_context)
    if not text:
        temp = last_row.get("dht22_1_TEMP_OUT")
        hum = last_row.get("dht22_1_HUM_OUT")
        text = f"Estado actual: temperatura={temp}, humedad={hum}, alertas recientes={len(alerts)}."
    return _response(True, "text", "Consulta resuelta", text=text)


def register_invernia_routes(app):
    app.include_router(router)
