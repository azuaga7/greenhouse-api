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

# HTML embebido para paridad total
INDEX_HTML = """<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ADTEC · Cloud Dashboard Pro</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/lucide@0.263.1/dist/umd/lucide.min.js"></script>
    <style>
        
    :root { 
        --accent: #38bdf8; --bg: #020617; --surface: rgba(15, 23, 42, 0.8); 
        --border: rgba(148, 163, 184, 0.2); --success: #22c55e; --warn: #eab308; --danger: #ef4444;
        --text: #f8fafc; --text-muted: #94a3b8;
        --adtec-accent: #38bdf8;
    }
    
    * { box-sizing: border-box; }
    body { 
        margin: 0; background: var(--bg); color: var(--text); 
        font-family: 'Inter', system-ui, -apple-system, sans-serif; min-height: 100vh;
        display: flex; flex-direction: column; align-items: center; padding: 40px 20px;
        background-image: radial-gradient(circle at top, #0f172a 0%, #020617 100%);
        overflow-x: hidden;
    }

    /* Glassmorphism Card */
    .adtec-card { 
        background: linear-gradient(145deg, rgba(15,23,42,0.97), rgba(15,23,42,0.7));
        border-radius: 12px; border: 1px solid rgba(148, 163, 184, 0.25);
        padding: 25px; position: relative; backdrop-filter: blur(20px);
        box-shadow: 0 10px 30px rgba(0, 0, 0, 0.5);
        display: flex; flex-direction: column;
    }

    .card-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; border-bottom: 1px solid rgba(148, 163, 184, 0.2); padding-bottom: 15px; }
    .logo-section { display: flex; align-items: center; gap: 12px; }
    .logo-icon { 
        width: 30px; height: 30px; background: #fbbf24; border-radius: 8px;
        display: flex; align-items: center; justify-content: center;
        box-shadow: 0 0 15px rgba(251, 191, 36, 0.4);
    }
    .logo-icon-inner { font-size: 9px; font-weight: 900; color: black; letter-spacing: -0.5px; }
    .card-title { font-weight: 600; font-size: 18px; letter-spacing: 0.05em; color: #f8fafc; margin: 0; }

    .tabs-nav { display: flex; gap: 8px; margin-bottom: 24px; overflow-x: auto; padding-bottom: 10px; width: 100%; max-width: 1000px; }
    .tab-btn { 
        padding: 12px 20px; cursor: pointer; border-radius: 10px; font-size: 13px; font-weight: 700;
        transition: all 0.2s; color: var(--text-muted); border: 1px solid transparent;
        display: flex; align-items: center; gap: 10px; white-space: nowrap; text-transform: uppercase;
    }
    .tab-btn:hover { background: rgba(255,255,255,0.05); color: var(--text); }
    .tab-btn.active { background: rgba(56, 189, 248, 0.1); border-color: rgba(56, 189, 248, 0.2); color: var(--accent); }

    .card-body { 
        position: relative; background: rgba(0,0,0,0.2); border-radius: 12px; 
        border: 1px solid rgba(148, 163, 184, 0.1); overflow: hidden; flex: 1;
        display: flex; flex-direction: column;
    }
    .grid-bg { 
        position: absolute; inset: 0; 
        background-image: radial-gradient(rgba(56, 189, 248, 0.05) 1px, transparent 1px); 
        background-size: 30px 30px; opacity: 0.5; pointer-events: none;
    }

    .widgets-area { position: relative; width: 100%; height: 100%; z-index: 1; flex: 1; min-height: 500px; }

    .adtec-label-widget {
        pointer-events: none;
        white-space: nowrap;
        font-family: 'Inter', sans-serif;
        letter-spacing: -0.02em;
    }

    .adtec-status-item { 
        position: absolute; 
        background: rgba(15, 23, 42, 0.6);
        border: 1px solid rgba(148, 163, 184, 0.1);
        border-radius: 12px; 
        padding: 12px 14px; 
        display: flex; 
        flex-direction: column;
        transition: all 0.3s ease; 
        backdrop-filter: blur(10px);
        box-sizing: border-box; 
        gap: 4px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
    }
    .adtec-status-item:hover {
        border-color: var(--accent);
        box-shadow: 0 0 15px rgba(56, 189, 248, 0.15);
    }
    .adtec-status-item h3 { 
        font-size: 10px; 
        text-transform: uppercase; 
        color: #94a3b8; 
        margin: 0; 
        font-weight: 700; 
        letter-spacing: 0.05em; 
    }
    .adtec-status-value { font-size: 20px; font-weight: 700; color: #f8fafc; margin: 5px 0; }
    .adtec-status-sub { font-size: 10px; color: #9ca3af; }
    
    .adtec-temp-bar { width: 100%; height: 6px; background: rgba(15,23,42,0.8); border: 1px solid rgba(148, 163, 184, 0.2); border-radius: 4px; margin-top: 5px; overflow: hidden; position: relative; }
    .adtec-temp-bar-fill { height: 100%; width: 0%; transition: all 0.5s cubic-bezier(0.4, 0, 0.2, 1); background: var(--accent); box-shadow: 0 0 10px rgba(56, 189, 248, 0.4); }

    .adtec-status-chip { display: inline-flex; align-items: center; gap: 6px; padding: 3px 10px; border-radius: 999px; font-size: 10px; font-weight: 700; margin-top: auto; text-transform: uppercase; border: 1px solid rgba(148, 163, 184, 0.3); letter-spacing: 0.5px; }
    .adtec-status-chip.on { background: rgba(34,197,94,0.1); color: #22c55e; border-color: rgba(34,197,94,0.4); }
    .adtec-status-chip.off { background: rgba(239,68,68,0.1); color: #ef4444; border-color: rgba(239,68,68,0.4); }
    .adtec-status-dot { width: 7px; height: 7px; border-radius: 50%; background: #9ca3af; }
    .adtec-status-dot.on { background: #22c55e; box-shadow: 0 0 10px #22c55e; }
    .adtec-status-dot.off { background: #ef4444; box-shadow: 0 0 10px #ef4444; }

    .control-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 25px; padding: 25px; }
    .control-column { display: flex; flex-direction: column; gap: 12px; background: rgba(255,255,255,0.03); padding: 20px; border-radius: 12px; border: 1px solid rgba(148,163,184,0.1); }
    .control-column h4 { font-size: 14px; color: #f8fafc; margin: 0 0 20px 0; display: flex; align-items: center; gap: 10px; }
    
    .actuator-card { background: #1e293b; padding: 12px; border-radius: 8px; border: 1px solid rgba(148, 163, 184, 0.1); display: flex; justify-content: space-between; align-items: center; }
    .actuator-info { display: flex; flex-direction: column; }
    .actuator-info .name { font-size: 13px; font-weight: 600; color: #f8fafc; }
    .actuator-info .status { font-size: 10px; font-weight: 600; }

    .switch { position: relative; display: inline-block; width: 44px; height: 24px; transform: scale(0.7); }
    .switch input { opacity: 0; width: 0; height: 0; }
    .slider { position: absolute; cursor: pointer; inset: 0; background-color: #334155; transition: .4s; border-radius: 24px; }
    .slider:before { position: absolute; content: ""; height: 18px; width: 18px; left: 3px; bottom: 3px; background-color: white; transition: .4s; border-radius: 50%; }
    input:checked + .slider { background-color: var(--accent); }
    input:checked + .slider:before { transform: translateX(20px); }

    .vfd-card-inner { display: flex; flexDirection: column; gap: 20px; }
    .vfd-gauge { align-self: center; width: 140px; height: 140px; border-radius: 50%; border: 8px solid #1e293b; position: relative; display: flex; align-items: center; justify-content: center; background: rgba(0,0,0,0.2); transition: all 0.3s; }
    .vfd-gauge.on { border-top-color: var(--adtec-accent); box-shadow: 0 0 20px rgba(56,189,248,0.1); }
    .vfd-value { text-align: center; }
    .vfd-value .num { font-size: 24px; font-weight: 900; color: white; }
    .vfd-value.on .num { color: var(--adtec-accent); }
    .vfd-value .label { font-size: 10px; color: #9ca3af; }

    .vfd-controls { display: flex; gap: 10px; margin-top: 10px; }
    .vfd-btn { flex: 1; padding: 12px; border-radius: 8px; border: 1px solid rgba(148,163,184,0.1); color: white; font-weight: 800; font-size: 11px; cursor: pointer; transition: all 0.2s; background: rgba(255,255,255,0.05); }
    .vfd-btn.start { background: #10b981; }
    .vfd-btn.stop { background: #ef4444; }

    .safety-notice { margin: 25px; padding: 15px 20px; background: rgba(250,204,21,0.05); border-radius: 10px; border: 1px solid rgba(250,204,21,0.2); display: flex; gap: 15px; align-items: center; }
    .safety-notice p { margin: 0; font-size: 11px; color: #fbbf24; line-height: 1.5; }

    .chart-header { display: flex; justify-content: space-between; align-items: center; padding: 15px 20px; border-bottom: 1px solid rgba(148, 163, 184, 0.2); }
    .chart-controls-wrapper { background: rgba(255,255,255,0.03); padding: 15px; border-radius: 12px; border: 1px solid rgba(148,163,184,0.1); margin: 20px; }
    .chart-controls-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 15px; margin-bottom: 15px; }
    .chart-controls-bottom { display: grid; grid-template-columns: 1fr 1fr 1.5fr; gap: 15px; }
    
    .form-group label { display: block; font-size: 10px; color: #9ca3af; text-transform: uppercase; margin-bottom: 5px; font-weight: 600; }
    .form-group select, .form-group input { width: 100%; background: #1e293b; color: white; border: 1px solid rgba(148,163,184,0.2); padding: 8px; border-radius: 6px; font-size: 12px; outline: none; }
    .form-group input[type="datetime-local"] { font-size: 11px; }

    .range-buttons { display: flex; gap: 5px; }
    .range-btn { flex: 1; padding: 8px 4px; border-radius: 6px; border: 1px solid rgba(148,163,184,0.1); background: #1e293b; color: #9ca3af; font-size: 10px; font-weight: 700; cursor: pointer; transition: all 0.2s; }
    .range-btn.active { background: rgba(56,189,248,0.1); border-color: var(--adtec-accent); color: var(--adtec-accent); }

    .view-buttons { display: flex; gap: 5px; }
    .view-btn { flex: 1; padding: 8px; border-radius: 6px; border: 1px solid rgba(148,163,184,0.1); background: #1e293b; color: #9ca3af; font-size: 11px; cursor: pointer; display: flex; align-items: center; justify-content: center; gap: 4px; transition: all 0.2s; }
    .view-btn.active { background: rgba(56,189,248,0.1); border-color: var(--adtec-accent); color: var(--adtec-accent); }

    .chart-area { background: rgba(0,0,0,0.3); border-radius: 12px; border: 1px solid rgba(148,163,184,0.1); padding: 40px 100px 80px 100px; min-height: 400px; position: relative; display: flex; flex-direction: column; margin: 0 20px 20px 20px; }
    .chart-grid-lines { position: absolute; inset: 0; background-image: linear-gradient(rgba(148,163,184,0.05) 1px, transparent 1px), linear-gradient(90deg, rgba(148,163,184,0.05) 1px, transparent 1px); background-size: 40px 40px; pointer-events: none; }

    </style>
</head>
<body style="margin:0; padding:0; background:#020617; color:#f8fafc; font-family:'Inter', sans-serif;">
    <div id="dashboard-root"></div>
    <script>
        // Configuración Maestra exportada desde el Studio
        const CONFIG = {
            dashboard: {"tabs":[{"id":"1","name":"Estado Actual","icon":"📄","title":"Estado Actual del Invernadero","width":970,"height":900,"type":"dashboard"},{"id":"2","name":"Grafico","icon":"📄","title":"Laboratorio de Gráficos","width":1000,"height":700,"type":"charts"},{"id":"3","name":"Control","icon":"📄","title":"Control Remoto de Invernadero","width":700,"height":800,"type":"control"}],"widgets":[{"id":"w_1768270825285","type":"gauge","label":"Humedad del Invernadero","key":"dht22_1_HUM_OUT","unit":"%","min":0,"max":100,"x":240,"y":10,"w":220,"h":180,"tabId":"1","format":"fixed1"},{"id":"w_1768270831388","type":"gauge","label":"Temperatura del Invernadero","key":"dht22_1_TEMP_OUT","unit":"°C","min":0,"max":100,"x":10,"y":10,"w":220,"h":180,"tabId":"1","format":"fixed1"},{"id":"w_1768270903173","type":"gauge","label":"Temperatura Exterior","key":"ds18b20_2_TEMP_OUT","unit":"°C","min":0,"max":100,"x":10,"y":210,"w":220,"h":180,"tabId":"1","format":"fixed1"},{"id":"w_1768270908326","type":"gauge","label":"Temperatura de Pozo","key":"ds18b20_1_TEMP_OUT","unit":"°C","min":0,"max":100,"x":240,"y":210,"w":220,"h":180,"tabId":"1","format":"fixed1"},{"id":"w_1768270938829","type":"gauge","label":"Frecuencia Ventiladores Pared","key":"vfd_1_FREQ_OUT","unit":"Hz","min":0,"max":100,"x":10,"y":410,"w":220,"h":180,"tabId":"1","format":"fixed1","inverted":false},{"id":"w_1768270972142","type":"gauge","label":"Luxes","key":"tsl2561_1_LUX_OUT","unit":"LX","min":0,"max":100,"x":240,"y":410,"w":220,"h":180,"tabId":"1","format":"fixed1"},{"id":"w_1768270996596","type":"indicator","label":"Estado Bomba de Agua","key":"relay_3_STATE_OUT","min":0,"max":100,"x":500,"y":120,"w":180,"h":80,"tabId":"1","format":"bool_onoff"},{"id":"w_1768271000492","type":"location","label":"Ubicación","key":"sin_key","min":0,"max":100,"x":700,"y":610,"w":200,"h":85,"tabId":"1","format":"fixed1"},{"id":"w_1768271000964","type":"datetime","label":"Reloj Sistema","key":"timestamp","min":0,"max":100,"x":480,"y":10,"w":220,"h":85,"tabId":"1","format":"raw"},{"id":"w_1768271001285","type":"connection","label":"Estado Red","key":"gsm_signal","min":0,"max":100,"x":720,"y":10,"w":180,"h":85,"tabId":"1","format":"raw"},{"id":"w_1768271019292","type":"indicator","label":"Estado de Ventiladores Axiales","key":"vfd_1_STATE_OUT","min":0,"max":100,"x":500,"y":450,"w":180,"h":80,"tabId":"1","format":"bool_onoff"},{"id":"w_1768271019940","type":"indicator","label":"Estado Vent. 2","key":"relay_2_STATE_OUT","min":0,"max":100,"x":500,"y":340,"w":180,"h":80,"tabId":"1","format":"bool_onoff"},{"id":"w_1768271020388","type":"indicator","label":"Estado Vent. 1","key":"relay_1_STATE_OUT","min":0,"max":100,"x":500,"y":230,"w":180,"h":80,"tabId":"1","format":"bool_onoff"},{"id":"w_1768271034349","type":"text","label":"Uso Ventiladores Axiales","key":"vfd_1_RUNTIME_OUT","unit":"","min":0,"max":100,"x":700,"y":450,"w":200,"h":90,"tabId":"1","format":"time_hms"},{"id":"w_1768271034900","type":"text","label":"Uso Ventiladores 3 - 4","key":"relay_2_RUNTIME_OUT","unit":"","min":0,"max":100,"x":700,"y":340,"w":200,"h":90,"tabId":"1","format":"time_hms"},{"id":"w_1768271035300","type":"text","label":"Uso Ventiladores 1 - 2","key":"relay_1_RUNTIME_OUT","unit":"","min":0,"max":100,"x":700,"y":230,"w":200,"h":90,"tabId":"1","format":"time_hms"},{"id":"w_1768271035668","type":"text","label":"Uso Bomba de Agua","key":"relay_3_RUNTIME_OUT","unit":"","min":0,"max":100,"x":700,"y":120,"w":200,"h":90,"tabId":"1","format":"time_hms"},{"id":"w_1768271685572","type":"control-relay","label":"Estado Bomba de Agua","key":"relay_3_STATE_OUT","target":"relay_3","min":0,"max":100,"x":30,"y":10,"w":220,"h":120,"tabId":"3","format":"fixed1"},{"id":"w_1768271690253","type":"control-relay","label":"Estado Vent. 1","key":"relay_1_STATE_OUT","target":"relay_1","min":0,"max":100,"x":30,"y":140,"w":220,"h":120,"tabId":"3","format":"fixed1"},{"id":"w_1768271692422","type":"control-relay","label":"Estado Vent. 2","key":"relay_2_STATE_OUT","target":"relay_2","min":0,"max":100,"x":30,"y":270,"w":220,"h":120,"tabId":"3","format":"fixed1"},{"id":"w_1768271697991","type":"control-vfd","label":"Frecuencia Ventiladores Pared","key":"vfd_1_FREQ_OUT","target":"vfd_1","min":0,"max":100,"x":330,"y":110,"w":220,"h":280,"tabId":"3","format":"fixed1"},{"id":"w_1768272928661","type":"control-relay","label":"Estado de Ventiladores Axiales","key":"vfd_1_STATE_OUT","target":"vfd_1","min":0,"max":100,"x":30,"y":400,"w":220,"h":140,"tabId":"3","format":"fixed1"}],"variables":[{"key":"dht22_1_HUM_OUT","label":"Humedad del Invernadero","dataType":"HUM","isDataEmitter":true},{"key":"dht22_1_TEMP_OUT","label":"Temperatura del Invernadero","dataType":"TEMP","isDataEmitter":true},{"key":"ds18b20_2_TEMP_OUT","label":"Temperatura Exterior","dataType":"TEMP","isDataEmitter":true},{"key":"relay_3_STATE_OUT","label":"Estado Bomba de Agua","dataType":"STATE","isDataEmitter":false},{"key":"relay_1_STATE_OUT","label":"Estado Vent. 1","dataType":"STATE","isDataEmitter":false},{"key":"relay_2_STATE_OUT","label":"Estado Vent. 2","dataType":"STATE","isDataEmitter":false},{"key":"vfd_1_FREQ_OUT","label":"Frecuencia Ventiladores Pared","dataType":"","isDataEmitter":true},{"key":"relay_3_RUNTIME_OUT","label":"Uso Bomba de Agua","dataType":"","isDataEmitter":false},{"key":"relay_1_RUNTIME_OUT","label":"Uso Ventiladores 1 - 2","dataType":"","isDataEmitter":false},{"key":"relay_2_RUNTIME_OUT","label":"Uso Ventiladores 3 - 4","dataType":"","isDataEmitter":false},{"key":"vfd_1_STATE_OUT","label":"Estado de Ventiladores Axiales","dataType":"STATE","isDataEmitter":true},{"key":"vfd_1_RUNTIME_OUT","label":"Uso Ventiladores Axiales","dataType":"","isDataEmitter":true},{"key":"ds18b20_1_TEMP_OUT","label":"Temperatura de Pozo","dataType":"TEMP","isDataEmitter":true},{"key":"tsl2561_1_LUX_OUT","label":"Luxes","dataType":"LUX","isDataEmitter":true}],"actuators":[{"id":"relay_1","label":"relay_1","type":"ACT_RELAY_SINGLE"},{"id":"relay_2","label":"relay_2","type":"ACT_RELAY_SINGLE"},{"id":"vfd_1","label":"vfd_1","type":"ACT_VFD_MODBUS"},{"id":"relay_3","label":"relay_3","type":"ACT_RELAY_SINGLE"}]},
            labels: {"dht22_1_HUM_OUT":"Humedad del Invernadero","dht22_1_TEMP_OUT":"Temperatura del Invernadero","ds18b20_2_TEMP_OUT":"Temperatura Exterior","relay_3_STATE_OUT":"Estado Bomba de Agua","relay_1_STATE_OUT":"Estado Vent. 1","relay_2_STATE_OUT":"Estado Vent. 2","vfd_1_FREQ_OUT":"Frecuencia Ventiladores Pared","relay_3_RUNTIME_OUT":"Uso Bomba de Agua","relay_1_RUNTIME_OUT":"Uso Ventiladores 1 - 2","relay_2_RUNTIME_OUT":"Uso Ventiladores 3 - 4","vfd_1_STATE_OUT":"Estado de Ventiladores Axiales","vfd_1_RUNTIME_OUT":"Uso Ventiladores Axiales","ds18b20_1_TEMP_OUT":"Temperatura de Pozo","tsl2561_1_LUX_OUT":"Luxes"},
            refreshInterval: 3000
        };
        
        // Lógica del Motor de Renderizado (ADTEC Engine)
        
    let activeTabId = CONFIG.dashboard.tabs[0].id;
    let lastData = {};
    let controlState = {};
    let historyData = [];
    let charts = {}; // Store Chart.js instances

    // ==================== ENGINE CORE ====================
    
    function render() {
        const root = document.getElementById('dashboard-root');
        const tab = CONFIG.dashboard.tabs.find(t => String(t.id) === String(activeTabId));
        if (!root || !tab) return;
        
        const widgets = CONFIG.dashboard.widgets.filter(w => String(w.tabId) === String(activeTabId));
        
        let html = '<div class="adtec-card" style="width:' + tab.width + 'px; min-height:' + tab.height + 'px">';
        html += '<div class="card-header">';
        html += '<div class="logo-section">';
        html += '<div class="logo-icon"><div class="logo-icon-inner">ADTEC</div></div>';
        html += '<span class="card-title visual-id-header">' + tab.title + '</span>';
        html += '</div>';
        html += renderHeaderRight(tab);
        html += '</div>';
        
        html += '<div class="tabs-nav">';
        CONFIG.dashboard.tabs.forEach(t => {
            const activeClass = String(t.id) === String(activeTabId) ? 'active' : '';
            html += '<div class="tab-btn ' + activeClass + '" onclick="switchTab(\'' + t.id + '\')">';
            html += t.icon + ' ' + t.name;
            html += '</div>';
        });
        html += '</div>';
        
        html += '<div class="card-body">';
        html += '<div class="grid-bg"></div>';
        html += '<div class="widgets-area">';
        widgets.forEach(w => {
            html += renderWidgetMarkup(w);
        });
        html += '</div>';
        html += '</div>';
        html += '</div>';
        
        root.innerHTML = html;
        
        // Post-render: Initialize charts
        widgets.forEach(w => {
            if (w.type === 'chart' || w.type === 'summary-chart') {
                initChart(w);
            }
        });

        updateUI();
        if (window.lucide) lucide.createIcons();
    }

    function renderHeaderRight(tab) {
        if (tab.type === 'control') {
            return '<div style="display:flex; align-items:center; gap:12px; background:rgba(255,255,255,0.05); padding:5px 15px; border-radius:20px; border: 1px solid rgba(148,163,184,0.1);">' +
                   '<span id="manual-label" style="font-size:9px; font-weight:700; color: #9ca3af">AUTO</span>' +
                   '<label class="switch">' +
                   '<input type="checkbox" id="master_manual" onchange="sendControl(\'manual\', this.checked)">' +
                   '<span class="slider"></span>' +
                   '</label>' +
                   '</div>';
        }
        return '<div class="adtec-status-chip on" id="conn-status"><span class="adtec-status-dot on"></span> ONLINE</div>';
    }

    function renderWidgetMarkup(w) {
        const style = 'left:' + w.x + 'px; top:' + w.y + 'px; width:' + w.w + 'px; height:' + w.h + 'px;';
        
        let content = '';
        switch(w.type) {
            case 'gauge':
                content = '<div class="adtec-status-value" id="val_' + w.key + '">--</div>' +
                          '<div class="adtec-temp-bar">' +
                          '<div class="adtec-temp-bar-fill" id="bar_' + w.key + '" style="width: 0%;"></div>' +
                          '</div>';
                break;
            case 'indicator':
                content = '<div class="adtec-status-chip off" id="chip_' + w.key + '">' +
                          '<span class="adtec-status-dot off" id="dot_' + w.key + '"></span>' +
                          '<span id="label_' + w.key + '">APAGADO</span>' +
                          '</div>';
                break;
            case 'control-relay':
                content = '<div style="display:flex; flex-direction:column; gap:10px; margin-top:10px;">' +
                          '<div class="adtec-status-chip off" id="chip_' + w.key + '">' +
                          '<span class="adtec-status-dot off" id="dot_' + w.key + '"></span>' +
                          '<span id="label_' + w.key + '">INACTIVO</span>' +
                          '</div>' +
                          '<div style="display:flex; gap:5px;">' +
                          '<button class="vfd-btn start" id="btn_on_' + w.id + '" onclick="sendControl(\'' + w.target + '\', true)">ON</button>' +
                          '<button class="vfd-btn stop" id="btn_off_' + w.id + '" onclick="sendControl(\'' + w.target + '\', false)">OFF</button>' +
                          '</div>' +
                          '</div>';
                break;
            case 'control-vfd':
                content = '<div class="vfd-card-inner" style="align-items:center;">' +
                          '<div class="vfd-gauge" id="gauge_' + w.id + '" style="width:80px; height:80px;">' +
                          '<div class="vfd-value">' +
                          '<div class="num" id="vfd_val_' + w.id + '">0.0</div>' +
                          '<div class="label">Hz</div>' +
                          '</div>' +
                          '</div>' +
                          '<input type="range" min="0" max="60" step="0.5" id="ctrl_' + w.id + '" ' +
                          'style="width:100%;" ' +
                          'oninput="document.getElementById(\'vfd_val_' + w.id + '\').innerText = parseFloat(this.value).toFixed(1)" ' +
                          'onchange="sendControl(\'' + w.target + '\', parseFloat(this.value))">' +
                          '<div class="vfd-controls">' +
                          '<button class="vfd-btn start" onclick="sendControl(\'' + w.target + '_state\', true)">RUN</button>' +
                          '<button class="vfd-btn stop" onclick="sendControl(\'' + w.target + '_state\', false)">STOP</button>' +
                          '</div>' +
                          '</div>';
                break;
            case 'chart':
            case 'summary-chart':
                content = '<canvas id="chart_' + w.id + '" style="width:100%; height:100%;"></canvas>';
                break;
            case 'connection':
                content = '<div style="display:flex; align-items:center; gap:10px; margin-top:10px;">' +
                          '<div style="display:flex; gap:2px; align-items:flex-end; height:20px;" id="gsm_' + w.key + '">' +
                          [1,2,3,4,5].map(() => '<div style="width:3px; height:4px; background:rgba(255,255,255,0.1); border-radius:1px;"></div>').join('') +
                          '</div>' +
                          '<div class="adtec-status-chip off" id="chip_' + w.key + '">' +
                          '<span class="adtec-status-dot off" id="dot_' + w.key + '"></span>' +
                          '<span id="label_' + w.key + '">GSM</span>' +
                          '</div>' +
                          '</div>';
                break;
            case 'label':
                return '<div class="adtec-label-widget" style="position:absolute;' + style + ' color:' + (w.color || 'white') + '; font-size:' + (w.fontSize || 14) + 'px; font-weight:bold; display:flex; align-items:center;">' + w.label + '</div>';
            default:
                content = '<div class="adtec-status-value" id="val_' + w.key + '">--</div>';
        }

        return '<div class="adtec-status-item" style="' + style + '" id="widget_' + w.id + '">' +
               '<div style="display:flex; justify-content:space-between; align-items:center;">' +
               '<h3>' + w.label + '</h3>' +
               '</div>' +
               content +
               '</div>';
    }

    // ==================== CHART LOGIC ====================

    function initChart(w) {
        const ctx = document.getElementById('chart_' + w.id).getContext('2d');
        const isSummary = w.type === 'summary-chart';
        
        const chart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: w.label,
                    data: [],
                    borderColor: '#38bdf8',
                    backgroundColor: 'rgba(56, 189, 248, 0.1)',
                    borderWidth: 2,
                    fill: isSummary,
                    tension: 0.4,
                    pointRadius: isSummary ? 2 : 0
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    x: { display: isSummary, grid: { display: false }, ticks: { color: '#94a3b8', font: { size: 10 } } },
                    y: { display: isSummary, grid: { color: 'rgba(148, 163, 184, 0.1)' }, ticks: { color: '#94a3b8', font: { size: 10 } } }
                }
            }
        });
        charts[w.id] = chart;
    }

    function updateCharts() {
        if (!historyData || historyData.length === 0) return;
        
        CONFIG.dashboard.widgets.forEach(w => {
            const chart = charts[w.id];
            if (!chart) return;

            const key = w.key || (w.type === 'summary-chart' ? (CONFIG.dashboard.variables[0]?.key) : null);
            if (!key) return;

            const values = historyData.map(d => parseFloat(d[key]) || 0);
            const labels = historyData.map(d => d.timestamp?.split('T')[1]?.split('.')[0] || '');

            chart.data.labels = labels;
            chart.data.datasets[0].data = values;
            chart.update('none'); // No animation for performance
        });
    }

    // ==================== DATA & UI SYNC ====================

    function formatValue(val, format) {
        if (val === undefined || val === null) return '--';
        if (format === 'time_hm' || format === 'time_hms') {
            let totalSeconds = Math.floor(parseFloat(val) || 0);
            const h = Math.floor(totalSeconds / 3600);
            const m = Math.floor((totalSeconds % 3600) / 60);
            const s = totalSeconds % 60;
            if (format === 'time_hm') return h + 'h ' + m + 'm';
            return h + ':' + String(m).padStart(2, '0') + ':' + String(s).padStart(2, '0');
        }
        if (format === 'fixed2') return !isNaN(parseFloat(val)) ? parseFloat(val).toFixed(2) : val;
        if (format === 'fixed1') return !isNaN(parseFloat(val)) ? parseFloat(val).toFixed(1) : val;
        if (format === 'percent') return !isNaN(parseFloat(val)) ? parseFloat(val).toFixed(0) + '%' : val;
        if (format === 'bool_onoff') return (val === true || val === 1 || String(val).toUpperCase() === 'ON' || val === '1.00') ? 'ON' : 'OFF';
        return val;
    }

    function updateUI() {
        const isManual = controlState.manual === true || controlState.manual === 'ON' || controlState.manual === 1;
        const masterManual = document.getElementById('master_manual');
        if (masterManual) masterManual.checked = isManual;
        const manualLabel = document.getElementById('manual-label');
        if (manualLabel) { 
            manualLabel.innerText = isManual ? 'MANUAL' : 'AUTO'; 
            manualLabel.style.color = isManual ? '#38bdf8' : '#9ca3af'; 
        }

        CONFIG.dashboard.widgets.forEach(w => {
            const val = lastData[w.key];
            const valEl = document.getElementById('val_' + w.key);
            if (valEl) valEl.innerText = formatValue(val, w.format) + (w.unit || '');

            if (w.type === 'gauge' && val !== undefined) {
                const bar = document.getElementById('bar_' + w.key);
                if (bar) bar.style.width = ((parseFloat(val) - (w.min || 0)) / ((w.max || 100) - (w.min || 0)) * 100) + '%';
            }
            if (w.type === 'indicator') {
                const isOn = val === true || val === 1 || val === '1' || val === '1.00' || String(val).toUpperCase() === 'ON' || String(val).toUpperCase() === 'TRUE';
                const chip = document.getElementById('chip_' + w.key);
                const dot = document.getElementById('dot_' + w.key);
                const label = document.getElementById('label_' + w.key);
                if (chip) chip.className = 'adtec-status-chip ' + (isOn ? 'on' : 'off');
                if (dot) dot.className = 'adtec-status-dot ' + (isOn ? 'on' : 'off');
                if (label) label.innerText = isOn ? 'ENCENDIDO' : 'APAGADO';
            }
            if (w.type === 'control-relay') {
                const isOn = controlState[w.target] === true || controlState[w.target] === 'ON';
                const chip = document.getElementById('chip_' + w.key);
                if (chip) chip.className = 'adtec-status-chip ' + (isOn ? 'on' : 'off');
                const btnOn = document.getElementById('btn_on_' + w.id);
                const btnOff = document.getElementById('btn_off_' + w.id);
                if (btnOn) { btnOn.disabled = !isManual; btnOn.style.opacity = isManual ? 1 : 0.5; }
                if (btnOff) { btnOff.disabled = !isManual; btnOff.style.opacity = isManual ? 1 : 0.5; }
            }
            if (w.type === 'control-vfd') {
                const freq = parseFloat(controlState[w.target]) || 0;
                const isOn = controlState[w.target + '_state'] === true || controlState[w.target + '_state'] === 'ON';
                const numEl = document.getElementById('vfd_val_' + w.id);
                if (numEl) numEl.innerText = freq.toFixed(1);
                const ctrl = document.getElementById('ctrl_' + w.id);
                if (ctrl) { ctrl.value = freq; ctrl.disabled = !isManual; ctrl.style.opacity = isManual ? 1 : 0.5; }
                const gauge = document.getElementById('gauge_' + w.id);
                if (gauge) { isOn ? gauge.classList.add('on') : gauge.classList.remove('on'); }
            }
            if (w.type === 'connection') {
                const isOnline = val === 'ONLINE' || val === '1' || val === 1 || val === true;
                const chip = document.getElementById('chip_' + w.key);
                const dot = document.getElementById('dot_' + w.key);
                const label = document.getElementById('label_' + w.key);
                const gsm = document.getElementById('gsm_' + w.key);
                if (chip) chip.className = 'adtec-status-chip ' + (isOnline ? 'on' : 'off');
                if (dot) dot.className = 'adtec-status-dot ' + (isOnline ? 'on' : 'off');
                if (label) label.innerText = isOnline ? 'GSM ONLINE' : 'DESCONECTADO';
                if (gsm) {
                    Array.from(gsm.children).forEach((node, i) => {
                        node.style.background = (isOnline && i < 5) ? '#38bdf8' : 'rgba(255,255,255,0.1)';
                    });
                }
            }
        });
        updateCharts();
    }

    async function sendControl(key, val) {
        try {
            await fetch('/api/control_state', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ [key]: val })
            });
            controlState[key] = val;
            updateUI();
        } catch(e) { console.error(e); }
    }

    async function fetchUpdate() {
        try {
            const [rData, rCtrl] = await Promise.all([
                fetch('/api/data'), 
                fetch('/api/control_state')
            ]);
            historyData = await rData.json();
            controlState = await rCtrl.json();
            if (historyData && historyData.length > 0) lastData = historyData[historyData.length - 1];
            updateUI();
        } catch(e) { console.error("Fetch error:", e); }
    }

    function switchTab(id) { activeTabId = id; render(); }
    
    setInterval(fetchUpdate, CONFIG.refreshInterval);
    window.onload = () => { render(); fetchUpdate(); };

    </script>
</body>
</html>"""

class Lectura(BaseModel):
    class Config:
        extra = "allow"

# Inicialización automática de CSV si no existe
if not os.path.exists(CSV_FILE):
    try:
        with open(CSV_FILE, "w", encoding="utf-8") as f:
            f.write("timestamp,Humedad del Invernadero,Temperatura del Invernadero,Temperatura Exterior,Estado Bomba de Agua,Estado Vent. 1,Estado Vent. 2,Frecuencia Ventiladores Pared,Uso Bomba de Agua,Uso Ventiladores 1 - 2,Uso Ventiladores 3 - 4,Estado de Ventiladores Axiales,Uso Ventiladores Axiales,Temperatura de Pozo,Luxes\n")
    except Exception as e:
        print(f"Error al crear CSV inicial: {e}")

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
    return HTMLResponse(content=INDEX_HTML)

@app.post("/api/ingreso")
async def api_ingreso(lectura: Lectura):
    global HISTORY
    data = lectura.dict()
    if "timestamp" not in data:
        data["timestamp"] = datetime.now().isoformat()
        
    HISTORY.append(data)
    if len(HISTORY) > 500: HISTORY.pop(0)
    
    try:
        # Usar etiquetas legibles para el CSV
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
        CONTROL_STATE[k] = v
    return CONTROL_STATE

if __name__ == "__main__":
    import uvicorn
    import sys
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
