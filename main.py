# main.py
import os
import sys
import json
import importlib
import pathlib
import uuid
from types import ModuleType
from typing import Any, Dict, Optional, Tuple

from fastapi import FastAPI, Header, HTTPException, Request, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ----------------------------
# Env / Config
# ----------------------------
STUDIO_SECRET = os.getenv("STUDIO_SECRET", "")
ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "").split(",") if o.strip()]
ALLOWED_ORIGIN_REGEX = os.getenv("ALLOWED_ORIGIN_REGEX", "")

# ----------------------------
# Minimal in-memory artifact store (shim)
# Must be registered in sys.modules BEFORE importing Tools.*
# ----------------------------
_ART_STORE: Dict[str, Any] = {}

def _save_artifact(obj: Any, name: str) -> str:
    key = f"mem:{name}:{uuid.uuid4().hex}"
    _ART_STORE[key] = obj
    return key

def _load_artifact(key: str) -> Any:
    return _ART_STORE.get(key)

# Register shim as `waveflow.artifacts`
if "waveflow.artifacts" not in sys.modules:
    artifacts_mod = ModuleType("artifacts")
    artifacts_mod.save_artifact = _save_artifact
    artifacts_mod.load_artifact = _load_artifact
    pkg_mod = ModuleType("waveflow")
    sys.modules["waveflow"] = pkg_mod
    sys.modules["waveflow.artifacts"] = artifacts_mod

# ----------------------------
# Make Tools importable (case-sensitive on Linux)
# ----------------------------
BASE_DIR = pathlib.Path(__file__).parent.resolve()
TOOLS_DIR = BASE_DIR / "Tools"
if TOOLS_DIR.exists():
    sys.path.insert(0, str(TOOLS_DIR))
    sys.path.insert(0, str(BASE_DIR))

def _import_first(candidates: Tuple[Tuple[str, str], ...]):
    errors = []
    for mod_name, attr in candidates:
        try:
            mod = importlib.import_module(mod_name)
            fn = getattr(mod, attr)
            print(f"[import-ok] {mod_name}.{attr}")
            return fn
        except Exception as e:
            errors.append(f"{mod_name}.{attr}: {e}")
    raise ImportError("Unable to import any of:\n  - " + "\n  - ".join(errors))

# ----------------------------
# Import orchestration tools
# ----------------------------
sheets_fetch_stage = _import_first((
    ("Tools.GoogleSheetsFetch", "sheets_fetch_stage"),
    ("Tools.GooglesheetsFetch", "sheets_fetch_stage"),
    ("GoogleSheetsFetch", "sheets_fetch_stage"),
    ("GooglesheetsFetch", "sheets_fetch_stage"),
))
dq_and_fe = _import_first((
    ("Tools.Data_quality_featureEngineer", "dq_and_fe"),
    ("Tools.data_quality_fe", "dq_and_fe"),
    ("data_quality_fe", "dq_and_fe"),
))
plan_and_forecast = _import_first((
    ("Tools.Model_planning_forecasting", "plan_and_forecast"),
    ("Tools.model_planning", "plan_and_forecast"),
    ("model_planning", "plan_and_forecast"),
))
aggregate_and_drivers = _import_first((
    ("Tools.Aggregator_Drivers", "aggregate_and_drivers"),
    ("Tools.aggregator", "aggregate_and_drivers"),
    ("aggregator", "aggregate_and_drivers"),
))
ui_pack_and_persist = _import_first((
    ("Tools.UI_Packager_persist", "ui_pack_and_persist"),
    ("Tools.UIPackager_persist", "ui_pack_and_persist"),
    ("UI_Packager_persist", "ui_pack_and_persist"),
    ("UIPackager_persist", "ui_pack_and_persist"),
))

# ----------------------------
# FastAPI & CORS
# ----------------------------
app = FastAPI(title="SANKALP Backend", version="1.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS or ["*"],
    allow_origin_regex=ALLOWED_ORIGIN_REGEX or None,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------------------------
# Models
# ----------------------------
class RunPayload(BaseModel):
    google: Optional[Dict[str, Any]] = None
    input: Optional[Dict[str, Any]] = None
    workflow_id: Optional[str] = None
    class Config:
        extra = "allow"

# ----------------------------
# Startup log
# ----------------------------
@app.on_event("startup")
async def _startup():
    print("== Registered routes ==")
    for r in app.routes:
        path = getattr(r, "path", "")
        methods = ",".join(getattr(r, "methods", []) or [])
        print(f"{r.name:20s} {path:25s} [{methods}]")
    print(f"Tools dir exists: {TOOLS_DIR.exists()} at {TOOLS_DIR}")

# ----------------------------
# Health
# ----------------------------
@app.get("/")
async def root():
    return {"ok": True, "service": "SANKALP backend", "docs": "/docs"}

@app.get("/health")
async def health():
    return {"ok": True, "status": "healthy"}

# ----------------------------
# Orchestrated run endpoint
# ----------------------------
@app.post("/run")
async def run_endpoint(
    payload: RunPayload,
    request: Request,
    x_studio_secret: Optional[str] = Header(None),
):
    # Auth
    if not STUDIO_SECRET:
        raise HTTPException(status_code=500, detail="Server misconfigured: STUDIO_SECRET missing")
    if x_studio_secret != STUDIO_SECRET:
        raise HTTPException(status_code=401, detail="Invalid or missing x-studio-secret")

    # Inspect raw body (helpful for WeWeb payloads)
    raw = await request.body()
    print("DEBUG raw_body_bytes:", len(raw))
    print("DEBUG headers.sample:", {k: v for k, v in list(request.headers.items())[:6]})

    # Normal parse + fallback manual parse
    g = payload.google or {}
    inp = payload.input or {}
    if not g and not inp and raw:
        try:
            fb = json.loads(raw.decode("utf-8"))
            if isinstance(fb, dict):
                g = fb.get("google") or {}
                inp = fb.get("input") or {}
                print("DEBUG fallback_json_parse_used:", True)
        except Exception as e:
            print("DEBUG fallback_json_parse_error:", e)

    # params: support both {input:{params:{...}}} and {input:{...}}
    params = (inp.get("params") or {}) if isinstance(inp.get("params"), dict) else (inp or {})
    # Extract filter params (non-breaking defaults)
    timeframe = str((params.get("timeframe") or "next_quarter")).lower()
    schemes = params.get("schemes") or []
    region_level = (params.get("region_level") or "national").strip().lower()
    region_value = (params.get("region_value") or "").strip()
    demographic = params.get("demographic") or None  # currently not used in model; reserved

    print("DEBUG params:", {
        "timeframe": timeframe, "schemes": schemes,
        "region_level": region_level, "region_value": region_value,
        "demographic": demographic
    })

    # ---- STEP 1: Google Sheets -> staged artifacts (env fallbacks) ----
    sheet_id   = (g.get("sheet_id") or os.getenv("SHEET_ID") or "").strip() or None
    tabs       = g.get("tabs") or os.getenv("SHEET_TABS") or "applications,promotions,demographics,socio_econ"

    raw_creds = (
        g.get("sheets_creds_json")
        or g.get("service_account_json")
        or os.getenv("GSHEETS_SA_JSON")
        or ""
    )
    if isinstance(raw_creds, dict):
        credsjson = json.dumps(raw_creds)
    else:
        credsjson = (raw_creds or "").strip()

    header_row = str(g.get("header_row", os.getenv("SHEET_HEADER_ROW", "1")))
    limit      = str(g.get("limit", os.getenv("SHEET_LIMIT", "250000")))

    print("DEBUG sheets_creds_json_len:", len(credsjson))
    print("DEBUG sheet_id:", sheet_id, "| tabs:", tabs, "| header_row:", header_row, "| limit:", limit)

    errors: list[str] = []

    try:
        sheets_res = sheets_fetch_stage(
            sheet_id=sheet_id,
            tabs=tabs,
            sheets_creds_json=credsjson,
            header_row=header_row,
            limit=limit,
        ) or {}
        if not sheets_res.get("ok", True):
            errors.append(f"sheets_fetch_stage error: {sheets_res.get('error')}")
    except Exception as e:
        sheets_res = {}
        errors.append(f"sheets_fetch_stage exception: {e}")

    # ---- STEP 2: DQ + FE ----
    try:
        dq_res = dq_and_fe(
            applications_id=sheets_res.get("applications_id"),
            promotions_id=sheets_res.get("promotions_id"),
            demographics_id=sheets_res.get("demographics_id"),
            socio_econ_id=sheets_res.get("socio_econ_id"),
        ) or {}
    except Exception as e:
        dq_res = {"features": None, "dq_report": {}}
        errors.append(f"dq_and_fe exception: {e}")

    features_id = dq_res.get("features")
    features_data = dq_res.get("features_data")
    print("DEBUG features_id:", features_id)

    # ---- STEP 3: Planning + Forecasts (now respects filters) ----
    try:
        pf_res = plan_and_forecast(
            features_id=features_id,
            timeframe=timeframe,
            features_data=features_data,
            # new kwargs supported by tool (safe if ignored)
            schemes=schemes,
            region_level=region_level,
            region_value=region_value
        ) or {}
    except Exception as e:
        pf_res = {"forecasts_raw": None, "model_plan": [], "forecasts_raw_data": []}
        errors.append(f"plan_and_forecast exception: {e}")

    forecasts_raw_id = pf_res.get("forecasts_raw")
    forecasts_raw_data = pf_res.get("forecasts_raw_data")
    print("DEBUG forecasts_raw_id:", forecasts_raw_id)

    # ---- STEP 4: Aggregate + Drivers (+ Analytics) ----
    try:
        ag_res = aggregate_and_drivers(
            forecasts_raw_id=forecasts_raw_id,
            features_id=features_id,
            forecasts_raw_data=forecasts_raw_data,
            features_data=features_data
        ) or {}
    except Exception as e:
        ag_res = {
            "forecasts_agg": None,
            "cards": {"total_forecast": 0, "confidence_range": [0, 0], "series_count": 0},
            "drivers": [],
            "analytics": {
                "uptake_by_state": {"labels": [], "data": []},
                "monthly_trend_multi": {"labels": [], "datasets": []},
                "promotions_vs_apps": {"data": [], "r": None},
                "demographics_pie": {"labels": [], "data": []}
            },
            "forecasts_agg_data": [],
            "insights": ["Insights unavailable", "—", "—"]
        }
        errors.append(f"aggregate_and_drivers exception: {e}")

    print("DEBUG cards:", ag_res.get("cards"))

    # ---- STEP 5: UI Packager (now forwards insights; trend auto-builds if absent) ----
    try:
        ui_res = ui_pack_and_persist(
            forecasts_agg_id=ag_res.get("forecasts_agg"),
            forecasts_agg_data=ag_res.get("forecasts_agg_data"),
            cards=ag_res.get("cards"),
            drivers=ag_res.get("drivers"),
            analytics=ag_res.get("analytics"),
            insights=ag_res.get("insights"),    # <-- forward 3-line insights
            # trend=ag_res.get("trend"),        # not required; UI tool builds default from table
            errorMsg="; ".join(errors) if errors else ""
        ) or {}
        payload_for_ui = ui_res
    except Exception as e:
        payload_for_ui = {
            "forecastResponse": {
                "cards": {"total_forecast": 0, "confidence_range": [0, 0], "series_count": 0},
                "drivers": [],
                "insights": ["Insights unavailable", "—", "—"],
                "generatedAt": None
            },
            "forecastTable": [],
            "analyticsData": {
                "uptake_by_state": {"labels": [], "data": []},
                "monthly_trend_multi": {"labels": [], "datasets": []},
                "promotions_vs_apps": {"data": [], "r": None},
                "demographics_pie": {"labels": [], "data": []}
            },
            "errorMsg": f"ui_pack_and_persist exception: {e}"
        }

    return payload_for_ui

# ----------------------------
# (Optional) SDK endpoints if you need them later
# ----------------------------
@app.post("/wfs/workflow")
async def upload_workflow(
    x_studio_secret: Optional[str] = Header(None),
    file: UploadFile = File(...),
):
    if x_studio_secret != STUDIO_SECRET:
        raise HTTPException(status_code=401, detail="Invalid or missing x-studio-secret")
    return {"ok": True, "msg": "workflow upload endpoint stub (not used in this flow)"}

@app.post("/wfs/chat")
async def chat_workflow(
    x_studio_secret: Optional[str] = Header(None),
    workflow_id: str = Form(...),
    query: str = Form(...),
    context: str = Form(""),
):
    if x_studio_secret != STUDIO_SECRET:
        raise HTTPException(status_code=401, detail="Invalid or missing x-studio-secret")
    return {"answer": f"(stub) You asked: {query}", "conversation": []}

# ----------------------------
# Local run
# ----------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8080"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
