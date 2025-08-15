# main.py
import os
import sys
import json
import importlib
import pathlib
from typing import Any, Dict, Optional, Tuple, Callable

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# =========================
# Environment / Config
# =========================
STUDIO_SECRET = os.getenv("STUDIO_SECRET", "")

# CORS: either list of exact origins or a regex pattern
ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "").split(",") if o.strip()]
ALLOWED_ORIGIN_REGEX = os.getenv("ALLOWED_ORIGIN_REGEX", "")

# Fallbacks for Google Sheets config
SHEET_ID_ENV = os.getenv("SHEET_ID", "").strip()
GSHEETS_SA_JSON_ENV = os.getenv("GSHEETS_SA_JSON", "").strip()

# =========================
# FastAPI app & CORS
# =========================
app = FastAPI(title="SANKALP Backend", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,                         # may be empty
    allow_origin_regex=ALLOWED_ORIGIN_REGEX or None,       # optional regex
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# Request model for /run
# =========================
class RunPayload(BaseModel):
    google: Optional[Dict[str, Any]] = None
    input: Optional[Dict[str, Any]] = None
    workflow_id: Optional[str] = None

    class Config:
        extra = "allow"

# =========================
# Make Tools importable (Linux is case-sensitive)
# =========================
BASE_DIR = pathlib.Path(__file__).parent.resolve()
TOOLS_DIR = BASE_DIR / "Tools"
if TOOLS_DIR.exists():
    sys.path.insert(0, str(TOOLS_DIR))  # e.g. "Tools.GoogleSheetsFetch"
    sys.path.insert(0, str(BASE_DIR))   # e.g. "GoogleSheetsFetch"

def _import_first(candidates: Tuple[Tuple[str, str], ...]) -> Callable:
    """
    Try (module, attribute) pairs in order and return the first callable found.
    Raises an ImportError listing all attempts if none succeed.
    """
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

# Sheets fetch
sheets_fetch_stage = _import_first((
    ("Tools.GoogleSheetsFetch", "sheets_fetch_stage"),
    ("Tools.GooglesheetsFetch", "sheets_fetch_stage"),
    ("GoogleSheetsFetch", "sheets_fetch_stage"),
    ("GooglesheetsFetch", "sheets_fetch_stage"),
))

# Data quality + feature engineering
dq_and_fe = _import_first((
    ("Tools.Data_quality_featureEngineer", "dq_and_fe"),
    ("Tools.data_quality_fe", "dq_and_fe"),
    ("data_quality_fe", "dq_and_fe"),
))

# Planning + forecasting
plan_and_forecast = _import_first((
    ("Tools.Model_planning_forecasting", "plan_and_forecast"),
    ("Tools.model_planning", "plan_and_forecast"),
    ("model_planning", "plan_and_forecast"),
))

# Aggregation + drivers
aggregate_and_drivers = _import_first((
    ("Tools.Aggregator_Drivers", "aggregate_and_drivers"),
    ("Tools.aggregator", "aggregate_and_drivers"),
    ("aggregator", "aggregate_and_drivers"),
))

# UI packaging / persistence
ui_pack_and_persist = _import_first((
    ("Tools.UI_Packager_persist", "ui_pack_and_persist"),
    ("Tools.UIPackager_persist", "ui_pack_and_persist"),
    ("UI_Packager_persist", "ui_pack_and_persist"),
    ("UIPackager_persist", "ui_pack_and_persist"),
))

# =========================
# Startup log
# =========================
@app.on_event("startup")
async def on_startup():
    print("== Registered routes ==")
    for r in app.routes:
        path = getattr(r, "path", "")
        methods = ",".join(getattr(r, "methods", []) or [])
        print(f"{r.name:20s} {path:25s} [{methods}]")
    print(f"Tools dir exists: {TOOLS_DIR.exists()} at {TOOLS_DIR}")

# =========================
# Health & debug helpers
# =========================
@app.get("/")
async def root():
    return {"ok": True, "service": "SANKALP backend", "docs": "/docs"}

@app.get("/health")
async def health():
    return {"ok": True, "status": "healthy"}

@app.post("/echo")
async def echo(request: Request):
    """Debug endpoint to see exactly what body/headers the client is sending."""
    raw = await request.body()
    try:
        parsed = json.loads(raw.decode("utf-8"))
        keys = list(parsed.keys()) if isinstance(parsed, dict) else None
    except Exception:
        parsed = None
        keys = None
    return {
        "len": len(raw),
        "content_type": request.headers.get("content-type"),
        "top_keys": keys,
        "raw_preview": raw[:400].decode("utf-8", errors="replace"),
    }

# =========================
# Orchestrated run endpoint
# =========================
@app.post("/run")
async def run_endpoint(
    payload: RunPayload,
    request: Request,
    x_studio_secret: Optional[str] = Header(None),
):
    # --- Auth check ---
    if not STUDIO_SECRET:
        raise HTTPException(status_code=500, detail="Server misconfigured: STUDIO_SECRET missing")
    if x_studio_secret != STUDIO_SECRET:
        raise HTTPException(status_code=401, detail="Invalid or missing x-studio-secret")

    # --- Log a little for debugging ---
    raw = await request.body()
    print("DEBUG raw_body_bytes:", len(raw))
    print("DEBUG headers.sample:", {k: v for k, v in list(request.headers.items())[:6]})

    g = payload.google or {}
    inp = payload.input or {}

    # Fallback parse if WeWeb didn't bind to the model (wrong content-type, etc.)
    if not g and not inp and raw:
        try:
            fb = json.loads(raw.decode("utf-8"))
            if isinstance(fb, dict):
                g = fb.get("google") or {}
                inp = fb.get("input") or {}
                print("DEBUG fallback_json_parse_used:", True)
        except Exception as e:
            print("DEBUG fallback_json_parse_error:", e)

    params = (inp.get("params") or {}) if isinstance(inp.get("params"), dict) else inp

    # ---- STEP 1: Google Sheets -> staged artifacts ----
    # Use request values if present; otherwise fall back to env vars.
    sheet_id = (str(g.get("sheet_id", "")).strip() or SHEET_ID_ENV) or None
    tabs = g.get("tabs") or "applications,promotions,demographics,socio_econ"

    # creds can be a dict or a JSON string; fall back to env JSON string if not provided
    raw_creds = g.get("sheets_creds_json") or GSHEETS_SA_JSON_ENV or ""
    if isinstance(raw_creds, dict):
        credsjson = json.dumps(raw_creds)
    else:
        credsjson = str(raw_creds).strip()

    header_row = str(g.get("header_row", "1"))
    limit = str(g.get("limit", "250000"))

    print("DEBUG sheets_creds_json_len:", len(credsjson))
    print("DEBUG sheet_id:", sheet_id, " | tabs:", tabs, " | header_row:", header_row, " | limit:", limit)

    errors: list[str] = []
    try:
        sheets_res = sheets_fetch_stage(
            sheet_id=sheet_id,
            tabs=tabs,
            sheets_creds_json=credsjson,
            header_row=header_row,
            limit=limit,
        )
        if not sheets_res or not sheets_res.get("ok", True):
            err_msg = sheets_res.get("error") if isinstance(sheets_res, dict) else "unknown"
            errors.append(f"sheets_fetch_stage error: {err_msg}")
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
        )
    except Exception as e:
        dq_res = {"features": None, "dq_report": {}}
        errors.append(f"dq_and_fe exception: {e}")

    features_id = (dq_res or {}).get("features")

    # ---- STEP 3: Planning + Forecasts ----
    timeframe = str((params.get("timeframe") or "next_quarter")).lower()
    try:
        pf_res = plan_and_forecast(features_id=features_id, timeframe=timeframe)
    except Exception as e:
        pf_res = {"forecasts_raw": None, "model_plan": []}
        errors.append(f"plan_and_forecast exception: {e}")

    forecasts_raw_id = (pf_res or {}).get("forecasts_raw")

    # ---- STEP 4: Aggregate + Drivers ----
    try:
        ag_res = aggregate_and_drivers(forecasts_raw_id=forecasts_raw_id, features_id=features_id)
    except Exception as e:
        ag_res = {
            "forecasts_agg": None,
            "cards": {"total_forecast": 0, "confidence_range": [0, 0], "series_count": 0},
            "drivers": [],
        }
        errors.append(f"aggregate_and_drivers exception: {e}")

    # ---- STEP 5: UI Packager ----
    try:
        ui_res = ui_pack_and_persist(
            forecasts_agg_id=ag_res.get("forecasts_agg"),
            cards=ag_res.get("cards"),
            drivers=ag_res.get("drivers"),
            dq_report=(dq_res or {}).get("dq_report"),
            errors=(errors or (sheets_res.get("warnings") if isinstance(sheets_res, dict) else None)),
            persist="true",
            params=params,
            model_plan=(pf_res or {}).get("model_plan"),
        )
        payload_for_ui = (ui_res or {}).get("response_json", {})
    except Exception as e:
        payload_for_ui = {
            "summary_cards": {
                "total_forecast": 0,
                "confidence_range": [0, 0],
                "series_count": 0,
                "warnings": errors + [f"ui_pack_and_persist exception: {e}"],
            },
            "chart": {"type": "line_with_band", "series": [{"period": "—", "low": 0, "expected": 0, "high": 0}]},
            "table": [{"region": "—", "period": "—", "expected": 0, "low": 0, "high": 0}],
            "drivers": [],
            "debug": {"errors": errors},
        }

    return payload_for_ui


# =========================
# Local run
# =========================
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8080"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)