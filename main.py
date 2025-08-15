import os
import sys
import json
import importlib
import pathlib
from typing import Any, Dict, Optional, Tuple

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ──────────────────────────────────────────────────────────────────────────────
# Env / Config
# ──────────────────────────────────────────────────────────────────────────────
STUDIO_SECRET = os.getenv("STUDIO_SECRET", "")

# Comma-separated list: "https://editor.weweb.io,https://foo.weweb.app,https://bar.weweb-preview.io"
ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "").split(",") if o.strip()]
# Optional regex that matches all WeWeb editor/preview/prod:
ALLOWED_ORIGIN_REGEX = os.getenv("ALLOWED_ORIGIN_REGEX", "")

# ──────────────────────────────────────────────────────────────────────────────
# WaveFlow Studio SDK (explicit import + client init)
# ──────────────────────────────────────────────────────────────────────────────
WFS_API_KEY = (
    os.getenv("WFS_API_KEY")
    or os.getenv("WAVEFLOW_STUDIO_API_KEY")
    or os.getenv("WAVEFLOW_API_KEY")
    or ""
)
WFS_BASE_URL = os.getenv("WFS_BASE_URL") or os.getenv("WAVEFLOW_BASE_URL") or None

WFS_CLIENT = None
try:
    # ←←← THIS is the import you asked for
    from waveflow_studio import WaveFlowStudio  # pip: waveflow-studio

    if WFS_API_KEY:
        WFS_CLIENT = WaveFlowStudio(api_key=WFS_API_KEY, base_url=WFS_BASE_URL)
        print(f"[wfs] WaveFlowStudio client initialized (base_url={WFS_BASE_URL or 'default'})")
    else:
        print("[wfs] API key not set; WaveFlowStudio client not initialized")
except Exception as e:
    print(f"[wfs] WaveFlowStudio import/init skipped: {e}")

# Expose to libs that read env directly
if WFS_API_KEY and not os.getenv("WAVEFLOW_API_KEY"):
    os.environ["WAVEFLOW_API_KEY"] = WFS_API_KEY
if WFS_BASE_URL and not os.getenv("WAVEFLOW_BASE_URL"):
    os.environ["WAVEFLOW_BASE_URL"] = WFS_BASE_URL

# ──────────────────────────────────────────────────────────────────────────────
# FastAPI & CORS
# ──────────────────────────────────────────────────────────────────────────────
app = FastAPI(title="SANKALP Backend", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,                    # explicit list (can be empty)
    allow_origin_regex=ALLOWED_ORIGIN_REGEX or None,  # optional regex
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ──────────────────────────────────────────────────────────────────────────────
# Model for /run
# ──────────────────────────────────────────────────────────────────────────────
class RunPayload(BaseModel):
    google: Optional[Dict[str, Any]] = None
    input: Optional[Dict[str, Any]] = None
    workflow_id: Optional[str] = None
    class Config:
        extra = "allow"

# ──────────────────────────────────────────────────────────────────────────────
# Make Tools importable (case-sensitive on Linux)
# ──────────────────────────────────────────────────────────────────────────────
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

# Try common filename variants
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

# ──────────────────────────────────────────────────────────────────────────────
# Startup / health / echo
# ──────────────────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def on_startup():
    print("== Registered routes ==")
    for r in app.routes:
        path = getattr(r, "path", "")
        methods = ",".join(getattr(r, "methods", []) or [])
        print(f"{r.name:20s} {path:25s} [{methods}]")
    print(f"Tools dir exists: {TOOLS_DIR.exists()} at {TOOLS_DIR}")
    print(f"[wfs] client: {'ready' if WFS_CLIENT else 'not-initialized'}")

@app.get("/")
async def root():
    return {"ok": True, "service": "SANKALP backend", "docs": "/docs"}

@app.get("/health")
async def health():
    return {"ok": True, "status": "healthy"}

@app.post("/echo")
async def echo(request: Request):
    raw = await request.body()
    try:
        parsed = json.loads(raw.decode("utf-8"))
        keys = list(parsed.keys()) if isinstance(parsed, dict) else None
    except Exception:
        parsed, keys = None, None
    return {
        "len": len(raw),
        "content_type": request.headers.get("content-type"),
        "top_keys": keys,
        "raw_preview": raw[:400].decode("utf-8", errors="replace"),
    }

# ──────────────────────────────────────────────────────────────────────────────
# /run endpoint
# ──────────────────────────────────────────────────────────────────────────────
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

    # Inspect raw
    raw = await request.body()
    print("DEBUG raw_body_bytes:", len(raw))
    print("DEBUG headers.sample:", {k: v for k, v in list(request.headers.items())[:6]})

    # Parse payload (fallback if Content-Type is off)
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
    params = (inp.get("params") or {}) if isinstance(inp.get("params"), dict) else inp

    # Google Sheets inputs (with env fallbacks)
    sheet_id_env = os.getenv("SHEET_ID") or os.getenv("GSHEET_ID") or os.getenv("GOOGLE_SHEET_ID")
    creds_env = os.getenv("GSHEETS_SA_JSON") or os.getenv("SHEETS_CREDS_JSON") or os.getenv("GOOGLE_SA_JSON")

    sheet_id   = (g.get("sheet_id") or "").strip() or sheet_id_env
    tabs       = g.get("tabs") or "applications,promotions,demographics,socio_econ"

    raw_creds = (
        g.get("sheets_creds_json")
        or g.get("service_account_json")
        or (creds_env or "")
    )
    if isinstance(raw_creds, dict):
        credsjson = json.dumps(raw_creds)
    else:
        credsjson = (raw_creds or "").strip()

    header_row = str(g.get("header_row", "1"))
    limit      = str(g.get("limit", "250000"))

    print("DEBUG sheets_creds_json_len:", len(credsjson))
    print("DEBUG sheet_id:", sheet_id, " | tabs:", tabs, " | header_row:", header_row, " | limit:", limit)

    errors: list[str] = []

    # Step 1: Sheets → staged artifacts
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

    # Step 2: DQ + FE
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

    # Step 3: Plan + Forecast
    timeframe = str((params.get("timeframe") or "next_quarter")).lower()
    try:
        pf_res = plan_and_forecast(features_id=features_id, timeframe=timeframe)
    except Exception as e:
        pf_res = {"forecasts_raw": None, "model_plan": []}
        errors.append(f"plan_and_forecast exception: {e}")

    forecasts_raw_id = (pf_res or {}).get("forecasts_raw")

    # Step 4: Aggregate + Drivers
    try:
        ag_res = aggregate_and_drivers(forecasts_raw_id=forecasts_raw_id, features_id=features_id)
    except Exception as e:
        ag_res = {
            "forecasts_agg": None,
            "cards": {"total_forecast": 0, "confidence_range": [0, 0], "series_count": 0},
            "drivers": [],
        }
        errors.append(f"aggregate_and_drivers exception: {e}")

    # Step 5: UI Packager (persist via WaveFlow runtime if available)
    try:
        ui_res = ui_pack_and_persist(
            forecasts_agg_id=ag_res.get("forecasts_agg"),
            cards=ag_res.get("cards"),
            drivers=ag_res.get("drivers"),
            dq_report=(dq_res or {}).get("dq_report"),
            errors=(errors or (sheets_res.get("warnings") if isinstance(sheets_res, dict) else None)),
            persist="true",
            params={"workflow_id": (payload.workflow_id or "scheme_forecast"), **(params or {})},
            model_plan=(pf_res or {}).get("model_plan"),
            # pass the initialized client to your tool (it should accept **kwargs)
            wfs_client=WFS_CLIENT,
        )
        payload_for_ui = (ui_res or {}).get("response_json", {})
        if ui_res and ui_res.get("run_id"):
            payload_for_ui["run_id"] = ui_res["run_id"]
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


# Optional local run
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8080"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
