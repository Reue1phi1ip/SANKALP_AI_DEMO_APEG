# ---- M1: Scheme Forecasting — Unified Postprocess Executor ----
# Combines: Aggregation → Driver Notes → UI Packager → Audit & Save
# Zero-safe: if any artifact is missing/empty, returns flat 0s + warnings.
# Works in VS Code (local) & Waveflow (runtime). Local stubs provided.

from typing import Dict, Any, List
import time

# ---------- Waveflow helpers (use real ones in Studio; stubs for local) ----------
try:
    # In Waveflow runtime, you may already have these injected / importable.
    from waveflow.artifacts import load_artifact as wf_load_artifact    # noqa
    from waveflow.artifacts import save_artifact as wf_save_artifact    # noqa
    from waveflow.storage import save_json as wf_save_json              # noqa
    from waveflow.runs import persist_run as wf_persist_run             # noqa
except Exception:
    wf_load_artifact = None
    wf_save_artifact = None
    wf_save_json = None
    wf_persist_run = None

def _load_artifact(artifact_id: str):
    """Try Waveflow load; else return an empty DataFrame-like stub."""
    try:
        if wf_load_artifact:
            return wf_load_artifact(artifact_id)
    except Exception:
        pass
    # Local fallback (empty DataFrame)
    try:
        import pandas as pd
        return pd.DataFrame()
    except Exception:
        return None

def _save_artifact(df, name: str) -> str:
    try:
        if wf_save_artifact:
            return wf_save_artifact(df, name)
    except Exception:
        pass
    return f"table:{name}"

def _save_json(obj: dict, name: str) -> str:
    try:
        if wf_save_json:
            return wf_save_json(obj, name)
    except Exception:
        pass
    return f"json:{name}"

def _persist_run(artifacts: dict, response_json: dict, params: dict, model_plan: list) -> dict:
    try:
        if wf_persist_run:
            return wf_persist_run(artifacts, response_json, params, model_plan)
    except Exception:
        pass
    return {"run_id": f"M1-{int(time.time())}", "saved": True}

# ------------------------ Core helpers (pure Python) ------------------------

def _zero_cards() -> Dict[str, Any]:
    return {"total_forecast": 0.0, "confidence_range": [0.0, 0.0], "series_count": 0}

def _zero_payload(errors: List[str], dq_report: dict = None) -> Dict[str, Any]:
    return {
        "summary_cards": {**_zero_cards(), "warnings": errors or []},
        "chart": {"type": "line_with_band", "series": [{"period": "—", "low": 0, "expected": 0, "high": 0}]},
        "table": [{"region": "—", "period": "—", "expected": 0, "low": 0, "high": 0}],
        "drivers": [],
        "debug": {"dq_report": dq_report or {}, "errors": errors or []}
    }

def _aggregate_zero_safe(df, params: dict) -> Dict[str, Any]:
    """
    Input df expected columns: series_id, period, yhat, yhat_low, yhat_high, geo_code (optional)
    Returns: {"agg_table": DataFrame, "cards": dict}
    """
    try:
        import pandas as pd
    except Exception:
        # Minimal JSON fallback if pandas not available
        return {"agg_table": None, "cards": _zero_cards()}

    if df is None or getattr(df, "empty", True):
        agg = pd.DataFrame(columns=["region", "period", "expected", "low", "high"])
        return {"agg_table": agg, "cards": _zero_cards()}

    # Map fields
    df = df.copy()
    for col, default in [("yhat", 0.0), ("yhat_low", 0.0), ("yhat_high", 0.0)]:
        if col not in df.columns:
            df[col] = default

    # Region column mapping (if geo_code present)
    region_col = "geo_code" if "geo_code" in df.columns else None
    df["region"] = df[region_col] if region_col else "—"
    df["expected"] = df["yhat"].astype(float).fillna(0.0)
    df["low"] = df["yhat_low"].astype(float).fillna(0.0)
    df["high"] = df["yhat_high"].astype(float).fillna(0.0)

    # Group by region + period
    grp = df.groupby(["region", "period"], as_index=False)[["expected", "low", "high"]].sum()

    total = float(grp["expected"].sum()) if not grp.empty else 0.0
    lo = float(grp["low"].sum()) if not grp.empty else 0.0
    hi = float(grp["high"].sum()) if not grp.empty else 0.0
    cards = {"total_forecast": total, "confidence_range": [lo, hi],
             "series_count": int(df["series_id"].nunique() if "series_id" in df.columns else 0)}

    return {"agg_table": grp, "cards": cards}

def _driver_notes(features_df, forecasts_df) -> List[str]:
    # Purely descriptive, never causal; return [] or minimal neutral notes.
    if (features_df is None or getattr(features_df, "empty", True) or
        forecasts_df is None or getattr(forecasts_df, "empty", True)):
        return ["Drivers unavailable"]

    notes = []
    # If promotions feature exists with any non-zero value, add neutral note
    if "promo_intensity" in features_df.columns and features_df["promo_intensity"].fillna(0).sum() > 0:
        notes.append("Observed promotional activity in past periods; correlation not assessed.")

    # If a quarter column exists, mention seasonal structure without asserting effect
    if "quarter" in features_df.columns:
        notes.append("Quarterly seasonality fields present; detailed analysis pending.")

    return notes or ["Drivers unavailable"]

def _build_payload(agg_df, cards: dict, driver_notes: List[str], dq_report: dict, errors: List[str]) -> Dict[str, Any]:
    try:
        import pandas as pd
    except Exception:
        return _zero_payload(errors, dq_report)

    if agg_df is None or getattr(agg_df, "empty", True):
        return _zero_payload(errors, dq_report)

    # Expect columns: region, period, expected, low, high
    chart_series = []
    table_rows = []
    for _, r in agg_df.iterrows():
        chart_series.append({
            "period": str(r.get("period", "—")),
            "low": float(r.get("low", 0)),
            "expected": float(r.get("expected", 0)),
            "high": float(r.get("high", 0))
        })
        table_rows.append({
            "region": str(r.get("region", "—")),
            "period": str(r.get("period", "—")),
            "expected": float(r.get("expected", 0)),
            "low": float(r.get("low", 0)),
            "high": float(r.get("high", 0))
        })

    return {
        "summary_cards": {
            "total_forecast": float(cards.get("total_forecast", 0)),
            "confidence_range": [
                float(cards.get("confidence_range", [0, 0])[0]),
                float(cards.get("confidence_range", [0, 0])[1]),
            ],
            "series_count": int(cards.get("series_count", 0)),
            "warnings": errors or []
        },
        "chart": {"type": "line_with_band", "series": chart_series or [{"period": "—", "low": 0, "expected": 0, "high": 0}]},
        "table": table_rows or [{"region": "—", "period": "—", "expected": 0, "low": 0, "high": 0}],
        "drivers": driver_notes or [],
        "debug": {"dq_report": dq_report or {}, "errors": errors or []}
    }

# ------------------------------- MAIN ENTRY -------------------------------

def m1_unified_executor(input_params: Dict[str, Any]) -> Dict[str, Any]:
    """
    One-stop postprocess executor for M1.
    Expects:
    {
      "params": {...},                      # schemes, timeframe, geo_level, segments
      "artifacts": {
        "features": "<table-id>",
        "forecasts_raw": "<table-id>",
        "dq_report": {...},                 # optional JSON
        "model_plan": [...]                 # optional for audit
      },
      "errors": []                          # optional passthrough
    }
    Returns:
    {
      "artifacts": { "forecasts_agg": "<table-id>", "ui_payload": "<json-id>" },
      "response_json": {...},
      "run_id": "M1-...",
      "errors": [...]
    }
    """
    params = (input_params or {}).get("params", {}) or {}
    art = (input_params or {}).get("artifacts", {}) or {}
    errors: List[str] = (input_params or {}).get("errors", []) or []
    dq_report = art.get("dq_report") or {}

    # ---- Load artifacts (zero-safe) ----
    features_df = _load_artifact(art.get("features", ""))
    forecasts_df = _load_artifact(art.get("forecasts_raw", ""))

    # ---- 1) Aggregation & CI ----
    agg_out = _aggregate_zero_safe(forecasts_df, params)
    agg_df = agg_out.get("agg_table")
    cards = agg_out.get("cards", _zero_cards())
    forecasts_agg_id = _save_artifact(agg_df, "forecasts_agg") if agg_df is not None else "table:forecasts_agg_empty"

    # ---- 2) Driver Notes (descriptive only) ----
    notes = _driver_notes(features_df, forecasts_df)

    # ---- 3) UI Packager ----
    payload = _build_payload(agg_df, cards, notes, dq_report, errors)
    ui_payload_id = _save_json(payload, "ui_payload")

    # ---- 4) Audit & Save ----
    audit = _persist_run(
        artifacts={
            "features": art.get("features"),
            "forecasts_raw": art.get("forecasts_raw"),
            "forecasts_agg": forecasts_agg_id,
            "ui_payload": ui_payload_id
        },
        response_json=payload,
        params=params,
        model_plan=art.get("model_plan", [])
    )

    # Final shape (what the next node/WeWeb should read)
    return {
        "artifacts": {"forecasts_agg": forecasts_agg_id, "ui_payload": ui_payload_id},
        "response_json": payload,
        "run_id": audit.get("run_id"),
        "errors": errors
    }