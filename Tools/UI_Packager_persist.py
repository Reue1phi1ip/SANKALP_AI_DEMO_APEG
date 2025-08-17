# Tools/UI_Packager_persist.py
# Packs aggregator outputs into a UI-friendly payload for WeWeb.
# Exports: ui_pack_and_persist  (plus alias ui_packager_persist)
from __future__ import annotations
from typing import Any, Dict, List

def ui_pack_and_persist(
    forecasts_agg_id: str = None,
    forecasts_agg_data: List[Dict[str, Any]] | None = None,  # fallback rows
    cards: Dict[str, Any] | None = None,
    drivers: List[str] | None = None,
    analytics: Dict[str, Any] | None = None,
    errorMsg: str = "",
    *args, **kwargs
) -> Dict[str, Any]:
    """
    Returns payload the WeWeb page can bind directly:
    {
      "forecastResponse": { "cards": {...}, "drivers": [...], "generatedAt": ISO8601Z },
      "forecastTable": [ {region, period, expected, low, high}, ... ],
      "analyticsData": {   // camelCase for your UI
         "uptake_by_state": {labels:[], data:[]},
         "monthly_trend_multi": {labels:[], datasets:[]},
         "promotions_vs_apps": {data:[], r:null},
         "demographics_pie": {labels:[], data:[]}
      },
      "errorMsg": ""
    }
    """
    import pandas as pd
    from datetime import datetime

    # Try artifact store if available (Waveflow)
    try:
        from waveflow.artifacts import load_artifact, save_artifact
    except Exception:
        load_artifact = save_artifact = None  # type: ignore

    # ---------- Load forecasts_agg table ----------
    agg_df = pd.DataFrame()
    if load_artifact and forecasts_agg_id:
        try:
            agg_df = load_artifact(forecasts_agg_id)
        except Exception as e:
            print("[ui_pack_and_persist] load_artifact failed:", e)

    if agg_df.empty and forecasts_agg_data:
        try:
            agg_df = pd.DataFrame(forecasts_agg_data)
        except Exception as e:
            print("[ui_pack_and_persist] forecasts_agg_data->DF failed:", e)

    # Ensure required columns exist
    for col in ("region", "period", "expected", "low", "high"):
        if col not in agg_df.columns:
            agg_df[col] = [] if col in ("region", "period") else 0.0

    # ---------- Defaults for cards/drivers/analytics ----------
    cards = cards or {"total_forecast": 0.0, "confidence_range": [0.0, 0.0], "series_count": 0}
    drivers = drivers or ["Drivers unavailable"]
    analytics = analytics or {}

    analyticsData = {
        "uptake_by_state": analytics.get("uptake_by_state") or {"labels": [], "data": []},
        "monthly_trend_multi": analytics.get("monthly_trend_multi") or {"labels": [], "datasets": []},
        "promotions_vs_apps": analytics.get("promotions_vs_apps") or {"data": [], "r": None},
        "demographics_pie": analytics.get("demographics_pie") or {"labels": [], "data": []},
    }

    # ---------- Build UI payload ----------
    forecastTable = agg_df[["region", "period", "expected", "low", "high"]].copy()

    ui_payload: Dict[str, Any] = {
        "forecastResponse": {
            "cards": cards,
            "drivers": drivers,
            "generatedAt": datetime.utcnow().isoformat() + "Z",
        },
        "forecastTable": forecastTable.to_dict(orient="records"),
        "analyticsData": analyticsData,   # camelCase
        "errorMsg": str(errorMsg or ""),
    }

    # Optional: persist a tiny snapshot for debugging
    if load_artifact and save_artifact:
        try:
            # store compact form; avoid saving large nested dicts as single cell
            flat = pd.json_normalize(ui_payload, sep=".")
            save_artifact(flat, "ui_payload_latest")
        except Exception as e:
            print("[ui_pack_and_persist] save_artifact failed:", e)

    return ui_payload

# Backward-compat alias (if any code still calls the old name)
def ui_packager_persist(*args, **kwargs):
    return ui_pack_and_persist(*args, **kwargs)

__all__ = ["ui_pack_and_persist", "ui_packager_persist"]
