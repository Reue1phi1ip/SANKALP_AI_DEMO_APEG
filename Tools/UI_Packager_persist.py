# Tools/UI_Packager_persist.py
# Packs aggregator outputs into a UI-friendly payload for WeWeb.
# NOTE: Uses camelCase key: analyticsData

from __future__ import annotations
from typing import Any, Dict, List

def ui_packager_persist(
    forecasts_agg_id: str = None,
    forecasts_agg_data: List[Dict[str, Any]] = None,  # fallback rows
    cards: Dict[str, Any] = None,
    drivers: List[str] = None,
    analytics: Dict[str, Any] = None,
    errorMsg: str = "",
    *args, **kwargs
) -> Dict[str, Any]:
    """
    Returns a payload the WeWeb page can bind directly:
    {
      "forecastResponse": { "cards": {...}, "drivers": [...] },
      "forecastTable": [ {region, period, expected, low, high}, ... ],
      "analyticsData": {   // <- camelCase
         "uptake_by_state": {labels:[], data:[]},
         "monthly_trend_multi": {labels:[], datasets:[]},
         "promotions_vs_apps": {data:[], r: null},
         "demographics_pie": {labels:[], data:[]}
      },
      "errorMsg": ""
    }
    """
    import pandas as pd
    from datetime import datetime

    # Try artifact store if available
    try:
        from waveflow.artifacts import load_artifact, save_artifact
    except Exception:
        load_artifact = save_artifact = None

    # ---------- Load forecasts_agg table ----------
    agg_df = pd.DataFrame()
    if load_artifact and forecasts_agg_id:
        try:
            agg_df = load_artifact(forecasts_agg_id)
        except Exception as e:
            print("[ui_packager_persist] load_artifact failed:", e)

    if agg_df.empty and forecasts_agg_data:
        try:
            agg_df = pd.DataFrame(forecasts_agg_data)
        except Exception as e:
            print("[ui_packager_persist] forecasts_agg_data->DF failed:", e)

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
        "demographics_pie": analytics.get("demographics_pie") or {"labels": [], "data": []}
    }

    # ---------- Build UI payload ----------
    forecastTable = agg_df[["region", "period", "expected", "low", "high"]].copy()

    ui_payload = {
        "forecastResponse": {
            "cards": cards,
            "drivers": drivers,
            "generatedAt": datetime.utcnow().isoformat() + "Z"
        },
        "forecastTable": forecastTable.to_dict(orient="records"),
        "analyticsData": analyticsData,   # <-- camelCase as requested
        "errorMsg": str(errorMsg or "")
    }

    # Optional: persist a snapshot for debugging / API retrieval
    ui_payload_id = ""
    if load_artifact and 'save_artifact' in globals():
        try:
            import pandas as pd
            # store compact tables; the dict itself may be too large for a single artifact
            tbl = pd.json_normalize(ui_payload, sep=".")
            ui_payload_id = save_artifact(tbl, "ui_payload_latest")
        except Exception as e:
            print("[ui_packager_persist] save_artifact failed:", e)

    # Return UI payload (and id if present)
    res = {**ui_payload}
    if ui_payload_id:
        res["ui_payload_id"] = ui_payload_id
    return res