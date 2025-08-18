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
      "forecastResponse": {
        "cards": {...},
        "drivers": [...],
        "insights": ["...", "...", "..."],     # NEW: always 3 short lines
        "trend": {                             # NEW: safe default if not provided
           "labels": ["YYYY-MM", ...],
           "series": [{ "name": "Expected", "data": [ ... ] }]
        },
        "generatedAt": ISO8601Z
      },
      "forecastTable": [ {region, period, expected, low, high}, ... ],
      "analyticsData": {
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

    # ---------- Insights (exactly 3 short lines) ----------
    # Prefer insights provided by the upstream Aggregator tool (passed via kwargs)
    insights = kwargs.get("insights") or []
    if not isinstance(insights, list):
        insights = [str(insights)]
    # Normalize, trim, and pad to 3
    insights = [str(x).strip() for x in insights if str(x).strip()]
    while len(insights) < 3:
        insights.append("No further anomalies detected.")
    insights = insights[:3]

    # ---------- Trend (labels + single Expected series) ----------
    # Prefer a pre-computed trend (kwargs); else build from aggregated table
    trend = kwargs.get("trend") or {}
    if not isinstance(trend, dict) or not trend.get("labels") or not trend.get("series"):
        # Build a safe default: sum expected per period; sort by natural P# order if possible
        trend_labels = []
        trend_values = []
        if not agg_df.empty:
            # If 'period' looks like P1..Pn, sort by numeric; else just alphabetical
            def _pkey(p):
                try:
                    return int(str(p).lstrip("Pp"))
                except Exception:
                    return p
            t = (agg_df.groupby("period", as_index=False)["expected"].sum()
                        .sort_values("period", key=lambda s: s.map(_pkey)))
            trend_labels = t["period"].astype(str).tolist()
            trend_values = t["expected"].fillna(0).astype(float).tolist()

        trend = {
            "labels": trend_labels,
            "series": [{"name": "Expected", "data": trend_values}]
        }

    # ---------- Build UI payload ----------
    forecastTable = agg_df[["region", "period", "expected", "low", "high"]].copy()

    ui_payload: Dict[str, Any] = {
        "forecastResponse": {
            "cards": cards,
            "drivers": drivers,
            "insights": insights,                 # NEW
            "trend": trend,                       # NEW
            "generatedAt": datetime.utcnow().isoformat() + "Z",
        },
        "forecastTable": forecastTable.to_dict(orient="records"),
        "analyticsData": analyticsData,           # camelCase
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
