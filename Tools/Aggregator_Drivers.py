def aggregate_and_drivers(forecasts_raw_id, features_id=None, *args, **kwargs):
    """
    returns:
    {
      "forecasts_agg":"table:forecasts_agg",
      "cards": {"total_forecast":..., "confidence_range":[lo,hi], "series_count":...},
      "drivers": [str, ...]
    }
    """
    import pandas as pd
    def _load(tid):
        try:
            from waveflow.artifacts import load_artifact
            return load_artifact(tid) if tid else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    fc  = _load(forecasts_raw_id)
    fx  = _load(features_id) if features_id else pd.DataFrame()

    # Aggregate + cards
    if fc is None or fc.empty:
        agg = pd.DataFrame(columns=["region","period","expected","low","high"])
        cards = {"total_forecast": 0.0, "confidence_range": [0.0, 0.0], "series_count": 0}
    else:
        work = fc.copy()
        for c in ["yhat","yhat_low","yhat_high"]:
            if c not in work.columns: work[c] = 0.0
        work["region"]   = work["geo_code"] if "geo_code" in work.columns else "—"
        work["expected"] = pd.to_numeric(work["yhat"], errors="coerce").fillna(0.0).astype(float)
        work["low"]      = pd.to_numeric(work["yhat_low"], errors="coerce").fillna(0.0).astype(float)
        work["high"]     = pd.to_numeric(work["yhat_high"], errors="coerce").fillna(0.0).astype(float)
        agg = work.groupby(["region","period"], as_index=False)[["expected","low","high"]].sum()
        cards = {
            "total_forecast": float(agg["expected"].sum()) if not agg.empty else 0.0,
            "confidence_range": [
                float(agg["low"].sum()) if not agg.empty else 0.0,
                float(agg["high"].sum()) if not agg.empty else 0.0
            ],
            "series_count": int(work["series_id"].nunique()) if "series_id" in work.columns else 0
        }

    # Drivers (descriptive, non-causal)
    drivers = []
    if not fx.empty and "promo_intensity" in fx.columns:
        try:
            if float(fx["promo_intensity"].fillna(0).sum()) > 0:
                drivers.append("Observed promotional activity in prior periods; relationship not assessed.")
        except Exception:
            pass
    if not fx.empty and ("quarter" in fx.columns or "month" in fx.columns):
        drivers.append("Seasonal fields present (quarter/month); detailed analysis pending.")
    if not fc.empty and {"yhat","yhat_low","yhat_high"}.issubset(fc.columns):
        try:
            band = (fc["yhat_high"].fillna(0) - fc["yhat_low"].fillna(0)).abs()
            if band.mean() > 0:
                drivers.append("Forecast confidence bands vary across periods.")
        except Exception:
            pass
    if not drivers:
        drivers = ["Drivers unavailable"]

    try:
        from waveflow.artifacts import save_artifact
        agg_id = save_artifact(agg, "forecasts_agg")
    except Exception:
        agg_id = "table:forecasts_agg"

    return {"forecasts_agg": agg_id, "cards": cards, "drivers": drivers}