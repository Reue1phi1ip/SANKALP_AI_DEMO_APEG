# Tools/Aggregator_Drivers.py

def aggregate_and_drivers(
    forecasts_raw_id=None,
    features_id=None,
    forecasts_raw_data=None,   # <-- in-memory fallback
    features_data=None,        # <-- in-memory fallback
    *args, **kwargs
):
    """
    Returns:
    {
      "forecasts_agg": "table:forecasts_agg",
      "cards": {"total_forecast":..., "confidence_range":[lo,hi], "series_count":...},
      "drivers": [ ... ],
      "forecasts_agg_data": [ {region, period, expected, low, high}, ... ]   # <-- added for UI packager
    }
    """
    import pandas as pd

    # Try artifact store if available
    try:
        from waveflow.artifacts import load_artifact, save_artifact
    except Exception:
        load_artifact = save_artifact = None

    # --- Load forecasts (fc) ---
    fc = pd.DataFrame()
    if load_artifact and forecasts_raw_id:
        try:
            fc = load_artifact(forecasts_raw_id)
        except Exception as e:
            print("[aggregate] load_artifact(forecasts_raw_id) failed:", e)

    if fc.empty and forecasts_raw_data:
        try:
            fc = pd.DataFrame(forecasts_raw_data)
            print("[aggregate] using forecasts_raw_data fallback; rows:", len(fc))
        except Exception as e:
            print("[aggregate] forecasts_raw_data->DF failed:", e)

    # --- Load features (fx) for driver hints ---
    fx = pd.DataFrame()
    if load_artifact and features_id:
        try:
            fx = load_artifact(features_id)
        except Exception as e:
            print("[aggregate] load_artifact(features_id) failed:", e)

    if fx.empty and features_data:
        try:
            fx = pd.DataFrame(features_data)
        except Exception as e:
            print("[aggregate] features_data->DF failed:", e)

    # --- Ensure required columns exist in fc ---
    for c in ("yhat", "yhat_low", "yhat_high"):
        if c not in fc.columns:
            fc[c] = 0.0

    if "period" not in fc.columns:
        # provide a simple fallback period label
        fc["period"] = [f"P{i+1}" for i in range(len(fc))]

    if "geo_code" not in fc.columns:
        if "series_id" in fc.columns:
            fc["geo_code"] = fc["series_id"].astype(str).str.split("|").str[1].fillna("—")
        else:
            fc["geo_code"] = "—"

    # Numeric coercion
    for c in ("yhat", "yhat_low", "yhat_high"):
        fc[c] = pd.to_numeric(fc[c], errors="coerce").fillna(0.0).astype(float)

    # --- Aggregate + cards ---
    if fc.empty:
        agg = pd.DataFrame(columns=["region", "period", "expected", "low", "high"])
        cards = {
            "total_forecast": 0.0,
            "confidence_range": [0.0, 0.0],
            "series_count": 0
        }
    else:
        work = fc.copy()
        work["region"]   = work["geo_code"].astype(str)
        work["expected"] = work["yhat"]
        work["low"]      = work["yhat_low"]
        work["high"]     = work["yhat_high"]

        agg = work.groupby(["region", "period"], as_index=False)[["expected","low","high"]].sum()

        series_count = 0
        if "series_id" in work.columns:
            try:
                series_count = int(work["series_id"].nunique())
            except Exception:
                series_count = 0
        if not series_count:
            series_count = int(work["region"].nunique())

        cards = {
            "total_forecast": float(agg["expected"].sum()) if not agg.empty else 0.0,
            "confidence_range": [
                float(agg["low"].sum()) if not agg.empty else 0.0,
                float(agg["high"].sum()) if not agg.empty else 0.0
            ],
            "series_count": series_count
        }

    # --- Drivers (descriptive) ---
    drivers = []
    if not fx.empty and "promo_intensity" in fx.columns:
        try:
            if float(pd.to_numeric(fx["promo_intensity"], errors="coerce").fillna(0).sum()) > 0:
                drivers.append("Observed promotional activity in prior periods; relationship not assessed.")
        except Exception:
            pass

    if not fx.empty and (("quarter" in fx.columns) or ("month" in fx.columns)):
        drivers.append("Seasonal fields present (quarter/month); detailed analysis pending.")

    if not fc.empty and {"yhat_low","yhat_high"}.issubset(fc.columns):
        try:
            band = (fc["yhat_high"].fillna(0) - fc["yhat_low"].fillna(0)).abs()
            if band.mean() > 0:
                drivers.append("Forecast confidence bands vary across periods.")
        except Exception:
            pass

    if not drivers:
        drivers = ["Drivers unavailable"]

    # --- Persist aggregate if possible (optional) ---
    agg_id = "table:forecasts_agg"
    if save_artifact:
        try:
            agg_id = save_artifact(agg, "forecasts_agg")
        except Exception:
            pass

    return {
        "forecasts_agg": agg_id,
        "cards": cards,
        "drivers": drivers,
        "forecasts_agg_data": agg.to_dict(orient="records")  # helps UI packager
    }