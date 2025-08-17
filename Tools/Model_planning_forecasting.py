# Tools/Model_planning_forecasting.py
# Builds a simple model plan and produces monthly forecasts per (scheme_id, geo_code).
# Non-breaking upgrades:
#  - Uses monthly history (sum) per series
#  - Horizon mapped to months (quarter=3, 6 months=6, year=12)
#  - Seasonal-naive / 3-mo moving average / last-value fallback
#  - Adds 'forecast_month' (YYYY-MM) while preserving 'period' (P1..Ph)

import json

def plan_and_forecast(features_id=None, timeframe=None, features_data=None, *args, **kwargs):
    """
    timeframe: next_quarter | next_6_months | next_year  (defaults to next_quarter)
    Returns:
      {
        "model_plan": [...],
        "forecasts_raw": "table:forecasts_raw",
        "forecasts_raw_data": [ ... ]    # always present
      }
    """
    import pandas as pd
    from datetime import datetime
    from dateutil.relativedelta import relativedelta

    # Try artifact store if present (Waveflow)
    try:
        from waveflow.artifacts import load_artifact, save_artifact
    except Exception:
        load_artifact = save_artifact = None

    # ---- horizon mapping (in months) ----
    tf = (timeframe or "").lower()
    horizon = 3 if tf in ("", "next_quarter") else 6 if tf == "next_6_months" else 12 if tf == "next_year" else 3

    # ---- Load features DF ----
    feats = pd.DataFrame()
    if load_artifact and features_id:
        try:
            feats = load_artifact(features_id)
        except Exception as e:
            print("[plan_and_forecast] load_artifact failed:", e)

    if feats.empty and features_data:
        try:
            feats = pd.DataFrame(features_data)
            print("[plan_and_forecast] using features_data fallback; rows:", len(feats))
        except Exception as e:
            print("[plan_and_forecast] features_data->DataFrame failed:", e)

    # ---- Guard: empty -> explicit zeros (preserve API) ----
    if feats.empty:
        rows = []
        for i in range(horizon):
            rows.append({
                "series_id": "—",
                "scheme_id": None,
                "geo_code": "—",
                "period": f"P{i+1}",
                "forecast_month": None,
                "yhat": 0.0, "yhat_low": 0.0, "yhat_high": 0.0,
                "model": "ZeroForecast",
                "history_points": 0
            })
        return {
            "model_plan": [],
            "forecasts_raw": "table:forecasts_raw",
            "forecasts_raw_data": rows
        }

    # ---- Normalize minimal schema/types ----
    # (tolerant if columns already clean)
    if "date" in feats.columns:
        feats["date"] = pd.to_datetime(feats["date"], errors="coerce")
    if "apps_count" in feats.columns:
        feats["apps_count"] = pd.to_numeric(feats["apps_count"], errors="coerce").fillna(0.0)
    for col in ("scheme_id", "geo_code"):
        if col not in feats.columns:
            feats[col] = pd.NA

    feats = feats.dropna(subset=["date"])
    feats["scheme_id"] = feats["scheme_id"].astype(str)
    feats["geo_code"]  = feats["geo_code"].astype(str)

    # ---- Build model plan based on history length ----
    plan = []
    rows = []

    # Utility: month key + resample monthly sum per series
    def month_key(dt: pd.Timestamp) -> str:
        return f"{dt.year}-{str(dt.month).zfill(2)}"

    # For each series, create continuous monthly index and forecasts
    for (sid, geo), g in feats.groupby(["scheme_id", "geo_code"], dropna=True):
        g = g.sort_values("date")
        # monthly sum
        g_m = g.set_index("date").resample("MS")["apps_count"].sum().rename("y").to_frame()

        # history points and model choice
        hp = int(len(g_m))
        if hp >= 18:
            model_name = "SeasonalNaive"
        elif hp >= 3:
            model_name = "MovingAverage-3"
        else:
            model_name = "LastValue"

        plan.append({"series_id": f"{sid}|{geo}", "model": model_name, "history_points": hp})

        # determine start month for forecasting (month after last history)
        if hp == 0:
            # just pad zeros
            last_month = pd.Timestamp.today().to_period("M").to_timestamp()
        else:
            last_month = g_m.index.max()

        # generate forecasts
        base_vals = g_m["y"].values.tolist()

        # helpers for seasonal and MA
        def seasonal_forecast(step_idx: int) -> float:
            # same month last year if exists else fallback to MA
            target = (last_month + pd.offsets.MonthBegin(step_idx+1))
            ly = target - pd.DateOffset(years=1)
            if ly in g_m.index:
                return float(g_m.loc[ly, "y"])
            return ma3_forecast()

        def ma3_forecast() -> float:
            if len(base_vals) >= 3:
                return float(pd.Series(base_vals[-3:]).mean())
            return float(base_vals[-1]) if base_vals else 0.0

        def last_forecast() -> float:
            return float(base_vals[-1]) if base_vals else 0.0

        for i in range(horizon):
            # compute month label
            f_month_dt = last_month + pd.offsets.MonthBegin(i+1)
            if model_name == "SeasonalNaive":
                yhat = seasonal_forecast(i)
            elif model_name == "MovingAverage-3":
                yhat = ma3_forecast()
            else:
                yhat = last_forecast()

            yhat = max(0.0, float(yhat))
            rows.append({
                "series_id": f"{sid}|{geo}",
                "scheme_id": sid,
                "geo_code": geo,
                "period": f"P{i+1}",
                "forecast_month": month_key(f_month_dt),
                "yhat": yhat,
                "yhat_low": round(max(0.0, yhat * 0.9), 3),
                "yhat_high": round(yhat * 1.1, 3),
                "model": model_name,
                "history_points": hp
            })

    # ---- Package and (optionally) save ----
    out_df = pd.DataFrame(rows, columns=[
        "series_id","scheme_id","geo_code","period","forecast_month",
        "yhat","yhat_low","yhat_high","model","history_points"
    ])

    try:
        forecasts_id = save_artifact(out_df, "forecasts_raw")
    except Exception:
        forecasts_id = "table:forecasts_raw"  # symbolic / fallback

    return {
        "model_plan": plan,
        "forecasts_raw": forecasts_id,
        "forecasts_raw_data": out_df.to_dict(orient="records")
    }