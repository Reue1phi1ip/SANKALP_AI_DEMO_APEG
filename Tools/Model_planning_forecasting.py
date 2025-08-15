import json

def plan_and_forecast(features_id, timeframe, *args, **kwargs):
    """
    timeframe: next_quarter | next_6_months | next_year
    returns: { "model_plan":[...], "forecasts_raw":"table:forecasts_raw" }
    """
    import pandas as pd
    try:
        from waveflow.artifacts import load_artifact, save_artifact
    except Exception:
        load_artifact = save_artifact = None

    tf = (timeframe or "").lower()
    horizon = 1 if tf == "next_quarter" else 2 if tf == "next_6_months" else 4 if tf == "next_year" else 1

    try:
        feats = load_artifact(features_id) if load_artifact else pd.DataFrame()
    except Exception:
        feats = pd.DataFrame()

    # Profile + plan
    plan = []
    if not feats.empty and set(["scheme_id","geo_code","date"]).issubset(feats.columns):
        for (sid, geo), g in feats.dropna(subset=["scheme_id","geo_code"]).groupby(["scheme_id","geo_code"]):
            hp = int(g.shape[0])
            model = "Prophet" if hp >= 24 else "ARIMA" if hp >= 8 else "ZeroForecast"
            plan.append({"series_id": f"{sid}|{geo}", "model": model, "history_points": hp})

    # Forecasts (baseline = last observed; zero-safe)
    rows = []
    if feats.empty or not plan:
        for i in range(horizon):
            rows.append({"series_id":"—","period":f"P{i+1}","yhat":0.0,"yhat_low":0.0,"yhat_high":0.0,"geo_code":"—"})
    else:
        for mp in plan:
            sid = mp.get("series_id","—")
            base = 0.0
            try:
                key = feats["scheme_id"].astype(str) + "|" + feats["geo_code"].astype(str)
                part = feats[key == sid]
                if not part.empty and "apps_count" in part.columns:
                    base = float(part.sort_values("date")["apps_count"].iloc[-1])
            except Exception:
                base = 0.0
            for i in range(horizon):
                rows.append({
                    "series_id": sid, "period": f"P{i+1}",
                    "yhat": base, "yhat_low": max(0.0, base*0.9), "yhat_high": base*1.1,
                    "geo_code": sid.split("|")[1] if "|" in sid else "—"
                })

    df = pd.DataFrame(rows, columns=["series_id","period","yhat","yhat_low","yhat_high","geo_code"])
    try:
        forecasts_id = save_artifact(df, "forecasts_raw")
    except Exception:
        forecasts_id = "table:forecasts_raw"

    return {"model_plan": plan, "forecasts_raw": forecasts_id}