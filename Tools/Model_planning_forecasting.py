# Tools/Model_planning_forecasting.py
import json

def plan_and_forecast(features_id=None, timeframe=None, features_data=None, *args, **kwargs):
    """
    timeframe: next_quarter | next_6_months | next_year
    Returns:
      { "model_plan": [...],
        "forecasts_raw": "table:forecasts_raw",      # id if artifact store exists, else symbolic
        "forecasts_raw_data": [ ... ]                # ALWAYS included as in-memory fallback
      }
    """
    import pandas as pd

    # Try artifact store if present
    try:
        from waveflow.artifacts import load_artifact, save_artifact
    except Exception:
        load_artifact = save_artifact = None

    # Map timeframe to horizon (periods)
    tf = (timeframe or "").lower()
    horizon = 1 if tf == "next_quarter" else 2 if tf == "next_6_months" else 4 if tf == "next_year" else 1

    # --- Load features DF ---
    feats = pd.DataFrame()
    # 1) Artifact path
    if load_artifact and features_id:
        try:
            feats = load_artifact(features_id)
        except Exception as e:
            print("[plan_and_forecast] load_artifact failed:", e)

    # 2) In-memory fallback
    if feats.empty and features_data:
        try:
            feats = pd.DataFrame(features_data)
            print("[plan_and_forecast] using features_data fallback; rows:", len(feats))
        except Exception as e:
            print("[plan_and_forecast] features_data->DataFrame failed:", e)

    # --- Build model plan ---
    plan = []
    if not feats.empty and set(["scheme_id", "geo_code", "date"]).issubset(feats.columns):
        # ensure types
        try:
            feats["date"] = pd.to_datetime(feats["date"], errors="coerce")
        except Exception:
            pass
        key_cols_ok = feats[["scheme_id", "geo_code"]].notna().all(axis=1)
        feats_valid = feats[key_cols_ok].copy()

        for (sid, geo), g in feats_valid.groupby(["scheme_id", "geo_code"], dropna=True):
            hp = int(g.shape[0])
            model = "Prophet" if hp >= 24 else "ARIMA" if hp >= 8 else "ZeroForecast"
            plan.append({"series_id": f"{sid}|{geo}", "model": model, "history_points": hp})

    # --- Naive baseline forecasts (non-zero if we have any history) ---
    rows = []
    if feats.empty or not plan:
        # graceful empty; UI will show 0 but at least it's explicit
        for i in range(horizon):
            rows.append({
                "series_id": "—", "period": f"P{i+1}",
                "yhat": 0.0, "yhat_low": 0.0, "yhat_high": 0.0, "geo_code": "—"
            })
    else:
        # last observed value per series as baseline
        if "apps_count" in feats.columns:
            try:
                feats["apps_count"] = pd.to_numeric(feats["apps_count"], errors="coerce").fillna(0.0)
            except Exception:
                pass

        feats = feats.sort_values(["scheme_id", "geo_code", "date"], na_position="last")
        merged_key = feats["scheme_id"].astype(str) + "|" + feats["geo_code"].astype(str)

        for mp in plan:
            sid = mp.get("series_id", "—")
            base = 0.0
            try:
                part = feats[merged_key == sid]
                if not part.empty and "apps_count" in part.columns:
                    base = float(part["apps_count"].iloc[-1])
            except Exception as e:
                print("[plan_and_forecast] baseline compute failed for", sid, ":", e)
                base = 0.0

            for i in range(horizon):
                rows.append({
                    "series_id": sid, "period": f"P{i+1}",
                    "yhat": base, "yhat_low": max(0.0, base * 0.9), "yhat_high": base * 1.1,
                    "geo_code": sid.split("|")[1] if "|" in sid else "—"
                })

    df = pd.DataFrame(rows, columns=["series_id", "period", "yhat", "yhat_low", "yhat_high", "geo_code"])

    # Save if possible (ok if not available)
    try:
        forecasts_id = save_artifact(df, "forecasts_raw")
    except Exception:
        forecasts_id = "table:forecasts_raw"  # symbolic

    # Always include an in-memory fallback so downstream can proceed
    return {"model_plan": plan, "forecasts_raw": forecasts_id, "forecasts_raw_data": df.to_dict(orient="records")}
