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
      "forecasts_agg_data": [ {region, period, expected, low, high}, ... ],
      "analytics": {                               # <-- NEW (non-breaking)
        "uptake_by_state": { "labels": [...], "data": [...] },
        "monthly_trend_multi": { "labels": [...], "datasets": [ {label, data: [...]}, ... ] },
        "promotions_vs_apps": { "data": [ {x, y, label} ], "r": 0.0 or null },
        "demographics_pie": { "labels": [...], "data": [...] }
      }
    }
    """
    import math
    import pandas as pd

    # Try artifact store if available
    try:
        from waveflow.artifacts import load_artifact, save_artifact
    except Exception:
        load_artifact = save_artifact = None

    # -----------------------------
    # Load forecasts (fc)
    # -----------------------------
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

    # -----------------------------
    # Load features (fx) for analytics/drivers
    # -----------------------------
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

    # Normalize minimal schema for fx
    if not fx.empty:
        if "date" in fx.columns:
            fx["date"] = pd.to_datetime(fx["date"], errors="coerce")
        for c in ("apps_count", "promo_intensity", "applicant_age"):
            if c in fx.columns:
                fx[c] = pd.to_numeric(fx[c], errors="coerce")
        for dim in ("scheme_id", "geo_code", "applicant_gender", "income_bracket", "occupation"):
            if dim in fx.columns:
                fx[dim] = fx[dim].astype(str).str.strip()
        fx = fx.dropna(subset=["date"], how="any")

    # -----------------------------
    # Ensure required columns exist in fc
    # -----------------------------
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

    # -----------------------------
    # Aggregate + cards
    # -----------------------------
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

    # -----------------------------
    # Drivers (descriptive)
    # -----------------------------
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

    # -----------------------------
    # Analytics for charts (non-breaking extras)
    # -----------------------------
    def _to_month(dt) -> str:
        return f"{dt.year}-{str(dt.month).zfill(2)}"

    def _pearson(xs, ys):
        xs = pd.Series(xs, dtype=float)
        ys = pd.Series(ys, dtype=float)
        xs = xs.replace([pd.NA, pd.NaT], 0).fillna(0.0)
        ys = ys.replace([pd.NA, pd.NaT], 0).fillna(0.0)
        n = len(xs)
        if n == 0:
            return None
        xbar = xs.mean()
        ybar = ys.mean()
        num = ((xs - xbar) * (ys - ybar)).sum()
        den = math.sqrt(((xs - xbar)**2).sum() * ((ys - ybar)**2).sum())
        if den == 0:
            return None
        return round(float(num / den), 3)

    analytics = {
        "uptake_by_state": {"labels": [], "data": []},
        "monthly_trend_multi": {"labels": [], "datasets": []},
        "promotions_vs_apps": {"data": [], "r": None},
        "demographics_pie": {"labels": [], "data": []}
    }

    if not fx.empty:
        # --- Uptake by State ---
        if "geo_code" in fx.columns and "apps_count" in fx.columns:
            by_state = (
                fx.groupby("geo_code", as_index=False)["apps_count"].sum()
                  .sort_values("geo_code")
            )
            analytics["uptake_by_state"] = {
                "labels": by_state["geo_code"].astype(str).tolist(),
                "data": by_state["apps_count"].fillna(0).round(0).astype(int).tolist()
            }

        # --- Monthly Trend (multi-series per scheme) ---
        if {"date","scheme_id","apps_count"}.issubset(fx.columns):
            mdf = fx.copy()
            mdf["month_key"] = mdf["date"].dt.to_period("M").astype(str)
            # Complete label set (sorted)
            labels = sorted(mdf["month_key"].dropna().unique().tolist())
            datasets = []
            for sid, g in mdf.groupby("scheme_id", dropna=False):
                s = (
                    g.groupby("month_key", as_index=False)["apps_count"].sum()
                     .set_index("month_key").reindex(labels).fillna(0)
                )
                datasets.append({
                    "label": str(sid),
                    "data": s["apps_count"].round(0).astype(int).tolist()
                })
            analytics["monthly_trend_multi"] = {"labels": labels, "datasets": datasets}

        # --- Promotions vs Applications (scatter + Pearson r) ---
        if {"date","scheme_id","geo_code","apps_count"}.issubset(fx.columns) and ("promo_intensity" in fx.columns):
            j = fx.copy()
            j["month_key"] = j["date"].dt.to_period("M").astype(str)
            # monthly sum per series for both metrics
            apps_m = (
                j.groupby(["scheme_id","geo_code","month_key"], as_index=False)["apps_count"].sum()
            )
            promo_m = (
                j.groupby(["scheme_id","geo_code","month_key"], as_index=False)["promo_intensity"].sum()
            )
            merged = pd.merge(apps_m, promo_m, on=["scheme_id","geo_code","month_key"], how="inner")
            scatter = [
                {
                    "x": float(row["promo_intensity"]),
                    "y": float(row["apps_count"]),
                    "label": f'{row["scheme_id"]} {row["geo_code"]} {row["month_key"]}'
                }
                for _, row in merged.iterrows()
            ]
            r = _pearson(merged["promo_intensity"], merged["apps_count"]) if len(merged) else None
            analytics["promotions_vs_apps"] = {"data": scatter, "r": r}

        # --- Demographics pie (auto-detect best available) ---
        # Preference order: gender -> income -> occupation -> age buckets
        if "applicant_gender" in fx.columns and fx["applicant_gender"].notna().any():
            g = (fx.groupby("applicant_gender", as_index=False)["apps_count"].sum()
                    .sort_values("apps_count", ascending=False))
            analytics["demographics_pie"] = {
                "labels": g["applicant_gender"].astype(str).tolist(),
                "data": g["apps_count"].fillna(0).round(0).astype(int).tolist()
            }
        elif "income_bracket" in fx.columns and fx["income_bracket"].notna().any():
            g = (fx.groupby("income_bracket", as_index=False)["apps_count"].sum()
                    .sort_values("apps_count", ascending=False))
            analytics["demographics_pie"] = {
                "labels": g["income_bracket"].astype(str).tolist(),
                "data": g["apps_count"].fillna(0).round(0).astype(int).tolist()
            }
        elif "occupation" in fx.columns and fx["occupation"].notna().any():
            g = (fx.groupby("occupation", as_index=False)["apps_count"].sum()
                    .sort_values("apps_count", ascending=False))
            analytics["demographics_pie"] = {
                "labels": g["occupation"].astype(str).tolist(),
                "data": g["apps_count"].fillna(0).round(0).astype(int).tolist()
            }
        elif "applicant_age" in fx.columns and fx["applicant_age"].notna().any():
            # Age buckets: <18, 18–25, 26–35, 36–50, 50+
            def age_bucket(x):
                try:
                    a = float(x)
                except Exception:
                    return "Unknown"
                if math.isnan(a):
                    return "Unknown"
                a = int(a)
                if a < 18: return "<18"
                if a <= 25: return "18–25"
                if a <= 35: return "26–35"
                if a <= 50: return "36–50"
                return "50+"
            tmp = fx.copy()
            tmp["age_bucket"] = tmp["applicant_age"].apply(age_bucket)
            g = (tmp.groupby("age_bucket", as_index=False)["apps_count"].sum()
                    .sort_values("apps_count", ascending=False))
            analytics["demographics_pie"] = {
                "labels": g["age_bucket"].astype(str).tolist(),
                "data": g["apps_count"].fillna(0).round(0).astype(int).tolist()
            }

    # -----------------------------
    # Persist aggregate if possible (optional)
    # -----------------------------
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
        "forecasts_agg_data": agg.to_dict(orient="records"),
        "analytics": analytics
    }
