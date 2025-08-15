def dq_and_fe(applications_id=None, promotions_id=None, demographics_id=None, socio_econ_id=None, *args, **kwargs):
    """
    Input: raw table ids from Tool 1
    Output:
    {
      "cleaned": {... ids ...},
      "features": "table:features_m1",
      "dq_report": {...}
    }
    """
    import pandas as pd
    def _load(tid):
        try:
            from waveflow.artifacts import load_artifact
            return load_artifact(tid) if tid else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    apps  = _load(applications_id)
    promos= _load(promotions_id)
    demo  = _load(demographics_id)
    socio = _load(socio_econ_id)

    # DQ — zero-safe, no imputation beyond numeric coercion
    if not apps.empty:
        for c in ["date","scheme_id","geo_code"]:
            if c not in apps.columns: apps[c] = None
        apps["apps_count"] = pd.to_numeric(apps.get("apps_count", 0), errors="coerce").fillna(0).astype(float)
    if not promos.empty:
        for c in ["date","scheme_id","geo_code"]:
            if c not in promos.columns: promos[c] = None
        promos["promo_intensity"] = pd.to_numeric(promos.get("promo_intensity", 0), errors="coerce").fillna(0).astype(float)

    dq_report = {"rows": {
        "applications": int(len(apps.index)), "promotions": int(len(promos.index)),
        "demographics": int(len(demo.index)), "socio_econ": int(len(socio.index))
    }}

    # Save cleaned
    try:
        from waveflow.artifacts import save_artifact
        cleaned = {
            "applications": save_artifact(apps, "applications_clean"),
            "promotions":   save_artifact(promos, "promotions_clean"),
            "demographics": save_artifact(demo, "demographics_clean"),
            "socio_econ":   save_artifact(socio, "socio_econ_clean")
        }
    except Exception:
        cleaned = {
            "applications": "table:applications_clean",
            "promotions":   "table:promotions_clean",
            "demographics": "table:demographics_clean",
            "socio_econ":   "table:socio_econ_clean"
        }

    # FE — time parts + lag
    if apps.empty:
        feats = pd.DataFrame(columns=["date","scheme_id","geo_code","apps_count","month","quarter","year","lag_1"])
    else:
        df = apps.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["month"] = df["date"].dt.month
        try: df["quarter"] = df["date"].dt.quarter
        except Exception: df["quarter"] = None
        df["year"] = df["date"].dt.year
        df = df.sort_values(["scheme_id","geo_code","date"])
        df["lag_1"] = df.groupby(["scheme_id","geo_code"])["apps_count"].shift(1).fillna(0)
        feats = df

    try:
        from waveflow.artifacts import save_artifact
        features_id = save_artifact(feats, "features_m1")
    except Exception:
        features_id = "table:features_m1"

    return {"cleaned": cleaned, "features": features_id, "dq_report": dq_report}