def fe_timeseries(applications_id=None, promotions_id=None, demographics_id=None, socio_econ_id=None, *args, **kwargs):
    """
    returns: {"features":"table:features_m1", "cols":[...]}
    """
    import pandas as pd
    def _load(tid):
        try:
            from waveflow.artifacts import load_artifact
            return load_artifact(tid) if tid else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    apps = _load(applications_id)
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
        tid = save_artifact(feats, "features_m1")
    except Exception:
        tid = "table:features_m1"
    return {"features": tid, "cols": list(feats.columns)}