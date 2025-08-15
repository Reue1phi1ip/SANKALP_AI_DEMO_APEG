# Tools/Data_quality_featureEngineer.py
def dq_and_fe(applications_id=None, promotions_id=None, demographics_id=None, socio_econ_id=None, *args, **kwargs):
    """
    Cleans + engineers features from the fetched raw tables.
    Returns:
      {
        "cleaned": {... ids ...},
        "features": "table:features_m1",
        "dq_report": { "rows": {...}, "columns": {...}, "notes": [...] }
      }
    """
    import pandas as pd
    notes = []

    def _load(tid):
        try:
            from waveflow.artifacts import load_artifact
            return load_artifact(tid) if tid else pd.DataFrame()
        except Exception as e:
            notes.append(f"load_artifact fallback for {tid}: {e}")
            return pd.DataFrame()

    apps  = _load(applications_id)
    promos= _load(promotions_id)
    demo  = _load(demographics_id)
    socio = _load(socio_econ_id)

    # ---- DQ: normalize essential columns on applications ----
    if not apps.empty:
        # Normalize column names we need later
        for c in ["date","scheme_id","geo_code","apps_count"]:
            if c not in apps.columns:
                # try common aliases
                if c == "geo_code":
                    for alt in ["geo", "region_code", "state_code", "geoid"]:
                        if alt in apps.columns:
                            apps[c] = apps[alt]; break
                elif c == "apps_count":
                    for alt in ["applications","app_count","count"]:
                        if alt in apps.columns:
                            apps[c] = apps[alt]; break
                if c not in apps.columns:
                    apps[c] = None
                    notes.append(f"apps missing column '{c}', filled with None")

        # types
        apps["apps_count"] = pd.to_numeric(apps.get("apps_count", 0), errors="coerce").fillna(0.0)
        apps["date"] = pd.to_datetime(apps.get("date"), errors="coerce")
        # drop rows with no date (they will break time features)
        before = len(apps)
        apps = apps.dropna(subset=["date"])
        after = len(apps)
        if after < before:
            notes.append(f"dropped {before-after} application rows with invalid/missing date")

    # promotions: keep numeric field if present
    if not promos.empty:
        promos["promo_intensity"] = pd.to_numeric(promos.get("promo_intensity", 0), errors="coerce").fillna(0.0)

    dq_report = {
        "rows": {
            "applications": int(len(apps.index)),
            "promotions":   int(len(promos.index)),
            "demographics": int(len(demo.index)),
            "socio_econ":   int(len(socio.index)),
        },
        "columns": {
            "applications": list(apps.columns),
            "promotions":   list(promos.columns),
            "demographics": list(demo.columns),
            "socio_econ":   list(socio.columns),
        },
        "notes": notes,
    }

    # ---- Save cleaned snapshots (useful for debugging downstream) ----
    try:
        from waveflow.artifacts import save_artifact
        cleaned = {
            "applications": save_artifact(apps, "applications_clean"),
            "promotions":   save_artifact(promos, "promotions_clean"),
            "demographics": save_artifact(demo, "demographics_clean"),
            "socio_econ":   save_artifact(socio, "socio_econ_clean"),
        }
    except Exception:
        cleaned = {
            "applications": "table:applications_clean",
            "promotions":   "table:promotions_clean",
            "demographics": "table:demographics_clean",
            "socio_econ":   "table:socio_econ_clean",
        }

    # ---- Feature Engineering ----
    if apps.empty:
        feats = pd.DataFrame(columns=["date","scheme_id","geo_code","apps_count","month","quarter","year","lag_1"])
    else:
        df = apps.copy()
        df["month"] = df["date"].dt.month
        try:
            df["quarter"] = df["date"].dt.quarter
        except Exception:
            df["quarter"] = ((df["date"].dt.month-1)//3 + 1)
        df["year"] = df["date"].dt.year
        df = df.sort_values(["scheme_id","geo_code","date"])
        df["lag_1"] = df.groupby(["scheme_id","geo_code"])["apps_count"].shift(1).fillna(0.0)
        feats = df

    try:
        from waveflow.artifacts import save_artifact
        features_id = save_artifact(feats, "features_m1")
    except Exception:
        features_id = "table:features_m1"

    return {
  "cleaned": cleaned,
  "features": features_id,
  "dq_report": dq_report,
  "features_data": feats.to_dict(orient="records")  # <— add
}

