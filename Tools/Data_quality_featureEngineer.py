# Tools/Data_quality_featureEngineer.py
# Cleans the staged tables and engineers features.
# Non-breaking upgrades:
#  - Robust column aliasing/typing (prevents zeros caused by strings)
#  - Monthly aggregates for apps/promos (for charts)
#  - Returns existing keys unchanged + adds an "engineered" block

def dq_and_fe(applications_id=None, promotions_id=None, demographics_id=None, socio_econ_id=None, *args, **kwargs):
    """
    Cleans + engineers features from the fetched raw tables.
    Returns:
      {
        "cleaned": {... ids ...},
        "features": "table:features_m1",
        "dq_report": { "rows": {...}, "columns": {...}, "notes": [...] },
        "features_data": [...],                 # same as before (for debugging)
        "engineered": {                         # new OPTIONAL outputs (safe to ignore)
            "monthly_apps_id": "table:...",
            "monthly_promos_id": "table:...",
            "month_join_id": "table:..."
        }
      }
    """
    import pandas as pd

    notes = []

    # ---------- helpers ----------
    def _load(tid):
        """Load Waveflow artifact; fallback to CSV path if id looks like table:/path.csv"""
        try:
            from waveflow.artifacts import load_artifact
            return load_artifact(tid) if tid else pd.DataFrame()
        except Exception as e:
            # Fallback: try file path "table:/mnt/data/x.csv"
            try:
                if isinstance(tid, str) and tid.startswith("table:"):
                    return pd.read_csv(tid.split("table:", 1)[1])
            except Exception:
                pass
            notes.append(f"load_artifact fallback for {tid}: {e}")
            return pd.DataFrame()

    def _save(df, name):
        try:
            from waveflow.artifacts import save_artifact
            return save_artifact(df, name)
        except Exception:
            # Fallback to filesystem so we still return a stable ID
            path = f"/mnt/data/{name}.csv"
            try:
                df.to_csv(path, index=False)
            except Exception as e:
                notes.append(f"save_artifact fallback for {name}: {e}")
            return f"table:{path}"

    def _alias_and_clean(df, table_name):
        """Apply canonical schema & basic typing/trim. Non-destructive."""
        if df.empty:
            return df

        # Canonical schema
        ALIASES = {
            # geo
            "state": "geo_code", "state_code": "geo_code", "region": "geo_code",
            "region_code": "geo_code", "geoid": "geo_code",
            # applications
            "applications": "apps_count", "application_count": "apps_count",
            "apps": "apps_count", "count": "apps_count", "app_count": "apps_count",
            # promos
            "promotion_intensity": "promo_intensity", "promo": "promo_intensity",
            # demographics
            "gender": "applicant_gender", "age": "applicant_age", "income": "income_bracket",
        }
        rename_map = {c: ALIASES.get(c, c) for c in df.columns}
        df = df.rename(columns=rename_map)

        # Coerce types
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")

        for col in ("apps_count", "promo_intensity", "applicant_age"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Trim common dims
        for dim in ("scheme_id", "geo_code", "applicant_gender", "income_bracket", "occupation"):
            if dim in df.columns and pd.api.types.is_string_dtype(df[dim]):
                df[dim] = df[dim].fillna("").astype(str).str.strip()

        # DQ: warn on missing essentials for applications/promotions
        if table_name == "applications":
            for c in ["date", "scheme_id", "geo_code", "apps_count"]:
                if c not in df.columns:
                    df[c] = pd.NA
                    notes.append(f"{table_name} missing column '{c}', filled with NA")
        if table_name == "promotions":
            for c in ["date", "scheme_id", "geo_code", "promo_intensity"]:
                if c not in df.columns:
                    df[c] = pd.NA
                    notes.append(f"{table_name} missing column '{c}', filled with NA")

        return df

    # ---------- load ----------
    apps   = _alias_and_clean(_load(applications_id), "applications")
    promos = _alias_and_clean(_load(promotions_id),   "promotions")
    demo   = _alias_and_clean(_load(demographics_id), "demographics")
    socio  = _alias_and_clean(_load(socio_econ_id),   "socio_econ")

    # ---------- DQ + light pruning ----------
    if not apps.empty:
        # Drop rows with no date or no scheme/geo (they break time features & grouping)
        before = len(apps)
        apps = apps.dropna(subset=["date"])
        dropped = before - len(apps)
        if dropped > 0:
            notes.append(f"dropped {dropped} application rows with invalid/missing date")

        # Ensure numeric
        if "apps_count" in apps:
            apps["apps_count"] = apps["apps_count"].fillna(0).astype(float)
    if not promos.empty:
        if "promo_intensity" in promos:
            promos["promo_intensity"] = promos["promo_intensity"].fillna(0).astype(float)

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

    # ---------- save cleaned snapshots ----------
    cleaned = {
        "applications": _save(apps, "applications_clean"),
        "promotions":   _save(promos, "promotions_clean"),
        "demographics": _save(demo, "demographics_clean"),
        "socio_econ":   _save(socio, "socio_econ_clean"),
    }

    # ---------- feature engineering (as before, non-breaking) ----------
    if apps.empty:
        feats = pd.DataFrame(columns=["date","scheme_id","geo_code","apps_count","month","quarter","year","lag_1"])
    else:
        df = apps.copy()
        df["month"] = df["date"].dt.month
        try:
            df["quarter"] = df["date"].dt.quarter
        except Exception:
            df["quarter"] = ((df["date"].dt.month - 1)//3 + 1)
        df["year"] = df["date"].dt.year
        df = df.sort_values(["scheme_id","geo_code","date"])
        df["lag_1"] = df.groupby(["scheme_id","geo_code"], dropna=False)["apps_count"].shift(1).fillna(0.0)
        feats = df

    features_id = _save(feats, "features_m1")

    # ---------- monthly aggregates for charts (optional extras) ----------
    engineered = {"monthly_apps_id": "", "monthly_promos_id": "", "month_join_id": ""}

    if not apps.empty:
        apps_m = apps.copy()
        apps_m["month_key"] = apps_m["date"].dt.to_period("M").astype(str)
        apps_m = (
            apps_m.groupby(["scheme_id","geo_code","month_key"], dropna=False, as_index=False)
                  .agg(apps_count=("apps_count","sum"))
        )
        engineered["monthly_apps_id"] = _save(apps_m, "monthly_apps")

    if not promos.empty:
        promos_m = promos.copy()
        # Coerce date to datetime if needed
        if "date" in promos_m.columns and not pd.api.types.is_datetime64_any_dtype(promos_m["date"]):
            promos_m["date"] = pd.to_datetime(promos_m["date"], errors="coerce")
        promos_m = promos_m.dropna(subset=["date"])
        promos_m["month_key"] = promos_m["date"].dt.to_period("M").astype(str)
        promos_m = (
            promos_m.groupby(["scheme_id","geo_code","month_key"], dropna=False, as_index=False)
                    .agg(promo_intensity=("promo_intensity","sum"))
        )
        engineered["monthly_promos_id"] = _save(promos_m, "monthly_promos")
    else:
        promos_m = None

    # Join monthly apps with promos for later correlation chart
    if not apps.empty:
        if engineered["monthly_apps_id"]:
            # Recreate frames we just saved for the join (safe + simple)
            apps_m_join = apps_m
            if promos_m is not None:
                joined = pd.merge(
                    apps_m_join, promos_m,
                    on=["scheme_id","geo_code","month_key"], how="left"
                )
                joined["promo_intensity"] = joined["promo_intensity"].fillna(0.0)
            else:
                joined = apps_m_join.copy()
                joined["promo_intensity"] = 0.0

            engineered["month_join_id"] = _save(joined, "monthly_apps_promos_join")

    # ---------- return ----------
    return {
        "cleaned": cleaned,
        "features": features_id,
        "dq_report": dq_report,
        "features_data": feats.to_dict(orient="records"),
        "engineered": engineered
    }
