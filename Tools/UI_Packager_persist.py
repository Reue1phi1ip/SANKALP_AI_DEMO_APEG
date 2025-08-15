# Tools/UI_Packager_persist.py
import json, time

def ui_pack_and_persist(
    forecasts_agg_id,
    cards,
    drivers,
    dq_report=None,
    errors=None,
    persist="false",
    params=None,
    model_plan=None,
    # NEW: in-memory fallback if no artifact store:
    forecasts_agg_data=None,
    *args,
    **kwargs
):
    """
    Returns:
      {
        "ui_payload": "json:ui_payload" | <id if storage available>,
        "response_json": {
           "summary_cards": {...},
           "chart": {"type":"line_with_band","series":[...]},
           "table": [...],
           "drivers": [...],
           "debug": {"dq_report": {...}, "errors": [...]}
        },
        "run_id": "M1-<epoch>"
      }
    """
    import pandas as pd

    def _loads(x, fallback):
        if isinstance(x, str):
            try:
                return json.loads(x)
            except Exception:
                return fallback
        return x if x is not None else fallback

    # accept both JSON strings and native objects
    cards        = _loads(cards,     {"total_forecast": 0, "confidence_range": [0,0], "series_count": 0})
    drivers      = _loads(drivers,   [])
    dq_report    = _loads(dq_report, {})
    errors       = _loads(errors,    [])
    params       = _loads(params,    {})
    model_plan   = _loads(model_plan, [])
    agg_fallback = _loads(forecasts_agg_data, None)  # may be list[dict] or None

    # try artifact store
    def _load_df(tid):
        try:
            from waveflow.artifacts import load_artifact
            return load_artifact(tid) if tid else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    agg = _load_df(forecasts_agg_id)

    # if artifact empty, use in-memory fallback
    if (agg is None or agg.empty) and isinstance(agg_fallback, list) and agg_fallback:
        try:
            agg = pd.DataFrame(agg_fallback)
            print("[ui_pack_and_persist] using forecasts_agg_data fallback; rows:", len(agg))
        except Exception as e:
            print("[ui_pack_and_persist] forecasts_agg_data->DF failed:", e)
            agg = pd.DataFrame()

    # normalize columns we expect
    if agg is None or agg.empty:
        payload = {
            "summary_cards": {**cards, "warnings": errors},
            "chart": {"type": "line_with_band", "series": [{"period": "—", "low": 0, "expected": 0, "high": 0}]},
            "table": [{"region": "—", "period": "—", "expected": 0, "low": 0, "high": 0}],
            "drivers": drivers or ["Drivers unavailable"],
            "debug": {"dq_report": dq_report, "errors": errors},
        }
    else:
        # coerce numeric + strings
        for c in ("expected", "low", "high"):
            if c not in agg.columns:
                agg[c] = 0.0
            agg[c] = pd.to_numeric(agg[c], errors="coerce").fillna(0.0)
        if "period" not in agg.columns:
            agg["period"] = [f"P{i+1}" for i in range(len(agg))]
        if "region" not in agg.columns:
            agg["region"] = "—"

        series = [
            {
                "period":   str(r.get("period", "—")),
                "low":      float(r.get("low", 0)),
                "expected": float(r.get("expected", 0)),
                "high":     float(r.get("high", 0)),
            }
            for _, r in agg.iterrows()
        ]

        table_rows = [
            {
                "region":   str(r.get("region", "—")),
                "period":   str(r.get("period", "—")),
                "expected": float(r.get("expected", 0)),
                "low":      float(r.get("low", 0)),
                "high":     float(r.get("high", 0)),
            }
            for _, r in agg.iterrows()
        ]

        payload = {
            "summary_cards": {**cards, "warnings": errors},
            "chart": {"type": "line_with_band", "series": series},
            "table": table_rows,
            "drivers": drivers or ["Drivers unavailable"],
            "debug": {"dq_report": dq_report, "errors": errors},
        }

    # try to persist UI payload if storage exists; otherwise return a symbolic id
    try:
        from waveflow.storage import save_json
        ui_id = save_json(payload, "ui_payload")
    except Exception:
        ui_id = "json:ui_payload"

    run_id = f"M1-{int(time.time())}"
    if str(persist).lower() == "true":
        try:
            from waveflow.runs import persist_run
            resp = persist_run(
                artifacts={"forecasts_agg": forecasts_agg_id, "ui_payload": ui_id},
                response_json=payload,
                params=params,
                model_plan=model_plan,
            )
            run_id = resp.get("run_id", run_id)
        except Exception:
            pass

    return {"ui_payload": ui_id, "response_json": payload, "run_id": run_id}