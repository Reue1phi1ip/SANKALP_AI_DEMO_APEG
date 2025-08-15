import json, time

def ui_pack_and_persist(forecasts_agg_id, cards, drivers, dq_report=None, errors=None,
                        persist="false", params=None, model_plan=None, *args, **kwargs):
    """
    cards, drivers, dq_report, errors, params, model_plan can be JSON strings.
    returns: {"ui_payload":"json:ui_payload", "response_json": {...}, "run_id":"M1-..."}
    """
    import pandas as pd

    def _loads(x, fallback):
        if isinstance(x, str):
            try: return json.loads(x)
            except Exception: return fallback
        return x if x is not None else fallback

    cards     = _loads(cards, {"total_forecast":0,"confidence_range":[0,0],"series_count":0})
    drivers   = _loads(drivers, [])
    dq_report = _loads(dq_report, {})
    errors    = _loads(errors, [])
    params    = _loads(params, {})
    model_plan= _loads(model_plan, [])

    def _load(tid):
        try:
            from waveflow.artifacts import load_artifact
            return load_artifact(tid) if tid else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    agg = _load(forecasts_agg_id)

    if agg is None or agg.empty:
        payload = {
            "summary_cards": {**cards, "warnings": errors},
            "chart": {"type":"line_with_band", "series":[{"period":"—","low":0,"expected":0,"high":0}]},
            "table": [{"region":"—","period":"—","expected":0,"low":0,"high":0}],
            "drivers": drivers,
            "debug": {"dq_report": dq_report, "errors": errors}
        }
    else:
        series, table_rows = [], []
        for _, r in agg.iterrows():
            series.append({"period": str(r.get("period","—")),
                           "low": float(r.get("low",0)),
                           "expected": float(r.get("expected",0)),
                           "high": float(r.get("high",0))})
            table_rows.append({"region": str(r.get("region","—")),
                               "period": str(r.get("period","—")),
                               "expected": float(r.get("expected",0)),
                               "low": float(r.get("low",0)),
                               "high": float(r.get("high",0))})
        payload = {
            "summary_cards": {**cards, "warnings": errors},
            "chart": {"type":"line_with_band", "series": series},
            "table": table_rows,
            "drivers": drivers,
            "debug": {"dq_report": dq_report, "errors": errors}
        }

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
                model_plan=model_plan
            )
            run_id = resp.get("run_id", run_id)
        except Exception:
            pass

    return {"ui_payload": ui_id, "response_json": payload, "run_id": run_id}

