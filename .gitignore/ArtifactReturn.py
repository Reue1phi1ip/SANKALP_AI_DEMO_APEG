import json

def rows_to_artifact(payload, name, data=None, *args, **kwargs):
    """
    payload: {"ok":..., "data": ...}  OR JSON string
    data: optional raw rows (dict/list or JSON string) — if provided, used instead of payload.data
    returns: {"ok": True, "table_id": "table:..."} | {"ok": False, "error":"..."}
    """
    if isinstance(payload, str):
        try: payload = json.loads(payload)
        except Exception: payload = {}
    if isinstance(data, str):
        try: data = json.loads(data)
        except Exception: data = None

    if data is None:
        if not (isinstance(payload, dict) and payload.get("ok")):
            return {"ok": False, "error": (payload.get("error") if isinstance(payload, dict) else "Fetch failed")}
        data = (payload.get("data") or {})
    rows = data.get("rows", data if isinstance(data, list) else [])
    try:
        import pandas as pd
        df = pd.DataFrame(rows)
    except Exception as e:
        return {"ok": False, "error": f"Pandas error: {e}"}
    try:
        from waveflow.artifacts import save_artifact
        table_id = save_artifact(df, name or "rows_artifact")
    except Exception:
        table_id = f"table:{name or 'rows_artifact'}"
    return {"ok": True, "table_id": table_id}