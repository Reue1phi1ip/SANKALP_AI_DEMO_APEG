import json, requests

def waveflowdb_fetch(base_url, api_key, database, collection, query=None, limit=100000, *args, **kwargs):
    """
    query: JSON string or dict (optional)
    returns: {"ok": True, "data": {...}} | {"ok": False, "error": "..."}
    """
    if not base_url or not api_key:
        return {"ok": False, "error": "Missing base_url or api_key"}
    if isinstance(query, str):
        try: query = json.loads(query) if query.strip() else {}
        except Exception: query = {}
    if not isinstance(query, dict): query = {}

    url = f"{base_url}/v1/fetch"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    body = {"database": database, "collection": collection, "query": query, "limit": int(limit)}

    try:
        r = requests.post(url, headers=headers, json=body, timeout=60)
        if r.status_code != 200:
            return {"ok": False, "error": f"HTTP {r.status_code}: {r.text}"}
        return {"ok": True, "data": r.json()}
    except Exception as e:
        return {"ok": False, "error": str(e)}