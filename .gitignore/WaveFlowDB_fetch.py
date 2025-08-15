import requests

def waveflowdb_fetch(params: dict) -> dict:
    # params: base_url, api_key, database, collection, query{}, limit
    base_url = params.get("base_url")
    api_key  = params.get("api_key")
    body = {
        "database": params.get("database"),
        "collection": params.get("collection"),
        "query": params.get("query", {}),
        "limit": params.get("limit", 100000)
    }
    if not base_url or not api_key:
        return {"ok": False, "error": "Missing base_url or api_key"}

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    url = f"{base_url}/v1/fetch"  # change if your admin gave a different path

    try:
        r = requests.post(url, headers=headers, json=body, timeout=60)
        if r.status_code != 200:
            return {"ok": False, "error": f"HTTP {r.status_code}: {r.text}"}
        return {"ok": True, "data": r.json()}
    except Exception as e:
        return {"ok": False, "error": str(e)}