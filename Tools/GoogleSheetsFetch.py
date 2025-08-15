def sheets_fetch_stage(
    sheet_id,
    tabs,
    sheets_creds_json,
    header_row="1",
    limit="250000",
    *args,
    **kwargs
):
    """
    Reads one or more tabs from a Google Sheet and stages them as artifacts.

    Params (strings):
      sheet_id           : Google Sheet ID (from URL)
      tabs               : comma-separated worksheet names, e.g. "applications,promotions,demographics,socio_econ"
      sheets_creds_json  : FULL Service Account JSON as a string (or a dict)
      header_row         : header row index (default "1")
      limit              : max rows per tab (default "250000")
    """
    import json
    import pandas as pd

    # ------------------------------
    # 1) Import Google deps
    # ------------------------------
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        import gspread.utils as U
    except Exception as e:
        return {"ok": False, "error": f"Missing gspread/google-auth deps: {e}"}

    # ------------------------------
    # 2) Normalize numeric params
    # ------------------------------
    try:
        hdr = int(header_row)
    except Exception:
        hdr = 1
    try:
        lim = int(limit)
    except Exception:
        lim = 250000

    # ------------------------------
    # 3) Fallbacks for creds & sheet
    #    Prefer request values, else env, else optional hard-coded
    # ------------------------------
    import os

    FALLBACK_SA_JSON = os.getenv("SHEETS_SA_JSON", "").strip()
    FALLBACK_SHEET_ID = os.getenv("FALLBACK_SHEET_ID", "").strip()

    # OPTIONAL DEV-ONLY hard-codes (leave blank for prod; or set as env vars above)
    if not FALLBACK_SA_JSON:
        FALLBACK_SA_JSON = ""  # e.g. '{"type":"service_account",...}'
    if not FALLBACK_SHEET_ID:
        FALLBACK_SHEET_ID = ""  # e.g. "1ezS_aex0jacESJiNVLnuUzwh-Y_3QpN2d70lbOp_DsI"

    # Prefer call-provided values; fallback to env/hardcoded
    sid = (sheet_id or "").strip() or FALLBACK_SHEET_ID
    raw_creds = sheets_creds_json or FALLBACK_SA_JSON

    # Parse creds: accept dict OR string; fix newline escapes
    sa_info = None
    if isinstance(raw_creds, dict):
        sa_info = raw_creds
    elif isinstance(raw_creds, str) and raw_creds.strip():
        txt = raw_creds.strip()
        # handle accidental double-encoding or surrounding quotes
        try:
            sa_info = json.loads(txt)
        except Exception:
            # Sometimes the JSON arrives quoted twice; try one more time
            try:
                sa_info = json.loads(json.loads(txt))
            except Exception as inner:
                return {"ok": False, "error": f"Invalid service account JSON (cannot parse): {inner}"}
    else:
        sa_info = None

    if not sid:
        return {"ok": False, "error": "Missing sheet_id (not in payload or env fallback)"}
    if not sa_info:
        return {"ok": False, "error": "Missing service account JSON (not in payload or env fallback)"}

    # Fix private_key newlines if needed
    try:
        if "private_key" in sa_info and "\\n" in sa_info["private_key"]:
            sa_info["private_key"] = sa_info["private_key"].replace("\\n", "\n")
    except Exception:
        pass

    # ------------------------------
    # 4) Authorize & open sheet
    # ------------------------------
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(sid)
    except Exception as e:
        return {"ok": False, "error": f"Google Sheets auth/open error: {e}"}

    # ------------------------------
    # 5) Read helper (supports custom header row)
    # ------------------------------
    def read_records(ws, header_row_idx):
        if header_row_idx == 1:
            # get_all_records() respects the first row as headers
            return ws.get_all_records()
        # Build a range from header_row_idx to sheet extents
        last_row = ws.row_count or 5000
        last_col = ws.col_count or 50
        rng = U.rowcol_to_a1(header_row_idx, 1) + ":" + U.rowcol_to_a1(last_row, last_col)
        values = ws.get(rng) or []
        if not values:
            return []
        headers = [str(h).strip() for h in (values[0] if values else [])]
        return [dict(zip(headers, row)) for row in values[1:] if any(str(x).strip() for x in row)]

    # ------------------------------
    # 6) Iterate requested tabs
    # ------------------------------
    warnings = []
    out_ids = {
        "applications_id": None,
        "promotions_id": None,
        "demographics_id": None,
        "socio_econ_id": None,
    }

    tab_list = [t.strip() for t in (tabs or "").split(",") if t.strip()]
    if not tab_list:
        return {"ok": False, "error": "No tabs requested (tabs was empty)"}

    for tab in tab_list:
        try:
            ws = sh.worksheet(tab)
            records = read_records(ws, hdr)[:lim]
        except Exception as e:
            warnings.append(f"Tab '{tab}' not found or read error: {e}")
            records = []

        # Normalize keys to snake_case-lite
        normed = []
        for r in records:
            nr = {}
            for k, v in r.items():
                key = str(k).strip().lower().replace(" ", "_")
                nr[key] = v
            normed.append(nr)

        df = pd.DataFrame(normed or [])

        # Stage as artifact
        try:
            from waveflow.artifacts import save_artifact
            table_id = save_artifact(df, f"{tab}_raw")
        except Exception:
            table_id = f"table:{tab}_raw"  # fallback symbolic id

        lk = tab.lower()
        if lk == "applications":
            out_ids["applications_id"] = table_id
        elif lk == "promotions":
            out_ids["promotions_id"] = table_id
        elif lk == "demographics":
            out_ids["demographics_id"] = table_id
        elif lk in ("socio_econ", "socioecon", "socioeconomic"):
            out_ids["socio_econ_id"] = table_id

    for k in list(out_ids.keys()):
        out_ids.setdefault(k, None)

    res = {"ok": True, **out_ids}
    if warnings:
        res["warnings"] = warnings
    return res