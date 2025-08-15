# Tools/GoogleSheetsFetch.py
def sheets_fetch_stage(sheet_id, tabs, sheets_creds_json, header_row="1", limit="250000", *args, **kwargs):
    """
    Reads one or more tabs from a Google Sheet and stages them as artifacts.
    Returns artifact IDs so downstream tools can load by ID.

    Returns:
      {
        "ok": True,
        "applications_id": "table:...",
        "promotions_id":   "table:...",
        "demographics_id": "table:...",
        "socio_econ_id":   "table:...",
        "warnings": [...],            # human-readable notes
        "samples": {tab: [rows...]},  # tiny sample to verify we actually read data
        "row_counts": {tab: n}        # exact counts read per tab
      }
    """
    import json
    import pandas as pd

    warnings = []
    samples  = {}
    row_counts = {}

    # --- deps
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except Exception as e:
        return {"ok": False, "error": f"Missing gspread/google-auth deps: {e}"}

    # --- parse params
    try:
        hdr = int(str(header_row).strip() or "1")
    except Exception:
        hdr = 1
        warnings.append("header_row not int, defaulted to 1")
    try:
        lim = int(str(limit).strip() or "250000")
    except Exception:
        lim = 250000
        warnings.append("limit not int, defaulted to 250000")

    # --- auth
    try:
        sa_info = json.loads(sheets_creds_json) if isinstance(sheets_creds_json, str) else sheets_creds_json
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(sheet_id)
    except Exception as e:
        return {"ok": False, "error": f"Google Sheets auth/open error: {e}"}

    # Helper: robust read
    def read_ws(ws):
        # gspread supports head=<row> on get_all_records
        try:
            records = ws.get_all_records(head=hdr, default_blank="")  # list of dicts
        except Exception:
            # fallback range read
            import gspread.utils as U
            last_row = ws.row_count or 5000
            last_col = ws.col_count or 50
            rng = U.rowcol_to_a1(hdr, 1) + ":" + U.rowcol_to_a1(last_row, last_col)
            values = ws.get(rng) or []
            headers = [str(h).strip() for h in (values[0] if values else [])]
            records = [
                {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}
                for row in values[1:]
                if any(str(x).strip() for x in row)
            ]
        return records

    # Normalize keys: lowercase + underscores
    def normalize_records(records):
        out = []
        for r in records:
            nr = {}
            for k, v in (r or {}).items():
                key = str(k).strip().lower().replace(" ", "_")
                nr[key] = v
            out.append(nr)
        return out

    # Iterate tabs
    out_ids = {"applications_id": None, "promotions_id": None, "demographics_id": None, "socio_econ_id": None}
    tab_list = [t.strip() for t in (tabs or "").split(",") if t.strip()]

    for tab in tab_list:
        try:
            ws = sh.worksheet(tab)
        except Exception as e:
            warnings.append(f"Tab '{tab}': not found ({e})")
            samples[tab] = []
            row_counts[tab] = 0
            continue

        try:
            recs = read_ws(ws)
            recs = normalize_records(recs)[:lim]
        except Exception as e:
            warnings.append(f"Tab '{tab}': read error ({e})")
            samples[tab] = []
            row_counts[tab] = 0
            continue

        df = pd.DataFrame(recs)
        row_counts[tab] = int(len(df))
        samples[tab] = df.head(3).to_dict(orient="records")

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
        elif lk in ["socio_econ", "socioecon", "socioeconomic"]:
            out_ids["socio_econ_id"] = table_id
        else:
            warnings.append(f"Tab '{tab}' read ok but not mapped to a known output key")

    res = {"ok": True, **out_ids, "warnings": warnings, "samples": samples, "row_counts": row_counts}
    return res