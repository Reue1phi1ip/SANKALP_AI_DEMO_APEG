# Tools/GoogleSheetsFetch.py
# Reads one or more Google Sheet tabs and stages them as artifacts.
# Returns artifact IDs + tiny samples for quick verification.

from __future__ import annotations
import json
from typing import Any, Dict, List
from datetime import datetime
import re

import pandas as pd

# Optional dependency: gspread + Google Service Account creds
# (Your Waveflow environment likely has these; otherwise this tool will fail at auth.)
import gspread
from google.oauth2.service_account import Credentials


def _snake(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^0-9a-z]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def _map_scheme_val(v: Any) -> Any:
    """Normalize 'Scheme 1/2/3' and 's1/s2/s3' to canonical S1/S2/S3."""
    if v is None:
        return v
    x = str(v).strip().lower()
    if re.search(r"\bscheme\s*1\b|\bs1\b", x):
        return "S1"
    if re.search(r"\bscheme\s*2\b|\bs2\b", x):
        return "S2"
    if re.search(r"\bscheme\s*3\b|\bs3\b", x):
        return "S3"
    # keep uppercase for already clean ids like S1/S2/S3
    return str(v).strip().upper()


def _infer_geo_level(geo_code: str, existing: str | None) -> str | None:
    """If geo_level is missing, infer 'state' when a non-empty code is present; else 'national'."""
    if existing not in (None, ""):
        return str(existing).strip()
    if geo_code and str(geo_code).strip():
        return "state"
    return "national"


def _normalize_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Lower/underscore headers, apply aliases, and coerce basic types."""
    if not records:
        return records

    # Build header map from first row
    headers = list(records[0].keys())
    norm_headers = [_snake(h) for h in headers]

    # Canonical schema aliases expected by downstream analytics
    ALIASES = {
        # geo
        "state": "geo_code",
        "state_code": "geo_code",
        "region": "geo_code",
        "region_code": "geo_code",

        # counts / applications
        "applications": "apps_count",
        "application_count": "apps_count",
        "apps": "apps_count",
        "count": "apps_count",

        # promotions
        "promotion_intensity": "promo_intensity",
        "promo": "promo_intensity",

        # demographics
        "gender": "applicant_gender",
        "age": "applicant_age",
        "income": "income_bracket",

        # scheme + period variants
        "scheme": "scheme_id",
        "scheme_name": "scheme_id",
        "month": "date",
        "period": "date",
    }

    header_map = {}
    for raw, norm in zip(headers, norm_headers):
        header_map[raw] = ALIASES.get(norm, norm)

    out = []
    for row in records:
        r = {}
        for raw_k, v in row.items():
            k = header_map.get(raw_k, _snake(raw_k))
            r[k] = v
        out.append(r)

    # Coerce basic types commonly used by analytics
    for r in out:
        # date -> ISO (yyyy-mm-dd) if possible
        if "date" in r and r["date"] not in (None, ""):
            try:
                r["date"] = pd.to_datetime(r["date"], errors="coerce")
                if pd.isna(r["date"]):
                    r["date"] = None
                else:
                    r["date"] = r["date"].date().isoformat()
            except Exception:
                r["date"] = None

        # numeric coercions used across modules
        for num_col in ("apps_count", "promo_intensity", "applicant_age", "expected", "low", "high"):
            if num_col in r and r[num_col] not in (None, ""):
                try:
                    r[num_col] = float(str(r[num_col]).replace(",", ""))
                except Exception:
                    r[num_col] = None

        # Trim string dims (avoid duplicate buckets due to whitespace)
        for dim in ("scheme_id", "geo_code", "applicant_gender", "income_bracket", "occupation", "geo_level"):
            if dim in r and r[dim] is not None:
                r[dim] = str(r[dim]).strip()

        # Normalize scheme ids so downstream filters work uniformly
        if "scheme_id" in r:
            r["scheme_id"] = _map_scheme_val(r["scheme_id"])

        # Backfill geo_level if missing
        r["geo_level"] = _infer_geo_level(r.get("geo_code", ""), r.get("geo_level"))

    return out


def sheets_fetch_stage(sheet_id: str,
                       tabs: Any,
                       sheets_creds_json: Any,
                       header_row: str = "1",
                       limit: str = "250000",
                       *args, **kwargs) -> Dict[str, Any]:
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
        "warnings": [...],
        "samples": {tab: [rows...]},
        "row_counts": {tab: int}
      }
    """
    warnings: List[str] = []
    samples: Dict[str, List[Dict[str, Any]]] = {}
    row_counts: Dict[str, int] = {}

    # Parse inputs
    try:
        hdr = int(str(header_row).strip() or "1")
    except Exception:
        hdr = 1
        warnings.append("header_row not an int; defaulted to 1")

    try:
        lim = int(str(limit).strip() or "250000")
    except Exception:
        lim = 250000
        warnings.append("limit not an int; defaulted to 250000")

    if isinstance(tabs, str):
        tabs_list = [t.strip() for t in tabs.split(",") if t.strip()]
    elif isinstance(tabs, (list, tuple)):
        tabs_list = [str(t).strip() for t in tabs if str(t).strip()]
    else:
        return {"ok": False, "error": "tabs must be a comma-separated string or list"}

    out_ids: Dict[str, str] = {
        "applications_id": "",
        "promotions_id": "",
        "demographics_id": "",
        "socio_econ_id": "",
    }

    # --- auth
    try:
        sa_info = json.loads(sheets_creds_json) if isinstance(sheets_creds_json, str) else sheets_creds_json
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(sheet_id)
    except Exception as e:
        return {"ok": False, "error": f"Google Sheets auth/open error: {e}"}

    # Robust read
    def read_ws(ws) -> List[Dict[str, Any]]:
        try:
            # gspread supports head=<row> on get_all_records
            recs = ws.get_all_records(head=hdr, default_blank="")
        except Exception:
            # Fallback range read
            import gspread.utils as U
            last_row = ws.row_count or 5000
            last_col = ws.col_count or 50
            rng = U.rowcol_to_a1(hdr, 1) + ":" + U.rowcol_to_a1(last_row, last_col)
            values = ws.get(rng) or []
            headers = [str(h).strip() for h in (values[0] if values else [])]
            recs = [
                {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}
                for row in values[1:]
                if any(str(x).strip() for x in row)
            ]
        return recs

    # Optional artifact saver (works in Waveflow Studio). Fallback writes CSV.
    def stage_artifact(df: pd.DataFrame, name: str) -> str:
        try:
            from waveflow.artifacts import save_artifact
            return save_artifact(df, f"{name}_raw")
        except Exception:
            # Fallback to filesystem so we still return a stable ID
            path = f"/mnt/data/{_snake(name)}_raw.csv"
            df.to_csv(path, index=False)
            return f"table:{path}"

    # Iterate tabs
    for tab in tabs_list:
        try:
            ws = sh.worksheet(tab)
        except Exception as e:
            warnings.append(f"Tab '{tab}': open error ({e})")
            samples[tab] = []
            row_counts[tab] = 0
            continue

        try:
            recs = read_ws(ws)
            recs = _normalize_records(recs)[:lim]
        except Exception as e:
            warnings.append(f"Tab '{tab}': read error ({e})")
            samples[tab] = []
            row_counts[tab] = 0
            continue

        df = pd.DataFrame(recs)
        row_counts[tab] = int(len(df))
        samples[tab] = df.head(3).to_dict(orient="records")

        # Stage as artifact
        table_id = stage_artifact(df, tab)

        lk = tab.strip().lower()
        if lk == "applications":
            out_ids["applications_id"] = table_id
        elif lk == "promotions":
            out_ids["promotions_id"] = table_id
        elif lk == "demographics":
            out_ids["demographics_id"] = table_id
        elif lk in ("socio_econ", "socioecon", "socioeconomic", "socio_economic"):
            out_ids["socio_econ_id"] = table_id
        else:
            warnings.append(f"Tab '{tab}' read ok but not mapped to a known output key")

    res = {
        "ok": True,
        **out_ids,
        "warnings": warnings,
        "samples": samples,
        "row_counts": row_counts
    }
    return res