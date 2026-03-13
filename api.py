import requests
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any
from db import upsert_records

BASE_URL = "https://services.arcgis.com/sFnw0xNflSi8J0uh/arcgis/rest/services/Boston_Arrests_Tbl_Pubview/FeatureServer/0/query"
PAGE_SIZE = 2000


def epoch_ms_to_iso(value):
    if value is None:
        return None
    try:
        dt = datetime.fromtimestamp(value / 1000, tz=timezone.utc)
        return dt.isoformat()
    except Exception:
        return None


def build_recent_where_clause(months_back: int = 6) -> str:
    """
    Build a recent-window where clause using YEAR + MONTH fields
    instead of ARR_DATE, which avoids ArcGIS date-query syntax issues.
    """
    if months_back <= 0:
        raise ValueError("months_back must be greater than 0")

    now = datetime.now(timezone.utc)
    pairs = []

    year = now.year
    month = now.month

    for _ in range(months_back):
        pairs.append((year, month))
        month -= 1
        if month == 0:
            month = 12
            year -= 1

    by_year: dict[int, list[int]] = {}
    for y, m in pairs:
        by_year.setdefault(y, []).append(m)

    clauses = []
    for y, months in sorted(by_year.items()):
        month_list = ", ".join(str(m) for m in sorted(months))
        clauses.append(f"(YEAR = '{y}' AND MONTH IN ({month_list}))")

    return " OR ".join(clauses)

def fetch_page(where: str, offset: int) -> Dict[str, Any]:
    params = {
        "where": where,
        "outFields": "*",
        "outSR": 4326,
        "f": "json",
        "resultOffset": offset,
        "resultRecordCount": PAGE_SIZE,
        "orderByFields": "ARR_DATE ASC, ARREST_NUM ASC, CHARGE_SEQ_NUM ASC",
    }

    response = requests.get(BASE_URL, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    if "error" in data:
        raise RuntimeError(f"ArcGIS API error: {data['error']}")

    return data


def fetch_all(where: str) -> List[Dict[str, Any]]:
    all_records: List[Dict[str, Any]] = []
    offset = 0

    while True:
        data = fetch_page(where, offset)
        features = data.get("features", [])

        if not features:
            break

        for feature in features:
            attrs = feature.get("attributes", {}).copy()

            if "ARR_DATE" in attrs:
                attrs["ARR_DATE"] = epoch_ms_to_iso(attrs["ARR_DATE"])

            all_records.append(attrs)

        print(f"Fetched {len(all_records)} records so far...")

        if len(features) < PAGE_SIZE:
            break

        offset += PAGE_SIZE

    return all_records


def sync_full_from_api(db_path: str) -> int:
    """
    Initial full sync: pull all available data.
    """
    records = fetch_all("1=1")
    if not records:
        return 0

    return upsert_records(db_path, records)


def sync_from_api(db_path: str, months_back: int = 6) -> int:
    """
    Incremental sync: pull recent records only.
    """
    where = build_recent_where_clause(months_back=months_back)
    records = fetch_all(where)
    if not records:
        return 0

    return upsert_records(db_path, records)