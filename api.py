import requests
from datetime import datetime, timezone
from typing import Dict, List, Any

from db import upsert_records_with_stats

BASE_URL = (
    "https://services.arcgis.com/sFnw0xNflSi8J0uh/arcgis/rest/services/"
    "Boston_Arrests_Tbl_Pubview/FeatureServer/0/query"
)

PAGE_SIZE = 2000


# =========================
# Helpers
# =========================

def epoch_ms_to_iso(value):
    if value is None:
        return None
    try:
        dt = datetime.fromtimestamp(value / 1000, tz=timezone.utc)
        return dt.isoformat()
    except Exception:
        return None


def feature_to_record(feature: Dict[str, Any]) -> Dict[str, Any]:
    attrs = dict(feature.get("attributes") or {})

    if "ARR_DATE" in attrs:
        attrs["ARR_DATE"] = epoch_ms_to_iso(attrs.get("ARR_DATE"))

    return attrs


def build_recent_where_clause(months_back: int = 6) -> str:
    if months_back <= 0:
        raise ValueError("months_back must be > 0")

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

    by_year = {}
    for y, m in pairs:
        by_year.setdefault(y, []).append(m)

    clauses = []
    for y, months in sorted(by_year.items()):
        month_list = ", ".join(str(m) for m in sorted(months))
        clauses.append(f"(YEAR = '{y}' AND MONTH IN ({month_list}))")

    return " OR ".join(clauses)


# =========================
# API Fetching
# =========================

def fetch_page(where: str, offset: int) -> Dict[str, Any]:
    params = {
        "where": where,
        "outFields": "*",
        "f": "json",
        "resultOffset": offset,
        "resultRecordCount": PAGE_SIZE,
    }

    response = requests.get(BASE_URL, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    if "error" in data:
        raise RuntimeError(f"ArcGIS API error: {data['error']}")

    return data


def fetch_all(where: str, progress: bool = True) -> List[Dict[str, Any]]:
    records = []
    offset = 0

    while True:
        data = fetch_page(where, offset)
        features = data.get("features", [])

        if not features:
            break

        for feature in features:
            records.append(feature_to_record(feature))

        if progress:
            print(f"Fetched {len(records)} total records...")

        if len(features) < PAGE_SIZE:
            break

        offset += PAGE_SIZE

    return records


# =========================
# Sync Functions
# =========================

def empty_stats():
    return {
        "rows_processed": 0,
        "inserted": 0,
        "updated": 0,
        "unchanged": 0,
        "skipped": 0,
        "final_row_count": 0,
    }


def sync_full_from_api(db_path: str):
    records = fetch_all(where="1=1")

    if not records:
        return empty_stats()

    return upsert_records_with_stats(db_path, records)


def sync_from_api(db_path: str, months_back: int = 6):
    where = build_recent_where_clause(months_back)
    records = fetch_all(where)

    if not records:
        return empty_stats()

    return upsert_records_with_stats(db_path, records)