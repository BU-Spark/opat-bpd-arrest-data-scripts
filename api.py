from __future__ import annotations
import re
import time
import requests
from datetime import datetime, timezone
from typing import Dict, Any, List, Callable, Optional

from db import upsert_records_with_stats

BASE_URL = (
    "https://services.arcgis.com/sFnw0xNflSi8J0uh/arcgis/rest/services/"
    "Boston_Arrests_Tbl_Pubview/FeatureServer/0/query"
)

PAGE_SIZE = 500
REQUEST_DELAY_SECONDS = 0.5
MAX_RETRIES = 5

OUT_FIELDS = ",".join([
    "ARREST_NUM",
    "INC_NUM",
    "CHARGE_SEQ_NUM",
    "CHARGE_CODE",
    "CHARGE_DESC",
    "NIBRS_CODE",
    "NIBRS_DESC",
    "ARR_DATE",
    "GENDER_DESC",
    "RACE_DESC",
    "ETHNICITY_DESC",
    "AGE",
    "JUVENILE",
    "HOUR_OF_DAY",
    "DAY_OF_WEEK",
    "YEAR",
    "QUARTER",
    "MONTH",
    "NEIGHBORHOOD",
    "DISTRICT",
])


def epoch_ms_to_iso(value: Any) -> str | None:
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

    pairs: List[tuple[int, int]] = []
    year = now.year
    month = now.month

    for _ in range(months_back):
        pairs.append((year, month))
        month -= 1
        if month == 0:
            month = 12
            year -= 1

    by_year: Dict[int, List[int]] = {}
    for y, m in pairs:
        by_year.setdefault(y, []).append(m)

    clauses = []
    for y, months in sorted(by_year.items()):
        month_list = ", ".join(str(m) for m in sorted(months))
        clauses.append(f"(YEAR = '{y}' AND MONTH IN ({month_list}))")

    return " OR ".join(clauses)


def empty_stats() -> Dict[str, int]:
    return {
        "rows_processed": 0,
        "inserted": 0,
        "updated": 0,
        "unchanged": 0,
        "skipped": 0,
        "final_row_count": 0,
    }


def merge_stats(total: Dict[str, int], chunk: Dict[str, int]) -> Dict[str, int]:
    total["rows_processed"] += chunk.get("rows_processed", 0)
    total["inserted"] += chunk.get("inserted", 0)
    total["updated"] += chunk.get("updated", 0)
    total["unchanged"] += chunk.get("unchanged", 0)
    total["skipped"] += chunk.get("skipped", 0)
    total["final_row_count"] = chunk.get("final_row_count", total["final_row_count"])
    return total


def _extract_retry_seconds(error_details: list[str]) -> int:
    if not error_details:
        return 60

    text = " ".join(error_details)
    match = re.search(r"Retry after (\d+)\s*sec", text, re.IGNORECASE)
    if match:
        return int(match.group(1))

    return 60


def fetch_page(where: str, offset: int, max_retries: int = MAX_RETRIES) -> Dict[str, Any]:
    params = {
        "where": where,
        "outFields": OUT_FIELDS,
        "f": "json",
        "resultOffset": offset,
        "resultRecordCount": PAGE_SIZE,
        "orderByFields": "ARR_DATE ASC, ARREST_NUM ASC, CHARGE_SEQ_NUM ASC",
    }

    for _ in range(max_retries):
        response = requests.get(BASE_URL, params=params, timeout=60)

        if response.status_code == 429:
            wait_seconds = 60
            time.sleep(wait_seconds)
            continue

        response.raise_for_status()
        data = response.json()

        if "error" in data:
            error = data["error"]
            code = error.get("code")
            details = error.get("details", [])

            if code == 429:
                wait_seconds = _extract_retry_seconds(details)
                time.sleep(wait_seconds)
                continue

            raise RuntimeError(f"ArcGIS API error: {error}")

        return data

    raise RuntimeError(
        f"ArcGIS API error: exceeded max retries after rate limiting at offset {offset}"
    )


def sync_pagewise(
    db_path: str,
    where: str,
    progress: bool = True,
    progress_callback: Optional[Callable[[str], None]] = None,
) -> Dict[str, int]:
    total_stats = empty_stats()
    offset = 0
    page_number = 0

    while True:
        data = fetch_page(where, offset)
        features = data.get("features", [])

        if not features:
            if progress_callback:
                progress_callback("No more records returned. Sync finishing...")
            break

        page_number += 1
        records = [feature_to_record(feature) for feature in features]
        page_stats = upsert_records_with_stats(db_path, records)
        total_stats = merge_stats(total_stats, page_stats)

        message = (
            f"Page {page_number} | "
            f"offset={offset} | "
            f"fetched={len(features)} | "
            f"processed={total_stats['rows_processed']} | "
            f"inserted={total_stats['inserted']} | "
            f"updated={total_stats['updated']} | "
            f"unchanged={total_stats['unchanged']} | "
            f"skipped={total_stats['skipped']} | "
            f"db_rows={total_stats['final_row_count']}"
        )

        if progress:
            print(message)
        if progress_callback:
            progress_callback(message)

        if len(features) < PAGE_SIZE:
            if progress_callback:
                progress_callback(
                    f"Last page reached. Returned {len(features)} rows, which is less than PAGE_SIZE={PAGE_SIZE}."
                )
            break

        offset += PAGE_SIZE
        time.sleep(REQUEST_DELAY_SECONDS)

    return total_stats


def sync_full_from_api(
    db_path: str,
    progress: bool = True,
    progress_callback: Optional[Callable[[str], None]] = None,
) -> Dict[str, int]:
    return sync_pagewise(
        db_path=db_path,
        where="1=1",
        progress=progress,
        progress_callback=progress_callback,
    )


def sync_from_api(
    db_path: str,
    months_back: int = 6,
    progress: bool = True,
    progress_callback: Optional[Callable[[str], None]] = None,
) -> Dict[str, int]:
    where = build_recent_where_clause(months_back)
    return sync_pagewise(
        db_path=db_path,
        where=where,
        progress=progress,
        progress_callback=progress_callback,
    )