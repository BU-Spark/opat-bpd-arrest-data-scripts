"""
db.py

SQLite helpers for Boston Arrests database.

Features:
- Initialize schema
- Composite primary key (ARREST_NUM, CHARGE_SEQ_NUM)
- Upsert records (safe for API + CSV imports)
- CSV import/export
- Sync state storage
- Health checks
"""

from __future__ import annotations
import re
import csv
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, Optional
from datetime import datetime

TABLE_NAME = "arrests"
SYNC_STATE_TABLE = "sync_state"

EXPORT_COLUMNS = [
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
]

REQUIRED_KEY_COLUMNS = ["ARREST_NUM", "CHARGE_SEQ_NUM"]


# =========================
# Connection
# =========================

def get_conn(db_path: str | Path) -> sqlite3.Connection:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    return conn


# =========================
# Schema
# =========================

def init_db(db_path: str | Path) -> None:
    conn = get_conn(db_path)
    try:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                ARREST_NUM TEXT NOT NULL,
                INC_NUM TEXT,
                CHARGE_SEQ_NUM TEXT NOT NULL,
                CHARGE_CODE TEXT,
                CHARGE_DESC TEXT,
                NIBRS_CODE TEXT,
                NIBRS_DESC TEXT,
                ARR_DATE TEXT,
                GENDER_DESC TEXT,
                RACE_DESC TEXT,
                ETHNICITY_DESC TEXT,
                AGE INTEGER,
                JUVENILE TEXT,
                HOUR_OF_DAY INTEGER,
                DAY_OF_WEEK TEXT,
                YEAR TEXT,
                QUARTER INTEGER,
                MONTH INTEGER,
                NEIGHBORHOOD TEXT,
                DISTRICT TEXT,
                PRIMARY KEY (ARREST_NUM, CHARGE_SEQ_NUM)
            )
        """)

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {SYNC_STATE_TABLE} (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        conn.commit()
    finally:
        conn.close()


# =========================
# Normalization
# =========================

def _clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _clean_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None
    
def normalize_arrest_num(value: Any) -> Optional[str]:
    """
    Normalize ARREST_NUM into a canonical numeric format.

    Examples:
    - '25-008332'    -> '20250008332'
    - '20250008332'  -> '20250008332'
    - '24-00541-11'  -> '20240054111'

    Rules:
    - YY-NNNNNN       -> YYYY + sequence padded to 7 digits
    - YY-NNNNN-SS     -> YYYY + first part + suffix concatenated, padded to 7 digits total if needed
    - 11-digit numeric -> kept as-is
    """
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    text = text.replace(" ", "")

    # Case 1: already canonical 11-digit format
    if re.fullmatch(r"\d{11}", text):
        return text

    # Case 2: YY-NNNNNN
    m = re.fullmatch(r"(\d{2})-(\d+)", text)
    if m:
        yy, seq = m.groups()
        year = f"20{yy}"
        seq_padded = seq.zfill(7)
        return f"{year}{seq_padded}"

    # Case 3: YY-NNNNN-SS -> concatenate second and third parts
    m = re.fullmatch(r"(\d{2})-(\d+)-(\d+)", text)
    if m:
        yy, part1, part2 = m.groups()
        year = f"20{yy}"
        combined = f"{part1}{part2}"
        combined_padded = combined.zfill(7)
        return f"{year}{combined_padded}"

    # Fallback: strip non-digits
    digits = re.sub(r"\D", "", text)
    if len(digits) == 11:
        return digits

    return text

def normalize_race_desc(value: Any) -> str:
    """
    Canonical race categories:

    - AMERICAN INDIAN OR ALASKA NATIVE
    - ASIAN
    - BLACK OR AFRICAN AMERICAN
    - WHITE
    - UNKNOWN
    """
    if value is None:
        return "UNKNOWN"

    text = str(value).strip().upper()

    if not text:
        return "UNKNOWN"

    # Normalize spacing
    text = re.sub(r"\s+", " ", text)

    if "AMERICAN INDIAN" in text or "ALASKA" in text:
        return "AMERICAN INDIAN OR ALASKA NATIVE"

    if "ASIAN" in text:
        return "ASIAN"

    if "BLACK" in text or "AFRICAN AMERICAN" in text:
        return "BLACK OR AFRICAN AMERICAN"

    if "WHITE" in text:
        return "WHITE"

    return "UNKNOWN"


def normalize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    arr_date = normalize_arr_date(record.get("ARR_DATE"))

    year_value = _clean_text(record.get("YEAR"))
    if year_value is None and arr_date:
        try:
            year_value = str(datetime.fromisoformat(arr_date).year)
        except ValueError:
            pass

    return {
        "ARREST_NUM": normalize_arrest_num(record.get("ARREST_NUM")),
        "INC_NUM": _clean_text(record.get("INC_NUM")),
        "CHARGE_SEQ_NUM": _clean_text(record.get("CHARGE_SEQ_NUM")),
        "CHARGE_CODE": _clean_text(record.get("CHARGE_CODE")),
        "CHARGE_DESC": _clean_text(record.get("CHARGE_DESC")),
        "NIBRS_CODE": _clean_text(record.get("NIBRS_CODE")),
        "NIBRS_DESC": _clean_text(record.get("NIBRS_DESC")),
        "ARR_DATE": arr_date,
        "GENDER_DESC": _clean_text(record.get("GENDER_DESC")),
        "RACE_DESC": normalize_race_desc(record.get("RACE_DESC")),
        "ETHNICITY_DESC": normalize_ethnicity_desc(record.get("ETHNICITY_DESC")),
        "AGE": _clean_int(record.get("AGE")),
        "JUVENILE": _clean_text(record.get("JUVENILE")),
        "HOUR_OF_DAY": _clean_int(record.get("HOUR_OF_DAY")),
        "DAY_OF_WEEK": _clean_text(record.get("DAY_OF_WEEK")),
        "YEAR": year_value,
        "QUARTER": _clean_int(record.get("QUARTER")),
        "MONTH": _clean_int(record.get("MONTH")),
        "NEIGHBORHOOD": _clean_text(record.get("NEIGHBORHOOD")),
        "DISTRICT": _clean_text(record.get("DISTRICT")),
    }


def validate_keys(record: Dict[str, Any]) -> None:
    missing = [k for k in REQUIRED_KEY_COLUMNS if not record.get(k)]
    if missing:
        raise ValueError(f"Missing required key fields: {missing}")


# =========================
# Sync State
# =========================

def get_sync_value(db_path: str | Path, key: str, default=None):
    conn = get_conn(db_path)
    try:
        row = conn.execute(
            f"SELECT value FROM {SYNC_STATE_TABLE} WHERE key = ?",
            (key,)
        ).fetchone()
        return row["value"] if row else default
    finally:
        conn.close()


def set_sync_value(db_path: str | Path, key: str, value: str):
    conn = get_conn(db_path)
    try:
        conn.execute(f"""
            INSERT INTO {SYNC_STATE_TABLE} (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """, (key, value))
        conn.commit()
    finally:
        conn.close()


# =========================
# Upsert Logic
# =========================

def upsert_record(conn: sqlite3.Connection, record: Dict[str, Any]):
    record = normalize_record(record)
    validate_keys(record)

    conn.execute(f"""
        INSERT INTO {TABLE_NAME} (
            ARREST_NUM, INC_NUM, CHARGE_SEQ_NUM,
            CHARGE_CODE, CHARGE_DESC,
            NIBRS_CODE, NIBRS_DESC,
            ARR_DATE, GENDER_DESC, RACE_DESC,
            ETHNICITY_DESC, AGE, JUVENILE,
            HOUR_OF_DAY, DAY_OF_WEEK,
            YEAR, QUARTER, MONTH,
            NEIGHBORHOOD, DISTRICT
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ARREST_NUM, CHARGE_SEQ_NUM) DO UPDATE SET
            INC_NUM = excluded.INC_NUM,
            CHARGE_CODE = excluded.CHARGE_CODE,
            CHARGE_DESC = excluded.CHARGE_DESC,
            NIBRS_CODE = excluded.NIBRS_CODE,
            NIBRS_DESC = excluded.NIBRS_DESC,
            ARR_DATE = excluded.ARR_DATE,
            GENDER_DESC = excluded.GENDER_DESC,
            RACE_DESC = excluded.RACE_DESC,
            ETHNICITY_DESC = excluded.ETHNICITY_DESC,
            AGE = excluded.AGE,
            JUVENILE = excluded.JUVENILE,
            HOUR_OF_DAY = excluded.HOUR_OF_DAY,
            DAY_OF_WEEK = excluded.DAY_OF_WEEK,
            YEAR = excluded.YEAR,
            QUARTER = excluded.QUARTER,
            MONTH = excluded.MONTH,
            NEIGHBORHOOD = excluded.NEIGHBORHOOD,
            DISTRICT = excluded.DISTRICT
    """, (
        record["ARREST_NUM"],
        record["INC_NUM"],
        record["CHARGE_SEQ_NUM"],
        record["CHARGE_CODE"],
        record["CHARGE_DESC"],
        record["NIBRS_CODE"],
        record["NIBRS_DESC"],
        record["ARR_DATE"],
        record["GENDER_DESC"],
        record["RACE_DESC"],
        record["ETHNICITY_DESC"],
        record["AGE"],
        record["JUVENILE"],
        record["HOUR_OF_DAY"],
        record["DAY_OF_WEEK"],
        record["YEAR"],
        record["QUARTER"],
        record["MONTH"],
        record["NEIGHBORHOOD"],
        record["DISTRICT"],
    ))


def upsert_records(db_path: str | Path, records: Iterable[Dict[str, Any]]) -> int:
    conn = get_conn(db_path)
    count = 0
    try:
        for record in records:
            upsert_record(conn, record)
            count += 1
        conn.commit()
        return count
    finally:
        conn.close()

def upsert_records_with_stats(
    db_path: str | Path,
    records: Iterable[Dict[str, Any]],
) -> Dict[str, int]:
    conn = get_conn(db_path)

    stats = {
        "rows_processed": 0,
        "inserted": 0,
        "updated": 0,
        "unchanged": 0,
        "skipped": 0,
        "final_row_count": 0,
    }

    try:
        for record in records:
            stats["rows_processed"] += 1

            try:
                normalized_record = normalize_record(record)
                validate_keys(normalized_record)
            except Exception:
                stats["skipped"] += 1
                continue

            existing = get_existing_record(
                conn,
                normalized_record["ARREST_NUM"],
                normalized_record["CHARGE_SEQ_NUM"],
            )

            if existing is None:
                upsert_record(conn, record)
                stats["inserted"] += 1
            else:
                if records_equal(existing, normalized_record):
                    stats["unchanged"] += 1
                else:
                    upsert_record(conn, record)
                    stats["updated"] += 1

        conn.commit()

        stats["final_row_count"] = conn.execute(
            f"SELECT COUNT(*) AS count FROM {TABLE_NAME}"
        ).fetchone()["count"]

        return stats

    finally:
        conn.close()

# =========================
# CSV Import / Export
# =========================
COLUMN_ALIASES = {
    "ARREST_NUM": "ARREST_NUM",
    "ARRESTNUMBER": "ARREST_NUM",

    "INC_NUM": "INC_NUM",

    "CHARGE_SEQ_NUM": "CHARGE_SEQ_NUM",
    "CHARGE_SEQUENCE_NUM": "CHARGE_SEQ_NUM",

    "CHARGE_CODE": "CHARGE_CODE",
    "CHARGE_CODE_NAME": "CHARGE_DESC",
    "CHARGE_DESC": "CHARGE_DESC",

    "NIBRS_CODE": "NIBRS_CODE",
    "NIBRS_DESC": "NIBRS_DESC",

    "ARREST_DATE": "ARR_DATE",
    "ARR_DATE": "ARR_DATE",

    "GENDER": "GENDER_DESC",
    "GENDER_DESC": "GENDER_DESC",

    "RACE": "RACE_DESC",
    "RACE_DESC": "RACE_DESC",

    "ETHNICITY_DESC": "ETHNICITY_DESC",

    "AGE": "AGE",
    "JUVENILE": "JUVENILE",
    "HOUR_OF_DAY": "HOUR_OF_DAY",
    "DAY_OF_WEEK": "DAY_OF_WEEK",
    "YEAR": "YEAR",
    "QUARTER": "QUARTER",
    "MONTH": "MONTH",
    "NEIGHBORHOOD": "NEIGHBORHOOD",
    "DISTRICT": "DISTRICT",
}

def normalize_ethnicity_desc(value: Any) -> Optional[str]:
    """
    Normalize ETHNICITY_DESC into one canonical set:
    - HISPANIC OR LATINX
    - NOT HISPANIC OR LATINX
    - UNKNOWN
    """
    if value is None:
        return "UNKNOWN"

    text = str(value).strip()
    if not text:
        return "UNKNOWN"

    normalized = text.upper().strip()

    # collapse repeated whitespace
    normalized = re.sub(r"\s+", " ", normalized)

    if normalized in {
        "HISPANIC OR LATINX",
        "HISPANIC ORIGIN",
        "HISPANIC",
        "LATINX",
        "LATINO",
        "LATINA",
    }:
        return "HISPANIC OR LATINX"

    if normalized in {
        "NOT HISPANIC OR LATINX",
        "NOT OF HISPANIC ORIGIN",
        "NON HISPANIC",
        "NON-HISPANIC",
        "NOT HISPANIC",
    }:
        return "NOT HISPANIC OR LATINX"
    
    return "UNKNOWN"

def normalize_column_name(name: str) -> str:
    cleaned = name.strip().upper()
    cleaned = cleaned.replace(" ", "_").replace("-", "_")
    return COLUMN_ALIASES.get(cleaned, cleaned)

def normalize_arr_date(value: Any) -> Optional[str]:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    # Handle CSV format like 11/1/2025 5:00
    for fmt in ("%m/%d/%Y %H:%M", "%m/%d/%Y %H:%M:%S"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.isoformat()
        except ValueError:
            pass

    return text

def normalize_csv_row(row: Dict[str, Any]) -> Dict[str, Any]:
    normalized = {}
    for key, value in row.items():
        if key is None:
            continue
        normalized_key = normalize_column_name(key)
        normalized[normalized_key] = value
    return normalized

def import_csv_to_db(db_path: str | Path, csv_path: str | Path) -> Dict[str, int]:
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    conn = get_conn(db_path)

    stats = {
        "rows_read": 0,
        "inserted": 0,
        "updated": 0,
        "unchanged": 0,
        "skipped": 0,
        "final_row_count": 0,
    }

    try:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)

            if not reader.fieldnames:
                raise ValueError("CSV file is missing a header row.")

            normalized_headers = [normalize_column_name(h) for h in reader.fieldnames if h]
            missing = [c for c in REQUIRED_KEY_COLUMNS if c not in normalized_headers]
            if missing:
                raise ValueError(f"CSV missing required columns after normalization: {missing}")

            for row in reader:
                stats["rows_read"] += 1

                try:
                    normalized_row = normalize_csv_row(row)
                    normalized_record = normalize_record(normalized_row)
                    validate_keys(normalized_record)
                except Exception:
                    stats["skipped"] += 1
                    continue

                existing = get_existing_record(
                    conn,
                    normalized_record["ARREST_NUM"],
                    normalized_record["CHARGE_SEQ_NUM"],
                )

                if existing is None:
                    upsert_record(conn, normalized_row)
                    stats["inserted"] += 1
                else:
                    if records_equal(existing, normalized_record):
                        stats["unchanged"] += 1
                    else:
                        upsert_record(conn, normalized_row)
                        stats["updated"] += 1

        conn.commit()

        stats["final_row_count"] = conn.execute(
            f"SELECT COUNT(*) AS count FROM {TABLE_NAME}"
        ).fetchone()["count"]

        return stats

    finally:
        conn.close()


def export_db_to_csv(db_path: str | Path, csv_path: str | Path) -> int:
    csv_path = Path(csv_path)

    if csv_path.suffix == "":
        csv_path = csv_path.with_suffix(".csv")

    if csv_path.exists() and csv_path.is_dir():
        raise IsADirectoryError(
            f"Export path is a directory, not a file: {csv_path}"
        )

    conn = get_conn(db_path)
    try:
        rows = conn.execute(
            f"""
            SELECT
                ARREST_NUM,
                INC_NUM,
                CHARGE_SEQ_NUM,
                CHARGE_CODE,
                CHARGE_DESC,
                NIBRS_CODE,
                NIBRS_DESC,
                ARR_DATE,
                GENDER_DESC,
                RACE_DESC,
                ETHNICITY_DESC,
                AGE,
                JUVENILE,
                HOUR_OF_DAY,
                DAY_OF_WEEK,
                YEAR,
                QUARTER,
                MONTH,
                NEIGHBORHOOD,
                DISTRICT
            FROM {TABLE_NAME}
            ORDER BY ARR_DATE, ARREST_NUM, CHARGE_SEQ_NUM
            """
        ).fetchall()
    finally:
        conn.close()

    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=EXPORT_COLUMNS)
        writer.writeheader()

        for row in rows:
            row_dict = dict(row)
            writer.writerow({
                col: row_dict.get(col)
                for col in EXPORT_COLUMNS
            })

    return len(rows)


# =========================
# Utilities
# =========================
def count_duplicate_csv_keys(csv_path: str | Path):
    csv_path = Path(csv_path)
    seen = {}
    duplicates = []

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        for row_num, row in enumerate(reader, start=2):
            normalized_row = normalize_csv_row(row)
            arrest_num = normalize_arrest_num(normalized_row.get("ARREST_NUM"))
            charge_seq_num = str(normalized_row.get("CHARGE_SEQ_NUM")).strip() if normalized_row.get("CHARGE_SEQ_NUM") else None

            key = (arrest_num, charge_seq_num)

            if key in seen:
                duplicates.append({
                    "key": key,
                    "first_row": seen[key],
                    "duplicate_row": row_num,
                })
            else:
                seen[key] = row_num

    return duplicates

def get_row_count(db_path: str | Path) -> int:
    conn = get_conn(db_path)
    row = conn.execute(
        f"SELECT COUNT(*) AS count FROM {TABLE_NAME}"
    ).fetchone()
    conn.close()
    return row["count"]

def record_exists(conn: sqlite3.Connection, arrest_num: str, charge_seq_num: str) -> bool:
    row = conn.execute(
        f"""
        SELECT 1
        FROM {TABLE_NAME}
        WHERE ARREST_NUM = ? AND CHARGE_SEQ_NUM = ?
        """,
        (arrest_num, charge_seq_num),
    ).fetchone()
    return row is not None


def get_existing_record(
    conn: sqlite3.Connection,
    arrest_num: str,
    charge_seq_num: str,
) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        f"""
        SELECT
            ARREST_NUM,
            INC_NUM,
            CHARGE_SEQ_NUM,
            CHARGE_CODE,
            CHARGE_DESC,
            NIBRS_CODE,
            NIBRS_DESC,
            ARR_DATE,
            GENDER_DESC,
            RACE_DESC,
            ETHNICITY_DESC,
            AGE,
            JUVENILE,
            HOUR_OF_DAY,
            DAY_OF_WEEK,
            YEAR,
            QUARTER,
            MONTH,
            NEIGHBORHOOD,
            DISTRICT
        FROM {TABLE_NAME}
        WHERE ARREST_NUM = ? AND CHARGE_SEQ_NUM = ?
        """,
        (arrest_num, charge_seq_num),
    ).fetchone()

    return dict(row) if row else None


def records_equal(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    compare_cols = [
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
    ]
    return all(a.get(col) == b.get(col) for col in compare_cols)


def health_check(db_path: str | Path):
    conn = get_conn(db_path)
    arrests_exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (TABLE_NAME,)
    ).fetchone() is not None

    row_count = 0
    if arrests_exists:
        row_count = conn.execute(
            f"SELECT COUNT(*) AS count FROM {TABLE_NAME}"
        ).fetchone()["count"]

    conn.close()

    return {
        "arrests_table_exists": arrests_exists,
        "row_count": row_count,
    }