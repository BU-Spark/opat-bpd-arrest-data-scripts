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
    Normalize ARREST_NUM into a canonical format.

    Expected examples:
    - '25-008332'   -> '20250008332'
    - '20250008332' -> '20250008332'

    Assumption:
    - Short format is YY-NNNNNN
    - Canonical format is YYYYNNNNNNN where YYYY = 20 + YY
    """
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    # Remove whitespace
    text = text.replace(" ", "")

    # Case 1: already full numeric format like 20250008332
    if re.fullmatch(r"\d{11}", text):
        return text

    # Case 2: short dashed format like 25-008332
    m = re.fullmatch(r"(\d{2})-(\d+)", text)
    if m:
        yy, seq = m.groups()
        year = f"20{yy}"
        seq_padded = seq.zfill(7) 
        return f"{year}{seq_padded}"

    # Fallback: strip non-digits, return if it looks usable
    digits = re.sub(r"\D", "", text)
    if len(digits) == 11:
        return digits

    # If it doesn't match known formats, keep cleaned original
    return text


def normalize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "ARREST_NUM": normalize_arrest_num(record.get("ARREST_NUM")),
        "INC_NUM": _clean_text(record.get("INC_NUM")),
        "CHARGE_SEQ_NUM": _clean_text(record.get("CHARGE_SEQ_NUM")),
        "CHARGE_CODE": _clean_text(record.get("CHARGE_CODE")),
        "CHARGE_DESC": _clean_text(record.get("CHARGE_DESC")),
        "NIBRS_CODE": _clean_text(record.get("NIBRS_CODE")),
        "NIBRS_DESC": _clean_text(record.get("NIBRS_DESC")),
        "ARR_DATE": _clean_text(record.get("ARR_DATE")),
        "GENDER_DESC": _clean_text(record.get("GENDER_DESC")),
        "RACE_DESC": _clean_text(record.get("RACE_DESC")),
        "ETHNICITY_DESC": _clean_text(record.get("ETHNICITY_DESC")),
        "AGE": _clean_int(record.get("AGE")),
        "JUVENILE": _clean_text(record.get("JUVENILE")),
        "HOUR_OF_DAY": _clean_int(record.get("HOUR_OF_DAY")),
        "DAY_OF_WEEK": _clean_text(record.get("DAY_OF_WEEK")),
        "YEAR": _clean_text(record.get("YEAR")),
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


# =========================
# CSV Import / Export
# =========================

def import_csv_to_db(db_path: str | Path, csv_path: str | Path) -> int:
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    conn = get_conn(db_path)
    count = 0

    try:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)

            missing = [c for c in REQUIRED_KEY_COLUMNS if c not in reader.fieldnames]
            if missing:
                raise ValueError(f"CSV missing required columns: {missing}")

            for row in reader:
                upsert_record(conn, row)
                count += 1

        conn.commit()
        return count
    finally:
        conn.close()


def export_db_to_csv(db_path: str | Path, csv_path: str | Path) -> int:
    conn = get_conn(db_path)
    rows = conn.execute(
        f"SELECT * FROM {TABLE_NAME} ORDER BY ARR_DATE"
    ).fetchall()
    conn.close()

    csv_path = Path(csv_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=EXPORT_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({col: row[col] for col in EXPORT_COLUMNS})

    return len(rows)


# =========================
# Utilities
# =========================

def get_row_count(db_path: str | Path) -> int:
    conn = get_conn(db_path)
    row = conn.execute(
        f"SELECT COUNT(*) AS count FROM {TABLE_NAME}"
    ).fetchone()
    conn.close()
    return row["count"]


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