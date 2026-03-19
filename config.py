import os
from pathlib import Path

APP_NAME = "OPATDataCollector"

DATA_DIR = Path(os.getenv("APPDATA") or ".") / APP_NAME
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "data.db"

EXPORT_DIR = DATA_DIR / "exports"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

APP_VERSION = "1.1.0"