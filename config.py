import os

APP_NAME = "LocalDataCollector"
DATA_DIR = os.path.join(os.getenv("APPDATA") or ".", APP_NAME)
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "data.db")
API_URL = "https://api.example.com/records"