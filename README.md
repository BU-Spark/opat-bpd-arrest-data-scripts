# OPAT Data Collector

A lightweight local desktop + CLI tool for collecting, normalizing, and maintaining Boston Arrest data from the public ArcGIS endpoint.

This tool:

- Pulls arrest records from the Boston ArcGIS API
- Normalizes arrest numbers and keys
- Prevents duplicate records using a composite primary key
- Supports CSV import and export
- Maintains a persistent local SQLite database
- Works as both a CLI tool and a simple desktop GUI
- Can be packaged as a standalone Windows `.exe`

---

## Features

### API Sync
- Full sync: pulls all available records
- Recent sync: pulls last N months using YEAR + MONTH fields
- Safe upsert logic (no duplicate rows)
- Detailed sync statistics:
  - Rows processed
  - Inserted
  - Updated
  - Unchanged
  - Skipped
  - Final unique row count

### CSV Import
- Handles case-insensitive column names
- Normalizes arrest number formats:
  - `25-008332` → `20250008332`
- Upserts into DB safely
- Reports insert/update stats

### CSV Export
- Exports full database to CSV
- Preserves normalized structure
- Automatically appends `.csv` if missing
- Shows final file path

### Desktop GUI
- Import button
- Sync Recent button
- Sync Full button
- Export button
- Log output panel
- Shows database location
- Displays row count and health status

### CLI Support
- Works without GUI
- Supports file paths from terminal
- Can be packaged into a single `.exe`

---

## Project Structure

```
config.py   → paths and application configuration
db.py       → SQLite schema + upsert + import/export logic
api.py      → ArcGIS API sync logic
app.py      → CLI entry point
gui.py      → Desktop GUI entry point
```

---

## Database Location

The database is stored in:

```
%APPDATA%\OPATDataCollector\data.db
```

Exports default to:

```
%APPDATA%\OPATDataCollector\exports\
```

---

## Installation (Development)

1. Clone the repository

```
git clone https://github.com/yourusername/opat-data-collector.git
cd opat-data-collector
```

2. Create virtual environment (optional but recommended)

```
python -m venv venv
venv\Scripts\activate
```

3. Install requirements

```
pip install -r requirements.txt
```

---

## CLI Usage

### Full Sync

```
python app.py sync-full
```

### Recent Sync (last 6 months default)

```
python app.py sync
```

### Import CSV

```
python app.py import --file path\to\input.csv
```

### Export CSV

```
python app.py export --file path\to\output.csv
```

---

## GUI Usage

Launch:

```
python gui.py
```

Optional pre-filled file paths:

```
python gui.py --import-file input.csv --export-file output.csv
```

---

## Building Windows Executable

### CLI Version

```
pyinstaller --onefile --name OPATDataCollector app.py
```

### GUI Version

```
pyinstaller --onefile --windowed --name OPATDataCollector gui.py
```

Output will be in:

```
dist/OPATDataCollector.exe
```

You can distribute the single `.exe` file.

---

## Data Source

Boston Arrest data is retrieved from the public ArcGIS FeatureServer endpoint:

Boston_Arrests_Tbl_Pubview

No API key required.

---

## Normalization Logic

The database uses a composite primary key:

```
PRIMARY KEY (ARREST_NUM, CHARGE_SEQ_NUM)
```

Arrest numbers are normalized:

- `YY-NNNNNN` → `YYYYNNNNNNN`
- Non-digit characters stripped
- Left-padded to preserve format

This ensures:
- No duplicate logical records
- Clean consistent IDs across API + CSV

---

## Health Check

The system verifies:

- arrests table exists
- sync_state table exists
- total row count

Displayed in both CLI and GUI.

---

## Tech Stack

- Python 3.11+
- SQLite
- Tkinter (GUI)
- Requests (API calls)
- PyInstaller (packaging)

---

## Future Improvements

- Incremental sync using last ARR_DATE
- Progress bar in GUI
- Auto-update mechanism

---

## License

MIT License