import argparse
from db import init_db, import_csv_to_db, export_db_to_csv
from api import sync_from_api, sync_full_from_api
from config import DB_PATH

def main(): 
    parser = argparse.ArgumentParser(description="Local data collector")
    subparsers = parser.add_subparsers(dest="command", required=True)

    import_parser = subparsers.add_parser("import", help="Import CSV into database")
    import_parser.add_argument("--file", required=True, help="Path to CSV file")

    export_parser = subparsers.add_parser("export", help="Export database to CSV")
    export_parser.add_argument("--file", required=True, help="Output CSV path")

    subparsers.add_parser("sync-full", help="Initial full sync from API")
    subparsers.add_parser("sync", help="Incremental sync from API")

    subparsers.add_parser("test", help="Run basic health checks")

    args = parser.parse_args()

    init_db(DB_PATH)

    if args.command == "import":
        stats= import_csv_to_db(DB_PATH, args.file)
        print(
                f"Processed {stats['rows_read']} CSV rows. "
                f"Inserted: {stats['inserted']}, "
                f"Updated: {stats['updated']}, "
                f"Unchanged: {stats['unchanged']}, "
                f"Skipped: {stats['skipped']}. "
                f"Final unique rows in DB: {stats['final_row_count']}."
            )

    elif args.command == "export":
        count = export_db_to_csv(DB_PATH, args.file)
        print(f"Exported {count} rows.")

    elif args.command == "sync-full":
        stats = sync_full_from_api(DB_PATH, progress=True)
        print(
                f"Processed {stats['rows_processed']} API rows. "
                f"Inserted: {stats['inserted']}, "
                f"Updated: {stats['updated']}, "
                f"Unchanged: {stats['unchanged']}, "
                f"Skipped: {stats['skipped']}. "
                f"Final unique rows in DB: {stats['final_row_count']}."
            )
        print(f"Full sync complete.")

    elif args.command == "sync":
        stats = sync_from_api(DB_PATH, progress=True)
        print(
            f"Processed {stats['rows_processed']} API rows. "
            f"Inserted: {stats['inserted']}, "
            f"Updated: {stats['updated']}, "
            f"Unchanged: {stats['unchanged']}, "
            f"Skipped: {stats['skipped']}. "
            f"Final unique rows in DB: {stats['final_row_count']}."
        )
        print(f"Synced {stats['rows_processed']} records from API.")

    # elif args.command == "test":
    #     from tests_runner import run_basic_tests
    #     run_basic_tests(DB_PATH)

if __name__ == "__main__":
    main()
