import argparse
import threading
import traceback
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, scrolledtext

from config import DB_PATH, EXPORT_DIR, APP_NAME, APP_VERSION
from db import (
    init_db,
    import_csv_to_db,
    export_db_to_csv,
    get_row_count,
    health_check,
)
from api import sync_from_api, sync_full_from_api


class App(tk.Tk):
    def __init__(self, import_file: str | None = None, export_file: str | None = None):
        super().__init__()

        self.title(f"{APP_NAME} v{APP_VERSION}")
        self.geometry("980x700")
        self.minsize(820, 560)

        init_db(DB_PATH)

        self.busy = False

        self.import_file_var = tk.StringVar(value=import_file or "")
        self.export_file_var = tk.StringVar(
            value=export_file or str(EXPORT_DIR / "opat_export.csv")
        )
        self.db_path_var = tk.StringVar(value=f"Database: {DB_PATH}")
        self.row_count_var = tk.StringVar(value="Rows in DB: loading...")
        self.health_var = tk.StringVar(value="Health: checking...")
        self.status_var = tk.StringVar(value="Ready.")

        self._build_ui()
        self.refresh_status()

        self.log(f"Application started: {APP_NAME} v{APP_VERSION}")
        self.log(f"Using database: {DB_PATH}")
        if self.import_file_var.get():
            self.log(f"Initial import file: {self.import_file_var.get()}")
        if self.export_file_var.get():
            self.log(f"Initial export file: {self.export_file_var.get()}")

    def _build_ui(self):
        outer = tk.Frame(self, padx=12, pady=12)
        outer.pack(fill="both", expand=True)

        header = tk.Label(
            outer,
            text=f"{APP_NAME} v{APP_VERSION}",
            font=("Segoe UI", 16, "bold"),
            anchor="w",
        )
        header.pack(fill="x", pady=(0, 8))

        info_frame = tk.Frame(outer)
        info_frame.pack(fill="x", pady=(0, 10))

        tk.Label(info_frame, textvariable=self.db_path_var, anchor="w", justify="left").pack(fill="x")
        tk.Label(info_frame, textvariable=self.row_count_var, anchor="w", justify="left").pack(fill="x")
        tk.Label(info_frame, textvariable=self.health_var, anchor="w", justify="left").pack(fill="x")

        paths_frame = tk.LabelFrame(outer, text="File Paths", padx=10, pady=10)
        paths_frame.pack(fill="x", pady=(0, 10))

        tk.Label(paths_frame, text="Import CSV:", anchor="w").grid(row=0, column=0, sticky="w")
        self.import_entry = tk.Entry(paths_frame, textvariable=self.import_file_var, width=90)
        self.import_entry.grid(row=0, column=1, padx=8, pady=4, sticky="ew")
        self.browse_import_btn = tk.Button(paths_frame, text="Browse...", command=self.choose_import_file, width=12)
        self.browse_import_btn.grid(row=0, column=2, padx=(0, 4), pady=4)

        tk.Label(paths_frame, text="Export CSV:", anchor="w").grid(row=1, column=0, sticky="w")
        self.export_entry = tk.Entry(paths_frame, textvariable=self.export_file_var, width=90)
        self.export_entry.grid(row=1, column=1, padx=8, pady=4, sticky="ew")
        self.browse_export_btn = tk.Button(paths_frame, text="Browse...", command=self.choose_export_file, width=12)
        self.browse_export_btn.grid(row=1, column=2, padx=(0, 4), pady=4)

        paths_frame.columnconfigure(1, weight=1)

        button_frame = tk.Frame(outer)
        button_frame.pack(fill="x", pady=(0, 10))

        self.import_btn = tk.Button(button_frame, text="Import CSV", width=16, command=self.on_import_csv)
        self.import_btn.grid(row=0, column=0, padx=(0, 8), pady=4, sticky="w")

        self.sync_btn = tk.Button(button_frame, text="Sync Recent", width=16, command=self.on_sync_recent)
        self.sync_btn.grid(row=0, column=1, padx=8, pady=4, sticky="w")

        self.sync_full_btn = tk.Button(button_frame, text="Sync Full", width=16, command=self.on_sync_full)
        self.sync_full_btn.grid(row=0, column=2, padx=8, pady=4, sticky="w")

        self.export_btn = tk.Button(button_frame, text="Export CSV", width=16, command=self.on_export_csv)
        self.export_btn.grid(row=0, column=3, padx=8, pady=4, sticky="w")

        self.refresh_btn = tk.Button(button_frame, text="Refresh Status", width=16, command=self.refresh_status)
        self.refresh_btn.grid(row=0, column=4, padx=8, pady=4, sticky="w")

        status_frame = tk.Frame(outer)
        status_frame.pack(fill="x", pady=(0, 8))

        tk.Label(status_frame, textvariable=self.status_var, anchor="w", fg="blue").pack(fill="x")

        log_label = tk.Label(outer, text="Log Output", anchor="w", font=("Segoe UI", 11, "bold"))
        log_label.pack(fill="x", pady=(6, 4))

        self.log_box = scrolledtext.ScrolledText(
            outer,
            wrap="word",
            height=24,
            font=("Consolas", 10),
            state="disabled",
        )
        self.log_box.pack(fill="both", expand=True)

    def set_busy(self, busy: bool, status_text: str = ""):
        self.busy = busy
        state = "disabled" if busy else "normal"

        self.import_btn.config(state=state)
        self.sync_btn.config(state=state)
        self.sync_full_btn.config(state=state)
        self.export_btn.config(state=state)
        self.refresh_btn.config(state=state)
        self.browse_import_btn.config(state=state)
        self.browse_export_btn.config(state=state)
        self.import_entry.config(state="disabled" if busy else "normal")
        self.export_entry.config(state="disabled" if busy else "normal")

        self.status_var.set(status_text or ("Working..." if busy else "Ready."))
        self.update_idletasks()

    def log(self, message: str):
        self.log_box.config(state="normal")
        self.log_box.insert("end", message.rstrip() + "\n")
        self.log_box.see("end")
        self.log_box.config(state="disabled")

    def run_background(self, label: str, func, on_success=None):
        if self.busy:
            return

        def worker():
            try:
                self.after(0, lambda: self.set_busy(True, f"{label}..."))
                self.after(0, lambda: self.log(f"{label} started."))
                result = func()
                self.after(0, lambda: self.log(f"{label} completed."))
                self.after(0, lambda: self.set_busy(False, "Ready."))
                self.after(0, self.refresh_status)
                if on_success:
                    self.after(0, lambda: on_success(result))
            except Exception as e:
                tb = traceback.format_exc()
                self.after(0, lambda: self.log(f"{label} failed: {e}"))
                self.after(0, lambda: self.log(tb))
                self.after(0, lambda: self.set_busy(False, "Ready."))
                self.after(0, lambda: messagebox.showerror("Error", str(e)))

        threading.Thread(target=worker, daemon=True).start()

    def refresh_status(self):
        try:
            init_db(DB_PATH)
            hc = health_check(DB_PATH)
            row_count = get_row_count(DB_PATH)

            self.row_count_var.set(f"Rows in DB: {row_count}")
            self.health_var.set(
                f"Health: arrests_table_exists={hc.get('arrests_table_exists')} | "
                f"sync_state_table_exists={hc.get('sync_state_table_exists', 'N/A')} | "
                f"row_count={hc.get('row_count')}"
            )
            self.status_var.set("Ready.")
        except Exception as e:
            self.status_var.set(f"Status error: {e}")
            self.log(f"Status refresh failed: {e}")

    def choose_import_file(self):
        file_path = filedialog.askopenfilename(
            title="Select CSV file to import",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if file_path:
            self.import_file_var.set(file_path)
            self.log(f"Selected import file: {file_path}")

    def choose_export_file(self):
        current_export = self.export_file_var.get().strip()
        initial_dir = str(EXPORT_DIR)
        initial_name = "opat_export.csv"

        if current_export:
            current_path = Path(current_export)
            if current_path.parent.exists():
                initial_dir = str(current_path.parent)
            if current_path.name:
                initial_name = current_path.name

        file_path = filedialog.asksaveasfilename(
            title="Save exported CSV",
            initialdir=initial_dir,
            initialfile=initial_name,
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if file_path:
            self.export_file_var.set(file_path)
            self.log(f"Selected export file: {file_path}")

    def on_import_csv(self):
        file_path = self.import_file_var.get().strip()
        if not file_path:
            messagebox.showwarning("Missing Import File", "Please select or enter a CSV file to import.")
            return

        if not Path(file_path).exists():
            messagebox.showerror("File Not Found", f"Import file does not exist:\n{file_path}")
            return

        def task():
            return import_csv_to_db(DB_PATH, file_path)

        def done(stats):
            self.log(f"Imported from: {file_path}")
            self.log(
                f"Processed {stats['rows_read']} CSV rows. "
                f"Inserted: {stats['inserted']}, "
                f"Updated: {stats['updated']}, "
                f"Unchanged: {stats['unchanged']}, "
                f"Skipped: {stats['skipped']}. "
                f"Final unique rows in DB: {stats['final_row_count']}."
            )
            messagebox.showinfo("Import Complete", "CSV import completed successfully.")

        self.run_background("CSV import", task, done)

    def on_sync_recent(self):
        def task():
            return sync_from_api(DB_PATH, months_back=6)

        def done(stats):
            self.log(
                f"Processed {stats['rows_processed']} API rows. "
                f"Inserted: {stats['inserted']}, "
                f"Updated: {stats['updated']}, "
                f"Unchanged: {stats['unchanged']}, "
                f"Skipped: {stats['skipped']}. "
                f"Final unique rows in DB: {stats['final_row_count']}."
            )
            messagebox.showinfo("Sync Complete", "Recent sync completed successfully.")

        self.run_background("Recent sync", task, done)

    def on_sync_full(self):
        confirm = messagebox.askyesno(
            "Confirm Full Sync",
            "Full sync may take longer and pull all available data.\n\nContinue?",
        )
        if not confirm:
            return

        def task():
            return sync_full_from_api(DB_PATH)

        def done(stats):
            self.log(
                f"Processed {stats['rows_processed']} API rows. "
                f"Inserted: {stats['inserted']}, "
                f"Updated: {stats['updated']}, "
                f"Unchanged: {stats['unchanged']}, "
                f"Skipped: {stats['skipped']}. "
                f"Final unique rows in DB: {stats['final_row_count']}."
            )
            messagebox.showinfo("Full Sync Complete", "Full sync completed successfully.")

        self.run_background("Full sync", task, done)

    def on_export_csv(self):
        file_path = self.export_file_var.get().strip()
        if not file_path:
            messagebox.showwarning("Missing Export File", "Please select or enter a CSV export path.")
            return

        export_path = Path(file_path)
        if export_path.suffix == "":
            export_path = export_path.with_suffix(".csv")
            self.export_file_var.set(str(export_path))

        def task():
            return export_db_to_csv(DB_PATH, export_path)

        def done(row_count):
            self.log(f"Exported {row_count} rows to: {export_path}")
            messagebox.showinfo("Export Complete", f"Exported {row_count} rows.\n\nFile:\n{export_path}")

        self.run_background("CSV export", task, done)


def parse_args():
    parser = argparse.ArgumentParser(description="OPAT Data Collector GUI")
    parser.add_argument("--import-file", help="Optional CSV file path to prefill import path")
    parser.add_argument("--export-file", help="Optional CSV file path to prefill export path")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    app = App(import_file=args.import_file, export_file=args.export_file)
    app.mainloop()