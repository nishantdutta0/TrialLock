import pandas as pd
import sqlite3
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent

def export_issues_csv():
    conn = sqlite3.connect(str(BASE_DIR / "triallock.db"))
    # Placeholder issues table
    try:
        df = pd.read_sql_query("SELECT * FROM pipeline_runs", conn)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    out_path = BASE_DIR / "data" / "quarantine" / "issues_export.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(str(out_path), index=False)
    return str(out_path)
