import sqlite3
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent

def init_db():
    db_path = BASE_DIR / "triallock.db"
    conn = sqlite3.connect(str(db_path))
    c = conn.cursor()
    tables = [
        "CREATE TABLE IF NOT EXISTS pipeline_runs (run_id TEXT PRIMARY KEY, study_id TEXT, start_time TEXT, end_time TEXT, duration_sec INTEGER, status TEXT, input_counts TEXT, silver_counts TEXT, gold_counts TEXT, quarantined_counts TEXT, quality_score REAL, readiness_status TEXT, error_message TEXT)",
        "CREATE INDEX IF NOT EXISTS idx_run_status ON pipeline_runs(status)",
        "CREATE TABLE IF NOT EXISTS audit_events (event_id INTEGER PRIMARY KEY AUTOINCREMENT, event_type TEXT, event_timestamp TEXT, details TEXT, previous_hash TEXT, current_hash TEXT)",
        "CREATE INDEX IF NOT EXISTS idx_audit_time ON audit_events(event_timestamp)",
        "CREATE TABLE IF NOT EXISTS gold_publication (run_id TEXT PRIMARY KEY, study_id TEXT, published_at TEXT, table_path TEXT, record_count INTEGER, quality_score REAL)",
        "CREATE INDEX IF NOT EXISTS idx_gold_run ON gold_publication(run_id)",
    ]
    for sql in tables:
        try:
            c.execute(sql)
        except Exception:
            pass
    conn.commit()
    conn.close()

def log_pipeline_run(run_id, study_id, start_time, status, details):
    init_db()
    conn = sqlite3.connect(BASE_DIR / "triallock.db")
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO pipeline_runs (run_id, study_id, start_time, status) VALUES (?, ?, ?, ?)",
              (run_id, study_id, start_time.isoformat() if hasattr(start_time, "isoformat") else start_time, status))
    conn.commit()
    conn.close()
