import sqlite3
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent

def get_latest_metrics():
    conn = sqlite3.connect(str(BASE_DIR / "triallock.db"))
    c = conn.cursor()
    c.execute("SELECT * FROM pipeline_runs ORDER BY start_time DESC LIMIT 1")
    row = c.fetchone()
    conn.close()
    if row:
        return {
            "run_id": row[0],
            "study_id": row[1],
            "start_time": row[2],
            "end_time": row[3],
            "duration_sec": row[4],
            "status": row[5],
            "quality_score": row[9] if row[9] is not None else None,
            "readiness_status": row[10],
        }
    return None
