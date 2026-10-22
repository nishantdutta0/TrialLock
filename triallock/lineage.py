import sqlite3
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent

def get_lineage(run_id):
    conn = sqlite3.connect(str(BASE_DIR / "triallock.db"))
    c = conn.cursor()
    c.execute("SELECT * FROM pipeline_runs WHERE run_id = ?", (run_id,))
    row = c.fetchone()
    c.execute("SELECT * FROM audit_events WHERE details LIKE ?", (f"%{run_id}%",))
    events = c.fetchall()
    conn.close()
    return {"run": row, "audit_events": events}
