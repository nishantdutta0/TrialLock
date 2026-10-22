import sqlite3
import hashlib
import json
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent

def serialize_event(event):
    return json.dumps(event, sort_keys=True, default=str)

def compute_hash(data_str, prev_hash=""):
    combined = prev_hash + "|" + data_str
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()

def append_audit(event_type, details):
    init_db = __import__("triallock.storage", fromlist=["init_db"]).init_db
    init_db()
    conn = sqlite3.connect(str(BASE_DIR / "triallock.db"))
    c = conn.cursor()
    # Get previous hash
    c.execute("SELECT current_hash FROM audit_events ORDER BY event_id DESC LIMIT 1")
    row = c.fetchone()
    prev_hash = row[0] if row else ""
    serialized = serialize_event({"type": event_type, "details": details})
    curr_hash = compute_hash(serialized, prev_hash)
    c.execute("INSERT INTO audit_events (event_type, event_timestamp, details, previous_hash, current_hash) VALUES (?, ?, ?, ?, ?)",
              (event_type, __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(), serialized, prev_hash, curr_hash))
    conn.commit()
    conn.close()

def verify_audit_chain():
    init_db = __import__("triallock.storage", fromlist=["init_db"]).init_db
    init_db()
    conn = sqlite3.connect(str(BASE_DIR / "triallock.db"))
    c = conn.cursor()
    c.execute("SELECT event_id, event_type, details, previous_hash, current_hash FROM audit_events ORDER BY event_id ASC")
    rows = c.fetchall()
    conn.close()
    valid = True
    results = []
    prev = ""
    for row in rows:
        event_id, event_type, details_str, prev_hash, curr_hash = row
        serialized = details_str if isinstance(details_str, str) else ""
        expected = compute_hash(serialized, prev)
        match = (expected == curr_hash)
        if not match:
            valid = False
        results.append({"event_id": event_id, "type": event_type, "match": match, "prev_hash": prev_hash, "curr_hash": curr_hash})
        prev = curr_hash
    return valid, results
