"""
Database and storage layer for TrialLock.
Manages SQLite metadata, audit trails, and run history.
"""

import sqlite3
import json
from pathlib import Path
from typing import Dict, List, Any, Optional

BASE_DIR = Path(__file__).resolve().parent.parent


def get_db_path() -> Path:
    """Get the path to the SQLite database."""
    return BASE_DIR / "triallock.db"


def init_db():
    """Initialize database tables with complete schema for TrialLock."""
    conn = sqlite3.connect(str(get_db_path()))
    c = conn.cursor()

    # 1. Pipeline Runs table
    # First check if the table exists
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pipeline_runs'")
    table_exists = c.fetchone() is not None

    if table_exists:
        # Check if new columns exist and add them if missing
        c.execute("PRAGMA table_info(pipeline_runs)")
        columns = {row[1] for row in c.fetchall()}

        new_columns = {
            'total_checks': 'INTEGER',
            'passed_checks': 'INTEGER',
            'failed_checks': 'INTEGER',
            'readiness_reason': 'TEXT',
        }

        for col_name, col_type in new_columns.items():
            if col_name not in columns:
                try:
                    c.execute(f"ALTER TABLE pipeline_runs ADD COLUMN {col_name} {col_type}")
                except Exception:
                    pass  # Column might have been added by concurrent process
    else:
        # Create table from scratch
        c.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id TEXT PRIMARY KEY,
            study_id TEXT,
            start_time TEXT,
            end_time TEXT,
            duration_sec INTEGER,
            status TEXT,
            input_counts TEXT,
            silver_counts TEXT,
            gold_counts TEXT,
            quarantined_counts TEXT,
            quality_score REAL,
            total_checks INTEGER,
            passed_checks INTEGER,
            failed_checks INTEGER,
            readiness_status TEXT,
            readiness_reason TEXT,
            error_message TEXT
        )
        """)

    c.execute("CREATE INDEX IF NOT EXISTS idx_run_status ON pipeline_runs(status)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_run_time ON pipeline_runs(start_time)")

    # 2. Data Quality Issues table (record-level evidence)
    c.execute("""
    CREATE TABLE IF NOT EXISTS dq_issues (
        issue_id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT,
        study_id TEXT,
        entity_type TEXT,
        rule_id TEXT,
        severity TEXT,
        field_name TEXT,
        message TEXT,
        record_count INTEGER,
        is_quarantine BOOLEAN,
        detected_at TEXT,
        FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
    )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_issue_run ON dq_issues(run_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_issue_severity ON dq_issues(severity)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_issue_rule ON dq_issues(rule_id)")

    # 3. Audit Events table (hash chain)
    c.execute("""
    CREATE TABLE IF NOT EXISTS audit_events (
        event_id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_type TEXT,
        event_timestamp TEXT,
        details TEXT,
        previous_hash TEXT,
        current_hash TEXT
    )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_audit_time ON audit_events(event_timestamp)")

    # 4. Ingestion Manifests table
    c.execute("""
    CREATE TABLE IF NOT EXISTS ingestion_manifests (
        ingestion_id TEXT PRIMARY KEY,
        study_id TEXT,
        entity_type TEXT,
        source_name TEXT,
        source_type TEXT,
        ingestion_timestamp TEXT,
        input_sha256 TEXT,
        raw_record_count INTEGER,
        file_size_bytes INTEGER,
        schema_validation_status TEXT,
        file_path TEXT
    )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_ingest_entity ON ingestion_manifests(entity_type)")

    # 5. Gold Publication table
    c.execute("""
    CREATE TABLE IF NOT EXISTS gold_publications (
        publication_id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT,
        study_id TEXT,
        published_at TEXT,
        table_name TEXT,
        output_format TEXT,
        table_path TEXT,
        record_count INTEGER,
        FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
    )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_gold_run ON gold_publications(run_id)")

    conn.commit()
    conn.close()


def record_dq_issues(run_id: str, study_id: str, issues: List[Dict[str, Any]]):
    """Persist data quality issues to SQLite."""
    if not issues:
        return

    init_db()
    conn = sqlite3.connect(str(get_db_path()))
    c = conn.cursor()
    import datetime
    now_str = datetime.datetime.now(datetime.timezone.utc).isoformat()

    for issue in issues:
        # Only record issues that actually detected violations (or configuration notes)
        c.execute("""
        INSERT INTO dq_issues (
            run_id, study_id, entity_type, rule_id, severity,
            field_name, message, record_count, is_quarantine, detected_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            run_id,
            study_id,
            issue.get("entity", ""),
            issue.get("rule_id", ""),
            issue.get("severity", "LOW"),
            issue.get("field", ""),
            issue.get("message", ""),
            issue.get("record_count", 0),
            1 if issue.get("quarantine", False) else 0,
            now_str,
        ))

    conn.commit()
    conn.close()


def get_dq_issues(run_id: Optional[str] = None, severity: Optional[str] = None) -> List[Dict[str, Any]]:
    """Retrieve data quality issues from SQLite."""
    init_db()
    conn = sqlite3.connect(str(get_db_path()))
    c = conn.cursor()

    query = "SELECT issue_id, run_id, study_id, entity_type, rule_id, severity, field_name, message, record_count, is_quarantine, detected_at FROM dq_issues WHERE 1=1"
    params = []

    if run_id:
        query += " AND run_id = ?"
        params.append(run_id)
    if severity:
        query += " AND severity = ?"
        params.append(severity)

    query += " ORDER BY issue_id DESC"
    c.execute(query, params)
    rows = c.fetchall()
    conn.close()

    return [
        {
            "issue_id": r[0],
            "run_id": r[1],
            "study_id": r[2],
            "entity_type": r[3],
            "rule_id": r[4],
            "severity": r[5],
            "field_name": r[6],
            "message": r[7],
            "record_count": r[8],
            "is_quarantine": bool(r[9]),
            "detected_at": r[10],
        }
        for r in rows
    ]


def record_gold_publication(run_id: str, study_id: str, table_name: str, output_format: str, table_path: str, record_count: int):
    """Record a successful Gold dataset publication."""
    init_db()
    conn = sqlite3.connect(str(get_db_path()))
    c = conn.cursor()
    import datetime
    now_str = datetime.datetime.now(datetime.timezone.utc).isoformat()

    c.execute("""
    INSERT INTO gold_publications (run_id, study_id, published_at, table_name, output_format, table_path, record_count)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (run_id, study_id, now_str, table_name, output_format, table_path, record_count))

    conn.commit()
    conn.close()
