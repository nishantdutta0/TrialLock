"""
Metrics reporting and retrieval for TrialLock.
"""

import sqlite3
import json
from pathlib import Path
from typing import Dict, Any, Optional, List

BASE_DIR = Path(__file__).resolve().parent.parent


def get_latest_metrics() -> Optional[Dict[str, Any]]:
    """Get the most recent pipeline run metrics."""
    conn = sqlite3.connect(str(BASE_DIR / "triallock.db"))
    c = conn.cursor()
    c.execute("""
    SELECT
        run_id, study_id, start_time, end_time, duration_sec, status,
        input_counts, silver_counts, gold_counts, quarantined_counts,
        quality_score, total_checks, passed_checks, failed_checks,
        readiness_status, readiness_reason, error_message
    FROM pipeline_runs
    ORDER BY start_time DESC
    LIMIT 1
    """)
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
            "input_counts": json.loads(row[6]) if row[6] and row[6].startswith("{") else {},
            "silver_counts": json.loads(row[7]) if row[7] and row[7].startswith("{") else {},
            "gold_counts": json.loads(row[8]) if row[8] and row[8].startswith("{") else {},
            "quarantined_counts": json.loads(row[9]) if row[9] and row[9].startswith("{") else {},
            "quality_score": row[10],
            "total_checks": row[11],
            "passed_checks": row[12],
            "failed_checks": row[13],
            "readiness_status": row[14],
            "readiness_reason": row[15],
            "error_message": row[16],
        }
    return None


def get_all_runs(limit: int = 10) -> List[Dict[str, Any]]:
    """Get recent pipeline runs."""
    conn = sqlite3.connect(str(BASE_DIR / "triallock.db"))
    c = conn.cursor()
    c.execute("""
    SELECT
        run_id, study_id, start_time, duration_sec, status,
        quality_score, readiness_status, error_message
    FROM pipeline_runs
    ORDER BY start_time DESC
    LIMIT ?
    """, (limit,))
    rows = c.fetchall()
    conn.close()

    return [
        {
            "run_id": r[0],
            "study_id": r[1],
            "start_time": r[2],
            "duration_sec": r[3],
            "status": r[4],
            "quality_score": r[5],
            "readiness_status": r[6],
            "error_message": r[7],
        }
        for r in rows
    ]
