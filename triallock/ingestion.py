import hashlib
import os
import datetime
import sqlite3
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

def init_db():
    db_path = BASE_DIR / "triallock.db"
    conn = sqlite3.connect(str(db_path))
    c = conn.cursor()
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
        schema_validation_status TEXT
    )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_ingest_entity ON ingestion_manifests(entity_type)")
    conn.commit()
    conn.close()

def compute_sha256(data_bytes):
    return hashlib.sha256(data_bytes).hexdigest()

def save_bronze(source_bytes, ingestion_id, entity_type, file_name):
    bronze_dir = BASE_DIR / "data" / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    path = bronze_dir / f"{entity_type}_{ingestion_id}_{file_name}"
    with open(path, "wb") as f:
        f.write(source_bytes)
    return str(path)

def record_ingestion(manifest):
    db_path = BASE_DIR / "triallock.db"
    conn = sqlite3.connect(str(db_path))
    c = conn.cursor()
    c.execute("INSERT INTO ingestion_manifests VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
              (manifest.get("ingestion_id"), manifest.get("study_id"), manifest.get("entity_type"),
               manifest.get("source_name"), manifest.get("source_type"), manifest.get("ingestion_timestamp"),
               manifest.get("input_sha256"), manifest.get("raw_record_count"), manifest.get("file_size_bytes"),
               manifest.get("schema_validation_status")))
    conn.commit()
    conn.close()
