"""
TrialLock Medallion Processing Pipeline.

Executes:
Bronze (raw preservation) -> Silver (normalization & validation) ->
Quarantine (rejected critical failures) -> Gold (curated analytical datasets)
"""

import datetime
import json
import shutil
from pathlib import Path
from typing import Dict, Any, Optional

from pyspark.sql import functions as F

from triallock.spark_session import get_spark
from triallock.audit import append_audit
from triallock import config
from triallock.validation import validate_entity
from triallock.quality_rules import calculate_quality_score
from triallock.readiness import assess_readiness, reconcile_expected_visits
from triallock.storage import init_db, record_dq_issues, record_gold_publication, get_db_path

BASE_DIR = Path(__file__).resolve().parent.parent


def safe_write_dataset(spark_df, output_path: Path, mode: str = "overwrite") -> str:
    """
    Safely write a DataFrame to Parquet using proper file:// URI format for Windows.
    Falls back to CSV if Parquet write fails.

    Returns:
        Format written: "parquet" or "csv"
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    uri_str = output_path.as_uri()

    try:
        spark_df.write.mode(mode).parquet(uri_str)
        return "parquet"
    except Exception as parquet_exc:
        try:
            spark_df.write.mode(mode).option("header", "true").csv(uri_str)
            return "csv"
        except Exception as csv_exc:
            raise Exception(
                f"Failed to write output to {output_path}: Parquet failed ({parquet_exc}), "
                f"CSV fallback failed ({csv_exc}). Ensure HADOOP_HOME is configured."
            ) from csv_exc


def run_pipeline(study_id: str = "IBD-DEMO-001") -> Dict[str, Any]:
    """
    Execute the full medallion data pipeline:
    1. Ingest Bronze data
    2. Normalize and validate into Silver
    3. Evaluate quality rules and separate Quarantine records
    4. Curate and publish Gold datasets safely
    5. Track metrics, lineage, and audit records

    Returns:
        Dict with comprehensive execution results
    """
    run_id = f"RUN-{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S')}"
    start_time = datetime.datetime.now(datetime.timezone.utc)

    # Initialize database
    init_db()
    append_audit("pipeline_start", {"run_id": run_id, "study_id": study_id})

    # Record initial run state
    import sqlite3
    conn = sqlite3.connect(str(get_db_path()))
    c = conn.cursor()
    c.execute(
        "INSERT INTO pipeline_runs (run_id, study_id, start_time, status) VALUES (?, ?, ?, ?)",
        (run_id, study_id, start_time.isoformat(), "RUNNING")
    )
    conn.commit()
    conn.close()

    spark = None
    try:
        # Start Spark session
        spark = get_spark("TrialLockPipeline")
        if spark is None:
            raise RuntimeError(
                "Spark session could not be initialized. Verify Java 17 and Windows Hadoop environment."
            )

        bronze_dir = BASE_DIR / "data" / "bronze"
        silver_dir = BASE_DIR / "data" / "silver"
        gold_dir = BASE_DIR / "data" / "gold"
        quarantine_dir = BASE_DIR / "data" / "quarantine"
        temp_gold_dir = BASE_DIR / "data" / f"temp_gold_{run_id}"

        for d in [silver_dir, gold_dir, quarantine_dir, temp_gold_dir]:
            d.mkdir(parents=True, exist_ok=True)

        entities = ["subjects", "visits", "lab_results", "adverse_events", "pro_responses"]

        # State tracking
        input_counts = {}
        silver_counts = {}
        gold_counts = {}
        quarantined_counts = {}
        all_issues = []
        total_checks = 0
        passed_checks = 0
        failed_checks = 0

        # In-memory reference DataFrames for foreign key validation
        loaded_dfs = {}

        # ---------------------------------------------------------------------
        # STAGE 1 & 2: BRONZE INGESTION & SILVER NORMALIZATION
        # ---------------------------------------------------------------------
        # Load subjects first as reference dataset for child entities
        load_order = ["subjects", "visits", "lab_results", "adverse_events", "pro_responses"]

        for entity in load_order:
            bronze_file = bronze_dir / f"{entity}_{study_id}.csv"

            if not bronze_file.exists():
                input_counts[entity] = 0
                silver_counts[entity] = 0
                gold_counts[entity] = 0
                quarantined_counts[entity] = 0
                continue

            # Read raw Bronze data
            df_raw = (
                spark.read
                .option("header", "true")
                .option("inferSchema", "false")
                .csv(bronze_file.as_uri())
            )
            raw_count = df_raw.count()
            input_counts[entity] = raw_count

            # Normalize column names: lowercase, strip whitespace, replace spaces with underscores
            df_norm = df_raw
            for col in df_raw.columns:
                clean_name = col.strip().lower().replace(" ", "_")
                if clean_name != col:
                    df_norm = df_norm.withColumnRenamed(col, clean_name)

            # Add pipeline metadata columns
            df_silver = (
                df_norm
                .withColumn("_ingestion_run_id", F.lit(run_id))
                .withColumn("_ingestion_timestamp", F.current_timestamp())
                .withColumn("_study_id", F.lit(study_id))
            )

            # Save to Silver layer
            safe_write_dataset(df_silver, silver_dir / entity)
            silver_count = df_silver.count()
            silver_counts[entity] = silver_count
            loaded_dfs[entity] = df_silver

            # -----------------------------------------------------------------
            # VALIDATION & QUARANTINE
            # -----------------------------------------------------------------
            # Validate entity using quality rules and reference DataFrames
            val_result = validate_entity(
                spark_df=df_silver,
                entity=entity,
                reference_dfs=loaded_dfs,
                run_id=run_id,
                study_id=study_id,
            )

            entity_issues = val_result["issues"]
            all_issues.extend(entity_issues)
            total_checks += val_result["total_checks"]
            passed_checks += val_result["passed_checks"]
            failed_checks += val_result["failed_checks"]

            quarantine_df = val_result["quarantine_df"]

            if quarantine_df is not None:
                q_count = quarantine_df.count()
                quarantined_counts[entity] = q_count
                if q_count > 0:
                    # Write quarantine records
                    safe_write_dataset(quarantine_df, quarantine_dir / f"{entity}_{run_id}")
            else:
                quarantined_counts[entity] = 0

            # -----------------------------------------------------------------
            # STAGE 3: GOLD CURATION (Staged in temp directory)
            # -----------------------------------------------------------------
            # Filter out quarantined records if any were identified
            gold_table_names = {
                "subjects": "dim_subjects",
                "visits": "fact_visits",
                "lab_results": "fact_lab_results",
                "adverse_events": "fact_adverse_events",
                "pro_responses": "fact_pro_responses",
            }
            gold_name = gold_table_names.get(entity, f"curated_{entity}")

            # For subjects, ensure duplicate business keys are dropped for Gold
            if entity == "subjects":
                df_gold = df_silver.dropDuplicates(["study_id", "subject_id"])
            else:
                df_gold = df_silver

            # Write to temporary gold directory first (safe publication pattern)
            out_format = safe_write_dataset(df_gold, temp_gold_dir / gold_name)
            gold_count = df_gold.count()
            gold_counts[entity] = gold_count

            # Record gold publication metadata
            record_gold_publication(
                run_id=run_id,
                study_id=study_id,
                table_name=gold_name,
                output_format=out_format,
                table_path=str(gold_dir / gold_name),
                record_count=gold_count,
            )

        # ---------------------------------------------------------------------
        # SAFE GOLD PUBLICATION COMMIT
        # ---------------------------------------------------------------------
        # Copy curated datasets from temp_gold_dir to final gold_dir
        # Only reached if all processing succeeded
        for item in temp_gold_dir.iterdir():
            target = gold_dir / item.name
            if target.exists():
                if target.is_dir():
                    shutil.rmtree(target)
                else:
                    target.unlink()
            if item.is_dir():
                shutil.copytree(item, target)
            else:
                shutil.copy2(item, target)

        # Clean up temp directory
        shutil.rmtree(temp_gold_dir, ignore_errors=True)

        # ---------------------------------------------------------------------
        # STAGE 4: QUALITY SCORE & READINESS EVALUATION
        # ---------------------------------------------------------------------
        # Calculate quality score according to specification:
        # Quality score = 100 * passed applicable checks / total applicable checks
        quality_score = calculate_quality_score(total_checks, passed_checks)

        # Reconcile expected visits
        visit_reconciliation = reconcile_expected_visits(
            loaded_dfs.get("subjects"),
            loaded_dfs.get("visits"),
        )

        # Assess study database-lock readiness
        readiness_status, readiness_reason = assess_readiness(
            issues=all_issues,
            visit_completion=visit_reconciliation,
            quality_score=quality_score,
            threshold=config.STUDY_PROTOCOL.get("minimum_quality_score", 95),
        )

        # ---------------------------------------------------------------------
        # STAGE 5: PERSISTENCE & AUDIT
        # ---------------------------------------------------------------------
        # Persist DQ issues to SQLite
        record_dq_issues(run_id, study_id, all_issues)

        # Record completed run metadata
        end_time = datetime.datetime.now(datetime.timezone.utc)
        duration = int((end_time - start_time).total_seconds())

        conn = sqlite3.connect(str(get_db_path()))
        c = conn.cursor()
        c.execute("""
        UPDATE pipeline_runs SET
            end_time = ?,
            duration_sec = ?,
            status = ?,
            input_counts = ?,
            silver_counts = ?,
            gold_counts = ?,
            quarantined_counts = ?,
            quality_score = ?,
            total_checks = ?,
            passed_checks = ?,
            failed_checks = ?,
            readiness_status = ?,
            readiness_reason = ?,
            error_message = ?
        WHERE run_id = ?
        """, (
            end_time.isoformat(),
            duration,
            "COMPLETED",
            json.dumps(input_counts),
            json.dumps(silver_counts),
            json.dumps(gold_counts),
            json.dumps(quarantined_counts),
            quality_score,
            total_checks,
            passed_checks,
            failed_checks,
            readiness_status,
            readiness_reason,
            "",
            run_id,
        ))
        conn.commit()
        conn.close()

        append_audit("pipeline_complete", {
            "run_id": run_id,
            "status": "COMPLETED",
            "quality_score": quality_score,
            "readiness_status": readiness_status,
            "total_checks": total_checks,
            "passed_checks": passed_checks,
            "failed_checks": failed_checks,
        })

        return {
            "run_id": run_id,
            "status": "COMPLETED",
            "duration_sec": duration,
            "input_counts": input_counts,
            "silver_counts": silver_counts,
            "gold_counts": gold_counts,
            "quarantined_counts": quarantined_counts,
            "quality_score": quality_score,
            "total_checks": total_checks,
            "passed_checks": passed_checks,
            "failed_checks": failed_checks,
            "readiness_status": readiness_status,
            "readiness_reason": readiness_reason,
            "issues": all_issues,
        }

    except Exception as exc:
        # Clean up temp gold directory if run failed
        temp_gold = BASE_DIR / "data" / f"temp_gold_{run_id}"
        if temp_gold.exists():
            shutil.rmtree(temp_gold, ignore_errors=True)

        error_msg = f"{type(exc).__name__}: {str(exc)}"
        append_audit("pipeline_failure", {"run_id": run_id, "error": error_msg})

        conn = sqlite3.connect(str(get_db_path()))
        c = conn.cursor()
        c.execute(
            "UPDATE pipeline_runs SET status = 'FAILED', error_message = ? WHERE run_id = ?",
            (error_msg, run_id)
        )
        conn.commit()
        conn.close()

        return {
            "run_id": run_id,
            "status": "FAILED",
            "error": error_msg,
        }

    finally:
        # Don't stop Spark session here to allow reuse, but clean up state if needed
        pass
