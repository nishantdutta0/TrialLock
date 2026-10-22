import datetime
import sqlite3
from pathlib import Path
from triallock.spark_session import get_spark
from triallock.audit import append_audit
from triallock import config
import pyarrow.parquet as pq

BASE_DIR = Path(__file__).resolve().parent.parent

def run_pipeline(study_id="IBD-DEMO-001"):
    spark = get_spark("PipelineRun")
    run_id = f"RUN-{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S')}"
    start_time = datetime.datetime.now(datetime.timezone.utc)
    append_audit("pipeline_start", {"run_id": run_id, "study_id": study_id})
    try:
        bronze_path = BASE_DIR / "data" / "bronze"
        silver_dir = BASE_DIR / "data" / "silver"
        gold_dir = BASE_DIR / "data" / "gold"
        quarantine_dir = BASE_DIR / "data" / "quarantine"
        for d in [silver_dir, gold_dir, quarantine_dir]:
            d.mkdir(parents=True, exist_ok=True)

        # Read bronze CSV files with Spark
        entities = ["subjects", "visits", "lab_results", "adverse_events", "pro_responses"]
        silver_counts = {}
        quarantined_counts = {}
        gold_counts = {}
        total_issues = 0
        from triallock import quality_rules, validation

        def safe_write(spark_df, output_path, mode="overwrite"):
            try:
                spark_df.write.mode(mode).parquet(str(output_path))
            except Exception as parquet_exc:
                try:
                    spark_df.write.mode(mode).option("header", "true").csv(str(output_path))
                except Exception as csv_exc:
                    raise Exception(
                        f"Failed to write output to {output_path}: parquet failed ({parquet_exc}), CSV fallback failed ({csv_exc}). "
                        f"Check that HADOOP_HOME is set and output directory is writable."
                    ) from csv_exc
        # Read bronze CSV files with Spark and process subjects

        # Process subjects (output already handled above with safe_write in the loop)
        # Ensure gold/quarantine/silver outputs exist even if Spark write fails by using CSV fallbacks already embedded
        sub_path = bronze_path / f"subjects_{study_id}.csv"
        if sub_path.exists():
            df_sub = spark.read.option("header", "true").option("inferSchema", "false").csv(str(sub_path))
            # Basic normalization: trim and lowercase column names
            for c in df_sub.columns:
                df_sub = df_sub.withColumnRenamed(c, c.strip().lower().replace(" ", "_"))
            sub_total = df_sub.count()
            silver_counts["subjects"] = sub_total
            from pyspark.sql import functions as F
            dup_sub = df_sub.groupBy("subject_id").count().filter(F.col("count") > 1)
            quarantine_sub = dup_sub.select("subject_id", F.lit("DUPLICATE_SUBJECT_KEY").alias("rule_id"), F.lit(study_id).alias("study_id"), F.lit("subjects").alias("entity_type"))
            safe_write(quarantine_sub, quarantine_dir / f"subjects_{run_id}")
            quarantined_counts["subjects"] = quarantine_sub.count()
            safe_write(df_sub, silver_dir / "subjects")
            clean_sub = df_sub.dropDuplicates(["study_id", "subject_id"])
            gold_counts["subjects"] = clean_sub.count()
            # Execute configured validation rules against this entity
            sub_issues = quality_rules.evaluate_rules(df_sub, "subjects", config.VALIDATION_RULES)
            sub_validation = validation.validate_entity(df_sub, "subjects")
            total_issues += len(sub_issues) if sub_issues else 0
            safe_write(clean_sub, gold_dir / "dim_subjects")
        else:
            silver_counts["subjects"] = 0
            quarantined_counts["subjects"] = 0
            gold_counts["subjects"] = 0

        # For simplicity, process visits similarly using synthetic counts from bronze if file exists
        visit_path = bronze_path / f"visits_{study_id}.csv"
        if visit_path.exists():
            df_vis = spark.read.option("header", "true").csv(str(visit_path))
            silver_counts["visits"] = df_vis.count()
            visit_issues = quality_rules.evaluate_rules(df_vis, "visits", config.VALIDATION_RULES)
            total_issues += len(visit_issues) if visit_issues else 0
            safe_write(df_vis, silver_dir / "visits")
            gold_counts["visits"] = df_vis.count()
            safe_write(df_vis, gold_dir / "fact_visits")
        else:
            # No bronze input found - report explicitly rather than inventing counts
            silver_counts["visits"] = 0
            quarantined_counts["visits"] = 0
            gold_counts["visits"] = 0

        # Lab results
        lab_path = bronze_path / f"lab_results_{study_id}.csv"
        if lab_path.exists():
            df_lab = spark.read.option("header", "true").csv(str(lab_path))
            silver_counts["lab_results"] = df_lab.count()
            lab_issues = quality_rules.evaluate_rules(df_lab, "lab_results", config.VALIDATION_RULES)
            total_issues += len(lab_issues) if lab_issues else 0
            safe_write(df_lab, silver_dir / "lab_results")
            gold_counts["lab_results"] = df_lab.count()
            safe_write(df_lab, gold_dir / "fact_lab_results")
        else:
            silver_counts["lab_results"] = 0
            quarantined_counts["lab_results"] = 0
            gold_counts["lab_results"] = 0

        # Adverse events
        ae_path = bronze_path / f"adverse_events_{study_id}.csv"
        if ae_path.exists():
            df_ae = spark.read.option("header", "true").csv(str(ae_path))
            silver_counts["adverse_events"] = df_ae.count()
            ae_issues = quality_rules.evaluate_rules(df_ae, "adverse_events", config.VALIDATION_RULES)
            total_issues += len(ae_issues) if ae_issues else 0
            safe_write(df_ae, silver_dir / "adverse_events")
            gold_counts["adverse_events"] = df_ae.count()
            safe_write(df_ae, gold_dir / "fact_adverse_events")
        else:
            silver_counts["adverse_events"] = 0
            quarantined_counts["adverse_events"] = 0
            gold_counts["adverse_events"] = 0

        # PRO responses
        pro_path = bronze_path / f"pro_responses_{study_id}.csv"
        if pro_path.exists():
            df_pro = spark.read.option("header", "true").csv(str(pro_path))
            silver_counts["pro_responses"] = df_pro.count()
            pro_issues = quality_rules.evaluate_rules(df_pro, "pro_responses", config.VALIDATION_RULES)
            total_issues += len(pro_issues) if pro_issues else 0
            safe_write(df_pro, silver_dir / "pro_responses")
            gold_counts["pro_responses"] = df_pro.count()
            safe_write(df_pro, gold_dir / "fact_pro_responses")
        else:
            silver_counts["pro_responses"] = 0
            quarantined_counts["pro_responses"] = 0
            gold_counts["pro_responses"] = 0

        # Ensure quarantined_counts exists (should come from validation rules execution in full version)
        if not quarantined_counts:
            quarantined_counts = {}

        # Compute quality score from actual usable vs input counts
        total_input = sum(silver_counts.values()) if silver_counts else 0
        total_usable = sum(gold_counts.values()) if gold_counts else 0
        if total_input > 0:
            score = round(100 * total_usable / total_input, 2)
        else:
            score = 0.0
        # Readiness based solely on score and availability of results (simplified)
        readiness = "READY FOR DATA REVIEW" if score >= 95 else ("AT RISK" if score >= 85 else "BLOCKED")

        end_time = datetime.datetime.now(datetime.timezone.utc)
        duration = (end_time - start_time).total_seconds()
        # Update DB
        from triallock.storage import init_db
        init_db()
        conn = sqlite3.connect(str(BASE_DIR / "triallock.db"))
        c = conn.cursor()
        # Ensure pipeline_runs table exists with right columns
        c.execute("""CREATE TABLE IF NOT EXISTS pipeline_runs (
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
            readiness_status TEXT,
            error_message TEXT
        )""")
        conn.commit()
        # Insert or update
        c.execute("INSERT OR IGNORE INTO pipeline_runs (run_id, study_id, start_time, status) VALUES (?, ?, ?, ?)",
                  (run_id, study_id, start_time.isoformat(), "COMPLETED"))
        c.execute("UPDATE pipeline_runs SET end_time=?, duration_sec=?, status=?, silver_counts=?, gold_counts=?, quarantined_counts=?, quality_score=?, readiness_status=?, error_message=? WHERE run_id=?",
                  (end_time.isoformat(), int(duration), "COMPLETED", str(silver_counts), str(gold_counts), str(quarantined_counts), score, readiness, "", run_id))
        conn.commit()
        conn.close()
        append_audit("pipeline_complete", {"run_id": run_id, "score": score, "readiness": readiness})
        return {
            "run_id": run_id,
            "status": "COMPLETED",
            "duration_sec": int(duration),
            "silver_counts": silver_counts,
            "gold_counts": gold_counts,
            "quarantined_counts": quarantined_counts,
            "quality_score": score,
            "readiness_status": readiness,
        }
    except Exception as exc:
        append_audit("pipeline_failure", {"run_id": run_id, "error": str(exc)})
        from triallock.storage import init_db
        init_db()
        conn = sqlite3.connect(str(BASE_DIR / "triallock.db"))
        c = conn.cursor()
        c.execute("UPDATE pipeline_runs SET status='FAILED', error_message=? WHERE run_id=?", (str(exc), run_id))
        conn.commit()
        conn.close()
        return {"run_id": run_id, "status": "FAILED", "error": str(exc)}
