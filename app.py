import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
import datetime
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent))

from triallock import config, synthetic_data, ingestion, pipeline, audit, metrics, readiness, storage, lineage, quality_rules

BASE_DIR = Path(__file__).resolve().parent

st.set_page_config(page_title="TrialLock", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
[data-testid="stAppViewContainer"] { background-color: #f6f8fa; }
[data-testid="stSidebar"] { background-color: #0a1f3c; }
[data-testid="stSidebar"] h1 { color: white; font-size: 1.4rem; margin-bottom: 0.5rem; }
[data-testid="stSidebar"] h2 { color: #b0bdd6; font-size: 1rem; margin-top: 1rem; }
[data-testid="stSidebar"] p { color: #c8d1e0; font-size: 0.9rem; }
[data-testid="stSidebar"] .stRadio > label { color: #e8edf3; font-weight: 500; }
[data-testid="block-container"] { padding-top: 1.5rem; padding-bottom: 1.5rem; }
h1 { color: #0a1f3c; font-weight: 700; letter-spacing: -0.02em; }
h2 { color: #11243d; font-weight: 600; margin-top: 1.5rem; }
h3 { color: #1a2d4a; font-weight: 600; }
.stButton > button { background-color: #0a1f3c; color: white; border-radius: 6px; padding: 0.5rem 1rem; border: none; }
.stButton > button:hover { background-color: #14325e; }
.stMetric { background-color: white; border-radius: 8px; padding: 1rem; box-shadow: 0 1px 3px rgba(10,31,60,0.08); border-left: 4px solid #0a1f3c; }
.stDataFrame { border-radius: 6px; box-shadow: 0 1px 2px rgba(10,31,60,0.06); }
</style>
""", unsafe_allow_html=True)

with st.sidebar:
    st.title("TrialLock")
    st.subheader("Clinical Data Quality and Pipeline Observability")
    st.markdown("---")
    st.write(f"**Study:** {config.STUDY_PROTOCOL.get('study_id', 'N/A')}")
    st.write(f"**Status:** Synthetic Portfolio Prototype")
    st.info("This is a synthetic-data portfolio prototype. Not a validated GxP-compliant clinical system.")
    page = st.radio("Navigation", ["Overview", "Data Ingestion", "Pipeline Runs", "Data Quality", "Lineage and Audit", "Gold Data Explorer"])

if page == "Overview":
    st.title("Overview Dashboard")
    st.header(f"Study {config.STUDY_PROTOCOL.get('study_id')}")
    latest = metrics.get_latest_metrics()
    if latest:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Pipeline Status", latest.get("status"))
        c2.metric("Quality Score", f"{latest.get('quality_score') or 0}%" if latest.get('quality_score') else "N/A")
        c3.metric("Readiness", latest.get("readiness_status", "Unknown"))
        c4.metric("Last Run", latest.get("start_time"))
    else:
        st.warning("No pipeline runs found. Generate demo data and run pipeline.")
    if st.button("Generate Demo Study"):
        data = synthetic_data.generate_demo(config.STUDY_PROTOCOL["study_id"])
        st.success(f"Generated {data['meta']['subjects']} subjects, {data['meta']['visits']} visits, {data['meta']['lab_results']} labs, {data['meta']['adverse_events']} AEs, {data['meta']['pro_responses']} PROs.")
        # The synthetic generator saves bronze CSV files automatically
    st.subheader("Charts")
    col1, col2 = st.columns(2)
    with col1:
        latest = metrics.get_latest_metrics()
        if latest and latest.get("silver_counts"):
            import json
            sc = json.loads(str(latest.get("silver_counts", {})))
            labels = list(sc.keys())
            vals = list(sc.values())
        else:
            labels = ["subjects", "visits", "lab_results"]
            vals = [0, 0, 0]
        st.plotly_chart(px.bar(x=labels, y=vals, title="Silver Layer Counts"))
    with col2:
        # Read real quarantine records if available
        quarantine_path = BASE_DIR / "data" / "quarantine"
        if quarantine_path.exists() and any(quarantine_path.iterdir()):
            import os
            files = [f for f in os.listdir(str(quarantine_path)) if f.endswith(".parquet")]
            if files:
                import pandas as pd
                df_q = pd.read_parquet(str(quarantine_path / files[0]))
                counts = df_q["entity_type"].value_counts().to_dict() if "entity_type" in df_q.columns else {}
                st.plotly_chart(px.pie(names=list(counts.keys()), values=list(counts.values()), title="Quarantine by Entity"))
            else:
                st.info("No quarantine data yet.")
        else:
            st.info("No quarantine data yet.")

elif page == "Data Ingestion":
    st.title("Data Ingestion")
    method = st.selectbox("Method", ["Generate Demo Study", "File Upload", "REST API"])
    if method == "Generate Demo Study":
        if st.button("Generate"):
            data = synthetic_data.generate_demo(config.STUDY_PROTOCOL["study_id"])
            st.success(f"Generated demo data: Subjects={data['meta']['subjects']}")
    elif method == "File Upload":
        upload_entity = st.selectbox("Entity Type", ["subjects", "visits", "lab_results", "adverse_events", "pro_responses"], key="upload_entity")
        uploaded = st.file_uploader("Upload CSV/JSON", type=["csv", "json"])
        if uploaded:
            import hashlib
            file_bytes = uploaded.read()
            checksum = hashlib.sha256(file_bytes).hexdigest()
            st.info(f"Uploaded {uploaded.name} ({len(file_bytes)} bytes) SHA-256: {checksum[:16]}...")
            # Save to bronze using ingestion module
            try:
                ingestion.init_db()
                ingestion.save_bronze(file_bytes, f"upload_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S')}", upload_entity, uploaded.name)
                ingestion.record_ingestion({
                    "ingestion_id": f"upload_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S')}",
                    "study_id": config.STUDY_PROTOCOL.get("study_id", "IBD-DEMO-001"),
                    "entity_type": upload_entity,
                    "source_name": uploaded.name,
                    "source_type": "upload",
                    "ingestion_timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "input_sha256": checksum,
                    "raw_record_count": 0,
                    "file_size_bytes": len(file_bytes),
                    "schema_validation_status": "pending",
                })
                st.success("File saved to Bronze. Ingestion metadata recorded.")
            except Exception as exc:
                st.error(f"Failed to save upload: {exc}")
    elif method == "REST API":
        url = st.text_input("Endpoint URL")
        entity = st.selectbox("Entity", ["subjects", "visits", "lab_results", "adverse_events", "pro_responses"])
        token = st.text_input("Bearer Token (optional)", type="password")
        timeout = st.number_input("Timeout (seconds)", min_value=5, max_value=120, value=30)
        if st.button("Fetch"):
            import requests
            if not url.startswith(("http://", "https://")):
                st.error("URL must start with http:// or https://")
            else:
                try:
                    headers = {}
                    if token:
                        headers["Authorization"] = f"Bearer {token}"
                    resp = requests.get(url, headers=headers, timeout=timeout)
                    resp.raise_for_status()
                    st.success(f"Fetched status {resp.status_code} from {url}")
                    try:
                        data_json = resp.json()
                        st.json(data_json if isinstance(data_json, dict) else {"records": data_json[:3] if isinstance(data_json, list) else data_json})
                    except Exception:
                        st.info("Response is not JSON or too large to preview.")
                except Exception as exc:
                    st.error(f"Request failed: {exc}")

elif page == "Pipeline Runs":
    st.title("Pipeline Runs")
    if st.button("Run Pipeline"):
        result = pipeline.run_pipeline(config.STUDY_PROTOCOL["study_id"])
        if result["status"] == "COMPLETED":
            st.success(f"Pipeline {result['run_id']} completed in {result['duration_sec']}s with score {result['quality_score']}")
        else:
            st.error(f"Pipeline failed: {result.get('error')}")
    latest = metrics.get_latest_metrics()
    if latest:
        st.json({"run_id": latest["run_id"], "status": latest["status"], "score": latest["quality_score"], "readiness": latest["readiness_status"]})

elif page == "Data Quality":
    st.title("Data Quality")
    # Read actual pipeline results for quality info
    latest = metrics.get_latest_metrics()
    st.info(f"Latest pipeline: {latest['run_id'] if latest else 'N/A'} | Status: {latest['status'] if latest else 'N/A'} | Score: {latest.get('quality_score', 'N/A') if latest else 'N/A'}")
    # Try reading quarantine parquet files for real issues
    import sqlite3, json
    issues = []
    quarantine_path = BASE_DIR / "data" / "quarantine"
    if quarantine_path.exists():
        for f in quarantine_path.iterdir():
            if f.suffix == ".parquet":
                try:
                    import pandas as pd
                    df_q = pd.read_parquet(str(f))
                    for _, row in df_q.iterrows():
                        issues.append({
                            "Rule ID": str(row.get("rule_id", "UNKNOWN")),
                            "Severity": "BLOCKER",
                            "Entity": str(row.get("entity_type", str(f.stem))),
                            "Message": f"Quarantined record from pipeline run",
                        })
                except Exception:
                    pass
    # Show real issues if available; otherwise show clear empty state
    if not issues:
        st.success("No data quality issues found in quarantine. If validation has not been executed, generate data and run the pipeline.")
    issues_df = pd.DataFrame(issues) if issues else pd.DataFrame(columns=["Rule ID", "Severity", "Entity", "Message"])
    st.dataframe(issues_df)
    score_display = latest.get("quality_score") if latest and latest.get("quality_score") is not None else None
    if score_display is not None:
        st.metric("Quality Score", f"{score_display}%")
        st.metric("Applicable Checks (demo estimate)", "Calculated from pipeline results")
    else:
        st.metric("Quality Score", "Not evaluated")
        st.info("No pipeline results available. Run the synthetic study and pipeline first.")

elif page == "Lineage and Audit":
    st.title("Lineage and Audit")
    valid, results = audit.verify_audit_chain()
    st.write(f"Audit chain verified: {'Yes' if valid else 'No'}")
    for r in results:
        st.write(f"Event {r['event_id']} match={r['match']}")
    if st.button("Verify Chain"):
        st.info("Audit chain verification performed.")

elif page == "Gold Data Explorer":
    st.title("Gold Data Explorer")
    table = st.selectbox("Gold Table", ["dim_subjects", "fact_visits", "fact_lab_results", "fact_adverse_events", "fact_pro_responses", "dq_issue_summary"])
    st.write(f"Selected: {table}")
    # Try reading actual parquet or CSV
    import os
    gold_path = BASE_DIR / "data" / "gold"
    file_path = gold_path / (table + ".parquet") if (gold_path / (table + ".parquet")).exists() else gold_path / (table + ".csv")
    if file_path.exists():
        try:
            import pandas as pd
            df_preview = pd.read_parquet(str(file_path)) if file_path.suffix == ".parquet" else pd.read_csv(str(file_path), nrows=20)
            st.write(f"Preview ({len(df_preview)} rows shown from {table}):")
            st.dataframe(df_preview.head(20))
        except Exception as exc:
            st.warning(f"Could not read {table}: {exc}")
    else:
        st.info(f"No output file found for {table}. Run the pipeline first.")
    st.code("SELECT * FROM " + table + " LIMIT 10")
    # Real CSV download when file exists
    if file_path.exists():
        with open(file_path, "rb") as f:
            data = f.read()
        st.download_button("Download CSV", data, file_name=f"{table}.csv", mime="text/csv")
    else:
        st.download_button("Download CSV (not available)", b"table,field\n", file_name="placeholder.csv", mime="text/csv", disabled=True)

st.markdown("---")
st.caption("Synthetic portfolio prototype for TrialLock. Not a validated GxP clinical system.")
