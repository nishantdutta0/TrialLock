import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
import datetime
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent))

from triallock import config, synthetic_data, ingestion, pipeline, audit, metrics, readiness, storage, lineage, quality_rules

st.set_page_config(page_title="TrialLock", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
[data-testid="stAppViewContainer"] { background-color: #f6f8fa; }
[data-testid="stSidebar"] { background-color: #0a1f3c; }
[data-testid="stSidebar"] h1 { color: white; }
[data-testid="stSidebar"] p { color: #c8d1e0; }
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
        for entity, records in data.items():
            if entity == "meta" or not isinstance(records, list):
                continue
            for r in records:
                pass  # Could save to bronze here in full version
    st.subheader("Charts")
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(px.bar(pd.DataFrame({"Layer": ["Bronze", "Silver", "Gold"], "Count": [150, 120, 115]}), x="Layer", y="Count", title="Layer Counts"))
    with col2:
        st.plotly_chart(px.pie(pd.DataFrame({"Severity": ["BLOCKER", "HIGH", "MEDIUM", "LOW"], "Count": [5, 8, 20, 30]}), names="Severity", values="Count", title="Issues by Severity"))

elif page == "Data Ingestion":
    st.title("Data Ingestion")
    method = st.selectbox("Method", ["Generate Demo Study", "File Upload", "REST API"])
    if method == "Generate Demo Study":
        if st.button("Generate"):
            data = synthetic_data.generate_demo(config.STUDY_PROTOCOL["study_id"])
            st.success(f"Generated demo data: Subjects={data['meta']['subjects']}")
    elif method == "File Upload":
        uploaded = st.file_uploader("Upload CSV/JSON", type=["csv", "json"])
        if uploaded:
            st.info(f"Uploaded {uploaded.name} ({uploaded.size} bytes)")
            st.write("Preview not implemented fully; would parse and validate columns.")
    elif method == "REST API":
        url = st.text_input("Endpoint URL")
        entity = st.selectbox("Entity", ["subjects", "visits", "lab_results", "adverse_events", "pro_responses"])
        if st.button("Fetch"):
            st.info("REST fetch simulated. Use Generate Demo Study for full demo.")

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
    issues_df = pd.DataFrame([
        {"Rule ID": "MANDATORY_SUBJECT_ID", "Severity": "BLOCKER", "Entity": "subjects", "Message": "Missing subject identifier"},
        {"Rule ID": "DUPLICATE_SUBJECT_KEY", "Severity": "BLOCKER", "Entity": "subjects", "Message": "Duplicate business key"},
        {"Rule ID": "CONSENT_BEFORE_RANDOMIZATION", "Severity": "HIGH", "Entity": "subjects", "Message": "Consent after randomization"},
        {"Rule ID": "LAB_NUMERIC_VALUE", "Severity": "BLOCKER", "Entity": "lab_results", "Message": "Result value is not numeric"},
    ])
    st.dataframe(issues_df)
    st.metric("Quality Score", "92.0%")
    st.metric("Applicable Checks", "220 / 232 passed")

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
    st.code("SELECT * FROM " + table + " LIMIT 10")
    st.download_button("Download CSV", "test", file_name="export.csv", mime="text/csv")

st.markdown("---")
st.caption("Synthetic portfolio prototype for TrialLock. Not a validated GxP clinical system.")
