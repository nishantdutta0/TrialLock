"""
TrialLock - Clinical Data Quality and Pipeline Observability Workbench.
Streamlit Application.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import datetime
import json
import os
import sys

# Ensure project root is on path
BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from triallock import (
    config,
    synthetic_data,
    ingestion,
    pipeline,
    audit,
    metrics,
    readiness,
    storage,
    lineage,
    quality_rules,
)

# -----------------------------------------------------------------------------
# PAGE CONFIGURATION & STYLING
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="TrialLock | Clinical Data Workbench",
    page_icon="🔒",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Clinical CSS Theme
st.markdown("""
<style>
    /* Main container background */
    [data-testid="stAppViewContainer"] {
        background-color: #F8F9FA;
        color: #1A202C;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    }

    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background-color: #0F172A;
        border-right: 1px solid #1E293B;
    }
    [data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3 {
        color: #F8FAFC;
    }
    [data-testid="stSidebar"] p, [data-testid="stSidebar"] span, [data-testid="stSidebar"] label {
        color: #94A3B8;
    }
    [data-testid="stSidebar"] .stRadio label {
        color: #CBD5E1 !important;
        font-weight: 500;
    }

    /* Page container padding */
    [data-testid="block-container"] {
        padding-top: 2rem;
        padding-bottom: 3rem;
        max-width: 1400px;
    }

    /* Typography */
    h1 {
        color: #0F172A;
        font-weight: 700;
        font-size: 1.85rem;
        letter-spacing: -0.025em;
        margin-bottom: 0.5rem;
    }
    h2 {
        color: #1E293B;
        font-weight: 600;
        font-size: 1.4rem;
        letter-spacing: -0.02em;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
    }
    h3 {
        color: #334155;
        font-weight: 600;
        font-size: 1.1rem;
    }

    /* KPI / Metric cards */
    [data-testid="stMetric"] {
        background-color: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 8px;
        padding: 1.25rem 1.5rem;
        box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.05);
        border-left: 4px solid #0284C7;
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.85rem !important;
        font-weight: 600 !important;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: #64748B !important;
    }
    [data-testid="stMetricValue"] {
        font-size: 1.8rem !important;
        font-weight: 700 !important;
        color: #0F172A !important;
    }

    /* Severity badges */
    .badge {
        display: inline-block;
        padding: 0.25rem 0.6rem;
        border-radius: 4px;
        font-size: 0.75rem;
        font-weight: 700;
        letter-spacing: 0.05em;
        text-transform: uppercase;
    }
    .badge-blocker { background-color: #FEE2E2; color: #991B1B; border: 1px solid #FCA5A5; }
    .badge-high { background-color: #FFEDD5; color: #9A3412; border: 1px solid #FDBA74; }
    .badge-medium { background-color: #FEF3C7; color: #92400E; border: 1px solid #FCD34D; }
    .badge-low { background-color: #E0F2FE; color: #075985; border: 1px solid #BAE6FD; }
    .badge-ready { background-color: #DCFCE7; color: #166534; border: 1px solid #86EFAC; }

    /* Custom cards */
    .clinical-card {
        background-color: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 8px;
        padding: 1.25rem;
        margin-bottom: 1rem;
        box-shadow: 0 1px 2px 0 rgba(0, 0, 0, 0.04);
    }
    .clinical-card-header {
        font-size: 0.95rem;
        font-weight: 600;
        color: #334155;
        margin-bottom: 0.75rem;
        border-bottom: 1px solid #F1F5F9;
        padding-bottom: 0.5rem;
    }

    /* Buttons */
    .stButton > button {
        background-color: #0F172A;
        color: #FFFFFF;
        font-weight: 500;
        border-radius: 6px;
        padding: 0.5rem 1.25rem;
        border: 1px solid #0F172A;
        transition: all 0.15s ease-in-out;
    }
    .stButton > button:hover {
        background-color: #1E293B;
        border-color: #1E293B;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }

    /* Disclaimer banner */
    .disclaimer-banner {
        background-color: #EFF6FF;
        border: 1px solid #BFDBFE;
        border-left: 4px solid #2563EB;
        padding: 0.75rem 1rem;
        border-radius: 6px;
        font-size: 0.85rem;
        color: #1E40AF;
        margin-bottom: 1.5rem;
    }
</style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# SIDEBAR NAVIGATION & PROTOCOL CONTEXT
# -----------------------------------------------------------------------------
with st.sidebar:
    st.markdown("## 🔒 TrialLock")
    st.caption("Clinical Data Quality & Observability")
    st.markdown("---")

    study_meta = config.STUDY_PROTOCOL
    st.markdown(f"**Study ID:** `{study_meta.get('study_id', 'N/A')}`")
    st.markdown(f"**Protocol:** `{study_meta.get('protocol_version', '1.0.0')}`")
    st.markdown(f"**Quality Threshold:** `{study_meta.get('minimum_quality_score', 95)}%`")

    st.markdown("---")
    page = st.radio(
        "Navigation",
        [
            "Overview",
            "Data Ingestion",
            "Pipeline Runs",
            "Data Quality",
            "Lineage and Audit",
            "Gold Data Explorer",
        ],
        index=0
    )

    st.markdown("---")
    st.markdown("""
    <div style="font-size: 0.75rem; color: #94A3B8; line-height: 1.4;">
        <strong>Prototype Disclaimer:</strong><br>
        Uses synthetic study data (IBD-DEMO-001). Not a validated GxP system. Not for real patient records.
    </div>
    """, unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# PAGE 1: OVERVIEW DASHBOARD
# -----------------------------------------------------------------------------
if page == "Overview":
    st.title("Clinical Data Pipeline Observability")
    st.markdown("""
    <div class="disclaimer-banner">
        <strong>Study IBD-DEMO-001</strong> | Portfolio Demonstration |
        Real-time pipeline monitoring from Bronze raw intake to Gold publication.
    </div>
    """, unsafe_allow_html=True)

    # Fetch latest pipeline execution metrics
    latest = metrics.get_latest_metrics()

    # KPI Top Bar
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        status = latest.get("status", "NOT RUN") if latest else "NOT RUN"
        c1.metric("Pipeline Status", status)
    with c2:
        score = latest.get("quality_score") if latest else None
        score_str = f"{score:.1f}%" if score is not None else "N/A"
        c2.metric("Quality Score", score_str)
    with c3:
        readiness_val = latest.get("readiness_status", "NOT EVALUATED") if latest else "NOT EVALUATED"
        c3.metric("Study Readiness", readiness_val)
    with c4:
        duration = latest.get("duration_sec") if latest else None
        duration_str = f"{duration}s" if duration is not None else "N/A"
        c4.metric("Last Run Duration", duration_str)

    st.markdown("---")

    # Quick Action: Generate Synthetic Data
    col_act1, col_act2 = st.columns([1, 3])
    with col_act1:
        if st.button("▶ Generate Demo Study & Run Pipeline", use_container_width=True):
            with st.spinner("Generating synthetic data and executing PySpark pipeline..."):
                synthetic_data.generate_demo(config.STUDY_PROTOCOL["study_id"])
                res = pipeline.run_pipeline(config.STUDY_PROTOCOL["study_id"])
                if res.get("status") == "COMPLETED":
                    st.success(f"Pipeline completed in {res.get('duration_sec')}s! Quality Score: {res.get('quality_score')}%")
                    st.rerun()
                else:
                    st.error(f"Pipeline failed: {res.get('error')}")

    with col_act2:
        if latest and latest.get("readiness_reason"):
            badge_class = "badge-blocker" if latest.get("readiness_status") == "BLOCKED" else ("badge-high" if latest.get("readiness_status") == "AT RISK" else "badge-ready")
            st.markdown(f"""
            <div style="padding-top: 0.5rem;">
                <span class="badge {badge_class}">{latest.get('readiness_status')}</span>
                <span style="font-size: 0.85rem; color: #475569; margin-left: 0.5rem;">{latest.get('readiness_reason')}</span>
            </div>
            """, unsafe_allow_html=True)

    # Medallion Architecture Row Counts
    st.subheader("Medallion Layer Progression")

    if latest and latest.get("silver_counts"):
        silver_c = latest.get("silver_counts", {})
        gold_c = latest.get("gold_counts", {})
        quarantine_c = latest.get("quarantined_counts", {})

        entities = list(silver_c.keys())

        # Prepare comparative bar chart
        fig = go.Figure()
        fig.add_trace(go.Bar(
            name='Silver (Normalized)',
            x=entities,
            y=[silver_c.get(e, 0) for e in entities],
            marker_color='#64748B'
        ))
        fig.add_trace(go.Bar(
            name='Gold (Curated)',
            x=entities,
            y=[gold_c.get(e, 0) for e in entities],
            marker_color='#0284C7'
        ))
        fig.add_trace(go.Bar(
            name='Quarantine (Rejected)',
            x=entities,
            y=[quarantine_c.get(e, 0) for e in entities],
            marker_color='#EF4444'
        ))

        fig.update_layout(
            barmode='group',
            height=350,
            margin=dict(l=20, r=20, t=30, b=20),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            plot_bgcolor="white",
            paper_bgcolor="white",
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No pipeline run data available. Click 'Generate Demo Study & Run Pipeline' above to initialize data.")

    # Two column layout: Quality Checks & Recent Issues
    c_left, c_right = st.columns(2)

    with c_left:
        st.subheader("Quality Rule Evaluation")
        if latest and latest.get("total_checks"):
            passed = latest.get("passed_checks", 0)
            failed = latest.get("failed_checks", 0)
            total = latest.get("total_checks", 0)

            fig_donut = go.Figure(data=[go.Pie(
                labels=['Passed Checks', 'Failed Checks'],
                values=[passed, failed],
                hole=.6,
                marker_colors=['#10B981', '#EF4444']
            )])
            fig_donut.update_layout(
                height=250,
                margin=dict(l=10, r=10, t=10, b=10),
                annotations=[dict(text=f"{passed}/{total}", x=0.5, y=0.5, font_size=20, showarrow=False)],
                plot_bgcolor="white",
                paper_bgcolor="white",
            )
            st.plotly_chart(fig_donut, use_container_width=True)
        else:
            st.info("No check evaluation history.")

    with c_right:
        st.subheader("Recent Quarantined Violations")
        issues = storage.get_dq_issues(run_id=latest.get("run_id") if latest else None)
        quarantine_issues = [i for i in issues if i.get("is_quarantine") and i.get("record_count", 0) > 0]

        if quarantine_issues:
            for issue in quarantine_issues[:4]:
                st.markdown(f"""
                <div style="background: white; border: 1px solid #E2E8F0; padding: 0.6rem 0.8rem; border-radius: 6px; margin-bottom: 0.5rem;">
                    <span class="badge badge-blocker">{issue['severity']}</span>
                    <strong>{issue['rule_id']}</strong> ({issue['entity_type']})<br>
                    <span style="font-size: 0.8rem; color: #64748B;">{issue['message']}</span>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.success("No active quarantine violations in latest run.")


# -----------------------------------------------------------------------------
# PAGE 2: DATA INGESTION
# -----------------------------------------------------------------------------
elif page == "Data Ingestion":
    st.title("Clinical Data Ingestion")
    st.markdown("Preserve raw source inputs into Bronze layer with SHA-256 integrity verification.")

    tab1, tab2, tab3 = st.tabs(["Synthetic Generator", "File Upload", "REST Connector (Mock)"])

    # Tab 1: Synthetic Generator
    with tab1:
        st.subheader("Generate Study IBD-DEMO-001")
        st.write("Generates deterministic clinical study datasets with realistic clinical anomalies:")
        st.markdown("""
        - **Subjects**: Missing IDs, duplicate keys, consent after randomization
        - **Visits**: Orphan visits referencing non-existent subjects
        - **Lab Results**: Non-numeric values, invalid measurement units
        - **Adverse Events**: Inverted date sequences, non-standard severity codes
        """)

        if st.button("Generate Synthetic Datasets", key="btn_gen_tab"):
            with st.spinner("Generating CSV files in Bronze layer..."):
                gen_data = synthetic_data.generate_demo(config.STUDY_PROTOCOL["study_id"])
                st.success(f"Generated Bronze datasets: {gen_data['meta']}")

                # Show file sizes
                bronze_p = BASE_DIR / "data" / "bronze"
                files = list(bronze_p.glob(f"*_{config.STUDY_PROTOCOL['study_id']}.csv"))
                df_files = pd.DataFrame([{
                    "Filename": f.name,
                    "Size (bytes)": f.stat().st_size,
                    "Modified": datetime.datetime.fromtimestamp(f.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                } for f in files])
                st.dataframe(df_files, use_container_width=True)

    # Tab 2: File Upload
    with tab2:
        st.subheader("Manual Data Intake")
        upload_entity = st.selectbox(
            "Target Entity Type",
            ["subjects", "visits", "lab_results", "adverse_events", "pro_responses"],
            key="upload_entity_select"
        )
        uploaded_file = st.file_uploader("Upload CSV or JSON dataset", type=["csv", "json"])

        if uploaded_file is not None:
            import hashlib
            content = uploaded_file.read()
            checksum = hashlib.sha256(content).hexdigest()
            file_size = len(content)

            st.info(f"File: `{uploaded_file.name}` | Size: `{file_size} bytes` | SHA-256: `{checksum[:16]}...`")

            if st.button("Ingest to Bronze"):
                try:
                    ingest_id = f"upload_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S')}"
                    ingestion.init_db()
                    saved_path = ingestion.save_bronze(content, ingest_id, upload_entity, uploaded_file.name)

                    ingestion.record_ingestion({
                        "ingestion_id": ingest_id,
                        "study_id": config.STUDY_PROTOCOL.get("study_id", "IBD-DEMO-001"),
                        "entity_type": upload_entity,
                        "source_name": uploaded_file.name,
                        "source_type": "upload",
                        "ingestion_timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                        "input_sha256": checksum,
                        "raw_record_count": content.count(b'\n'),
                        "file_size_bytes": file_size,
                        "schema_validation_status": "received",
                        "file_path": saved_path
                    })
                    st.success(f"Successfully preserved to Bronze: `{saved_path}`")
                except Exception as e:
                    st.error(f"Ingestion failed: {e}")

    # Tab 3: REST API Connector
    with tab3:
        st.subheader("EDC / Lab REST Integration")
        st.caption("Configurable REST endpoint ingestion for clinical systems.")

        api_url = st.text_input("Endpoint URL", "https://api.clinical-edc.mock/v1/studies/IBD-DEMO-001/subjects")
        api_token = st.text_input("Bearer Token (optional)", type="password")
        timeout_sec = st.slider("Timeout (seconds)", min_value=5, max_value=60, value=15)

        if st.button("Fetch and Stage to Bronze"):
            if not api_url.startswith(("http://", "https://")):
                st.error("Invalid URL format.")
            else:
                st.info("Portfolio simulation: REST connection tested. In production, this ingests directly into Bronze manifests.")


# -----------------------------------------------------------------------------
# PAGE 3: PIPELINE RUNS
# -----------------------------------------------------------------------------
elif page == "Pipeline Runs":
    st.title("Pipeline Orchestration & Execution History")
    st.markdown("Track and trigger medallion pipeline runs with truthful timing, counts, and diagnostic errors.")

    col_btn, col_info = st.columns([1, 4])
    with col_btn:
        if st.button("▶ Run Full Pipeline", use_container_width=True):
            with st.spinner("Processing Medallion Pipeline with PySpark..."):
                res = pipeline.run_pipeline(config.STUDY_PROTOCOL["study_id"])
                if res.get("status") == "COMPLETED":
                    st.success(f"Run {res.get('run_id')} completed successfully in {res.get('duration_sec')}s.")
                    st.rerun()
                else:
                    st.error(f"Pipeline execution failed: {res.get('error')}")

    # Execution History
    st.subheader("Historical Pipeline Runs")
    runs = metrics.get_all_runs(limit=15)

    if runs:
        df_runs = pd.DataFrame(runs)
        st.dataframe(
            df_runs[[
                "run_id", "status", "quality_score", "readiness_status", "duration_sec", "start_time", "error_message"
            ]],
            use_container_width=True,
            column_config={
                "quality_score": st.column_config.NumberColumn("Score (%)", format="%.2f"),
                "duration_sec": st.column_config.NumberColumn("Duration (s)"),
                "start_time": st.column_config.DatetimeColumn("Started At"),
            }
        )
    else:
        st.info("No pipeline execution records found in database.")


# -----------------------------------------------------------------------------
# PAGE 4: DATA QUALITY
# -----------------------------------------------------------------------------
elif page == "Data Quality":
    st.title("Data Quality Rule Evaluation")
    st.markdown("Inspect record-level quality findings, severity classifications, and quarantine evidence.")

    latest = metrics.get_latest_metrics()
    all_issues = storage.get_dq_issues(run_id=latest.get("run_id") if latest else None)

    # Issue Counts by Severity
    real_issues = [i for i in all_issues if i.get("record_count", 0) > 0]
    blockers = [i for i in real_issues if i.get("severity") == "BLOCKER"]
    highs = [i for i in real_issues if i.get("severity") == "HIGH"]
    mediums = [i for i in real_issues if i.get("severity") == "MEDIUM"]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("BLOCKER Issues", len(blockers))
    c2.metric("HIGH Issues", len(highs))
    c3.metric("MEDIUM Issues", len(mediums))
    c4.metric("Quarantined Rules", sum(1 for i in real_issues if i.get("is_quarantine")))

    st.markdown("---")

    # Filters
    col_f1, col_f2 = st.columns(2)
    with col_f1:
        sev_filter = st.multiselect("Filter by Severity", ["BLOCKER", "HIGH", "MEDIUM", "LOW"], default=["BLOCKER", "HIGH"])
    with col_f2:
        entities = sorted(list({i.get("entity_type") for i in all_issues if i.get("entity_type")}))
        entity_filter = st.multiselect("Filter by Entity", entities, default=entities)

    # Filtered Table
    filtered = [
        i for i in all_issues
        if (not sev_filter or i.get("severity") in sev_filter) and
           (not entity_filter or i.get("entity_type") in entity_filter)
    ]

    if filtered:
        df_dq = pd.DataFrame(filtered)
        st.dataframe(
            df_dq[[
                "severity", "rule_id", "entity_type", "field_name", "record_count", "is_quarantine", "message"
            ]],
            use_container_width=True,
            column_config={
                "record_count": st.column_config.NumberColumn("Affected Records"),
                "is_quarantine": st.column_config.CheckboxColumn("Quarantined"),
            }
        )
    else:
        st.success("No issues match the selected filters.")

    # Quarantined Data Inspector
    st.subheader("Quarantined Records Evidence")
    quarantine_p = BASE_DIR / "data" / "quarantine"

    if quarantine_p.exists():
        q_files = [f for f in quarantine_p.iterdir() if f.is_dir() or f.suffix == ".parquet"]
        if q_files:
            selected_q = st.selectbox("Select Quarantined Dataset", [f.name for f in q_files])
            q_target = quarantine_p / selected_q
            try:
                df_preview = pd.read_parquet(str(q_target))
                st.write(f"Showing {len(df_preview)} quarantined records:")
                st.dataframe(df_preview.head(25), use_container_width=True)
            except Exception as e:
                st.warning(f"Could not read quarantine dataset: {e}")
        else:
            st.info("No quarantine datasets on disk.")


# -----------------------------------------------------------------------------
# PAGE 5: LINEAGE AND AUDIT
# -----------------------------------------------------------------------------
elif page == "Lineage and Audit":
    st.title("Data Lineage & Cryptographic Audit Trail")
    st.markdown("Verify the application-level SHA-256 tamper-evident hash chain and track run provenance.")

    # Audit Chain Verification
    valid_chain, results = audit.verify_audit_chain()

    col_stat, col_btn = st.columns([3, 1])
    with col_stat:
        if valid_chain:
            st.markdown("""
            <div style="background-color: #DCFCE7; border: 1px solid #86EFAC; border-left: 4px solid #16A34A; padding: 0.8rem; border-radius: 6px;">
                <strong style="color: #166534;">✓ AUDIT CHAIN VALID:</strong> Cryptographic integrity verified. No historical audit records have been tampered with.
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div style="background-color: #FEE2E2; border: 1px solid #FCA5A5; border-left: 4px solid #DC2626; padding: 0.8rem; border-radius: 6px;">
                <strong style="color: #991B1B;">⚠ AUDIT CHAIN COMPROMISED:</strong> One or more hash signatures failed verification.
            </div>
            """, unsafe_allow_html=True)

    with col_btn:
        if st.button("Re-verify Chain", use_container_width=True):
            st.rerun()

    st.subheader("Audit Event Trail")
    if results:
        df_audit = pd.DataFrame(results)
        st.dataframe(
            df_audit[["event_id", "type", "match", "curr_hash", "prev_hash"]],
            use_container_width=True,
            column_config={
                "match": st.column_config.CheckboxColumn("Signature Match"),
                "curr_hash": st.column_config.TextColumn("Block Hash"),
                "prev_hash": st.column_config.TextColumn("Previous Hash"),
            }
        )
    else:
        st.info("No audit events recorded.")


# -----------------------------------------------------------------------------
# PAGE 6: GOLD DATA EXPLORER
# -----------------------------------------------------------------------------
elif page == "Gold Data Explorer":
    st.title("Gold Dataset Explorer")
    st.markdown("Inspect, query, and export curated clinical datasets published by the pipeline.")

    gold_p = BASE_DIR / "data" / "gold"
    available_tables = [
        "dim_subjects",
        "fact_visits",
        "fact_lab_results",
        "fact_adverse_events",
        "fact_pro_responses"
    ]

    selected_table = st.selectbox("Select Gold Dataset", available_tables)
    table_path = gold_p / selected_table

    if table_path.exists():
        try:
            df_gold = pd.read_parquet(str(table_path))

            # Table Metadata
            c1, c2, c3 = st.columns(3)
            c1.metric("Total Records", len(df_gold))
            c2.metric("Total Columns", len(df_gold.columns))
            c3.metric("Storage Format", "Parquet")

            # Table Preview
            st.subheader(f"Preview: {selected_table}")
            st.dataframe(df_gold.head(50), use_container_width=True)

            # Export Options
            csv_bytes = df_gold.to_csv(index=False).encode('utf-8')
            st.download_button(
                label=f"⬇ Export {selected_table} to CSV",
                data=csv_bytes,
                file_name=f"{selected_table}_{datetime.datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )

        except Exception as e:
            st.warning(f"Could not read Gold dataset '{selected_table}': {e}")
    else:
        st.info(f"Dataset `{selected_table}` has not been published yet. Run the pipeline first.")
