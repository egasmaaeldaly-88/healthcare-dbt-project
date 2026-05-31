# app.py — Healthcare Data Platform
# Extends Module D with: Patient Registration, Bulk CSV Upload, Ingestion Monitor

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from databricks import sql
from datetime import datetime, timezone
import uuid

from utils.ingestion_utils import (
    validate_national_id,
    ingest_csv_streamlit,
    register_patient,
    load_ingestion_stats,
    load_rejected_records,
)

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Healthcare Platform",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Database connection ────────────────────────────────────────────────────────
def get_connection():
    return sql.connect(
        server_hostname=st.secrets["databricks"]["server_hostname"],
        http_path=st.secrets["databricks"]["http_path"],
        access_token=st.secrets["databricks"]["access_token"],
    )

# ── Cached queries (from Module D) ────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner="Fetching dashboard data…")
def load_doctor_dashboard() -> pd.DataFrame:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM workspace.healthcare_platform.vw_doctor_dashboard"
            )
            return pd.DataFrame(
                cur.fetchall(),
                columns=[d[0] for d in cur.description]
            )

@st.cache_data(ttl=300, show_spinner="Loading vitals…")
def load_vitals_timeseries(patient_id: str | None = None) -> pd.DataFrame:
    with get_connection() as conn:
        with conn.cursor() as cur:
            query = """
                SELECT * FROM
                workspace.healthcare_platform.vw_patient_vitals_timeseries
            """
            if patient_id:
                query += f" WHERE patient_id = '{patient_id}'"
                query += " ORDER BY recorded_at"
            cur.execute(query)
            return pd.DataFrame(
                cur.fetchall(),
                columns=[d[0] for d in cur.description]
            )

@st.cache_data(ttl=60)
def patient_exists(patient_id: str) -> bool:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT 1 FROM workspace.healthcare_platform.patients
                WHERE patient_id = '{patient_id}' LIMIT 1
            """)
            return cur.fetchone() is not None

def insert_vitals(patient_id: str, vitals: dict) -> bool:
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO workspace.healthcare_platform.vitals
                        (vital_id, patient_id, recorded_at,
                         systolic_bp, diastolic_bp, heart_rate,
                         temperature_c, spo2_pct, weight_kg,
                         source_system)
                    VALUES (
                        '{str(uuid.uuid4())}',
                        '{patient_id}',
                        '{datetime.now(timezone.utc).isoformat()}',
                        {vitals['systolic_bp']},
                        {vitals['diastolic_bp']},
                        {vitals['heart_rate']},
                        {vitals['temperature_c']},
                        {vitals['spo2_pct']},
                        {vitals['weight_kg']},
                        'streamlit_patient_portal'
                    )
                """)
        load_vitals_timeseries.clear()
        return True
    except Exception as e:
        st.error(f"Insert failed: {e}")
        return False

# ── Authentication ─────────────────────────────────────────────────────────────
def authenticate(role: str, password: str) -> bool:
    expected = st.secrets["roles"].get(role)
    return expected is not None and password == expected

# ── Sidebar ────────────────────────────────────────────────────────────────────
st.sidebar.image(
    "https://img.icons8.com/fluency/96/caduceus.png", width=64
)
st.sidebar.title("Healthcare Platform")
st.sidebar.divider()

role     = st.sidebar.selectbox("I am a:", ["Patient", "Doctor"])
password = st.sidebar.text_input("Access code", type="password")
login    = st.sidebar.button("Enter")

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
    st.session_state.role          = None

if login:
    role_key = role.lower()
    if authenticate(role_key, password):
        st.session_state.authenticated = True
        st.session_state.role          = role_key
        st.sidebar.success(f"Logged in as {role}")
    else:
        st.sidebar.error("Incorrect access code")

if not st.session_state.authenticated:
    st.title("🏥 Healthcare Data Platform")
    st.info(
        "Please select your role and enter your "
        "access code in the sidebar."
    )
    st.stop()

# ══════════════════════════════════════════════════════════════════════════════
# PATIENT ROLE
# ══════════════════════════════════════════════════════════════════════════════
if st.session_state.role == "patient":
    st.title("Patient Portal")

    tab_vitals, tab_register, tab_upload = st.tabs([
        "📋 Submit Vitals",
        "🆕 Register",
        "📤 Bulk Upload"
    ])

    # ── Tab 1: Submit Vitals (original Module D) ───────────────────────────────
    with tab_vitals:
        st.subheader("Submit Your Daily Vitals")

        with st.form("vitals_form"):
            patient_id = st.text_input(
                "Patient ID",
                placeholder="Enter your 14-digit National ID"
            )
            st.divider()
            col1, col2, col3 = st.columns(3)

            with col1:
                systolic  = st.number_input(
                    "Systolic BP (mmHg)",  60,  250, 120
                )
                diastolic = st.number_input(
                    "Diastolic BP (mmHg)", 40,  150,  80
                )
            with col2:
                heart_rate  = st.number_input(
                    "Heart Rate (bpm)", 30, 220, 72
                )
                temperature = st.number_input(
                    "Temperature (°C)", 34.0, 43.0, 37.0, step=0.1
                )
            with col3:
                spo2   = st.number_input(
                    "SpO₂ (%)", 70.0, 100.0, 98.0, step=0.1
                )
                weight = st.number_input(
                    "Weight (kg)", 20.0, 300.0, 70.0, step=0.5
                )

            submitted = st.form_submit_button(
                "Submit Readings", type="primary"
            )

        if submitted:
            is_valid, reason = validate_national_id(patient_id.strip())
            if not is_valid:
                st.error(reason)
            elif systolic <= diastolic:
                st.error(
                    "Systolic BP must be greater than Diastolic BP."
                )
            else:
                with st.spinner("Verifying patient…"):
                    exists = patient_exists(patient_id.strip())

                if not exists:
                    st.error(
                        "Patient ID not found. "
                        "Please register first or check with your clinic."
                    )
                else:
                    vitals = {
                        "systolic_bp":   systolic,
                        "diastolic_bp":  diastolic,
                        "heart_rate":    heart_rate,
                        "temperature_c": temperature,
                        "spo2_pct":      spo2,
                        "weight_kg":     weight,
                    }
                    with st.spinner("Saving your readings…"):
                        success = insert_vitals(
                            patient_id.strip(), vitals
                        )
                    if success:
                        st.success(
                            "✅ Vitals submitted successfully!"
                        )
                        st.balloons()

    # ── Tab 2: Patient Registration ────────────────────────────────────────────
    with tab_register:
        st.subheader("New Patient Registration")
        st.info(
            "Register yourself to get access to the patient portal. "
            "Your National ID must be exactly 14 digits."
        )

        with st.form("registration_form"):
            col1, col2 = st.columns(2)

            with col1:
                reg_national_id = st.text_input(
                    "National ID *",
                    placeholder="14-digit number",
                    max_chars=14
                )
                reg_first_name  = st.text_input(
                    "First Name *",
                    placeholder="Ahmed"
                )
                reg_last_name   = st.text_input(
                    "Last Name *",
                    placeholder="Hassan"
                )
                reg_dob = st.date_input(
                    "Date of Birth *",
                    min_value=datetime(1900, 1, 1),
                    max_value=datetime.today()
                )

            with col2:
                reg_gender = st.selectbox(
                    "Gender *",
                    ["M", "F", "UNSPECIFIED"]
                )
                reg_blood = st.selectbox(
                    "Blood Type",
                    ["A+", "A-", "B+", "B-",
                     "AB+", "AB-", "O+", "O-", "UNKNOWN"]
                )
                reg_email = st.text_input(
                    "Contact Email",
                    placeholder="your@email.com"
                )

            st.divider()
            register_btn = st.form_submit_button(
                "Register Patient", type="primary"
            )

        if register_btn:
            # Validate inputs
            errors = []

            id_valid, id_reason = validate_national_id(
                reg_national_id.strip()
            )
            if not id_valid:
                errors.append(id_reason)
            if not reg_first_name.strip():
                errors.append("First name is required.")
            if not reg_last_name.strip():
                errors.append("Last name is required.")

            if errors:
                for err in errors:
                    st.error(err)
            else:
                # Check for duplicate
                with st.spinner("Checking for existing registration…"):
                    already_exists = patient_exists(
                        reg_national_id.strip()
                    )

                if already_exists:
                    st.warning(
                        "⚠️ A patient with this National ID "
                        "is already registered."
                    )
                else:
                    patient_data = {
                        "national_id":  reg_national_id.strip(),
                        "first_name":   reg_first_name.strip(),
                        "last_name":    reg_last_name.strip(),
                        "date_of_birth": str(reg_dob),
                        "gender":       reg_gender,
                        "blood_type":   reg_blood,
                        "contact_email": reg_email.strip(),
                    }
                    with st.spinner("Registering patient…"):
                        try:
                            register_patient(patient_data)
                            patient_exists.clear()
                            st.success(
                                f"✅ Patient **{reg_first_name} "
                                f"{reg_last_name}** registered "
                                f"successfully!\n\n"
                                f"Your Patient ID is: "
                                f"`{reg_national_id.strip()}`"
                            )
                            st.balloons()
                        except Exception as e:
                            st.error(f"Registration failed: {e}")

    # ── Tab 3: Bulk CSV Upload ─────────────────────────────────────────────────
    with tab_upload:
        st.subheader("Bulk Patient Upload via CSV")

        # Template download
        st.markdown("**Step 1 — Download the template**")
        template_df = pd.DataFrame(columns=[
            "national_id", "first_name", "last_name",
            "date_of_birth", "gender", "blood_type", "contact_email"
        ])
        st.download_button(
            label="⬇️ Download CSV Template",
            data=template_df.to_csv(index=False),
            file_name="patient_upload_template.csv",
            mime="text/csv"
        )

        st.divider()
        st.markdown("**Step 2 — Upload your completed CSV**")

        uploaded_file = st.file_uploader(
            "Choose a CSV file",
            type=["csv"],
            help="File must contain: national_id, first_name, "
                 "last_name, date_of_birth, gender"
        )

        if uploaded_file:
            # Preview
            preview_df = pd.read_csv(uploaded_file, dtype=str, nrows=5)
            uploaded_file.seek(0)  # reset after preview read

            st.markdown("**Preview — first 5 rows:**")
            st.dataframe(preview_df, use_container_width=True)

            st.markdown(
                f"**Detected:** `{uploaded_file.name}` — "
                f"`{uploaded_file.size / 1024:.1f} KB`"
            )

            st.divider()
            st.markdown("**Step 3 — Run ingestion**")

            if st.button("🚀 Start Ingestion", type="primary"):
                with st.spinner(
                    "Running ingestion — validating National IDs…"
                ):
                    try:
                        result = ingest_csv_streamlit(
                            uploaded_file,
                            source_name="patients_csv"
                        )

                        # Results summary
                        st.success("✅ Ingestion complete!")
                        col1, col2, col3 = st.columns(3)
                        col1.metric(
                            "Total Rows",
                            result["rows_total"]
                        )
                        col2.metric(
                            "✅ Loaded",
                            result["rows_loaded"],
                            delta=None
                        )
                        col3.metric(
                            "❌ Rejected",
                            result["rows_rejected"],
                            delta=None,
                            delta_color="inverse"
                        )

                        # Show rejected rows inline
                        if result["rows_rejected"] > 0:
                            st.warning(
                                f"⚠️ {result['rows_rejected']} rows "
                                f"were rejected. See details below:"
                            )
                            st.dataframe(
                                result["rejected_df"][[
                                    "row_number",
                                    "national_id_value",
                                    "rejection_reason"
                                ]],
                                use_container_width=True
                            )

                        # Invalidate ingestion stats cache
                        load_ingestion_stats.clear()

                    except Exception as e:
                        st.error(f"Ingestion failed: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# DOCTOR ROLE
# ══════════════════════════════════════════════════════════════════════════════
elif st.session_state.role == "doctor":
    st.title("Clinical Dashboard")

    tab_dashboard, tab_vitals_drill, tab_monitor = st.tabs([
        "📊 Dashboard",
        "🩺 Patient Vitals",
        "🔍 Ingestion Monitor"
    ])

    # ── Tab 1: Main Dashboard (original Module D) ──────────────────────────────
    with tab_dashboard:
        df = load_doctor_dashboard()

        if df.empty:
            st.warning("No patient data. Run the dbt pipeline first.")
            st.stop()

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Patients",  len(df))
        col2.metric("High Risk",
                    len(df[df["risk_level"] == "HIGH"]))
        col3.metric("Avg Systolic BP",
                    f"{df['avg_systolic_bp'].mean():.0f} mmHg")
        col4.metric("Avg SpO₂",
                    f"{df['avg_spo2_pct'].mean():.1f}%")

        st.divider()
        color_map = {
            "HIGH": "#E24B4A",
            "MEDIUM": "#EF9F27",
            "LOW": "#1D9E75"
        }
        col_left, col_right = st.columns([1, 2])

        with col_left:
            st.subheader("Risk Distribution")
            risk_counts = df["risk_level"].value_counts().reset_index()
            risk_counts.columns = ["Risk Level", "Count"]
            fig_pie = px.pie(
                risk_counts,
                values="Count",
                names="Risk Level",
                color="Risk Level",
                color_discrete_map=color_map,
                hole=0.45
            )
            fig_pie.update_layout(
                showlegend=True,
                margin=dict(t=20, b=10)
            )
            st.plotly_chart(fig_pie, use_container_width=True)

        with col_right:
            st.subheader("Patient Risk Scores")
            fig_bar = px.bar(
                df.sort_values(
                    "composite_risk_score", ascending=True
                ).tail(20),
                x="composite_risk_score",
                y="full_name",
                color="risk_level",
                color_discrete_map=color_map,
                orientation="h",
                labels={
                    "composite_risk_score": "Risk Score",
                    "full_name": ""
                },
            )
            fig_bar.update_layout(
                showlegend=False,
                margin=dict(t=10, b=10)
            )
            st.plotly_chart(fig_bar, use_container_width=True)

        st.divider()
        risk_filter = st.multiselect(
            "Filter by risk level:",
            ["HIGH", "MEDIUM", "LOW"],
            default=["HIGH", "MEDIUM", "LOW"]
        )
        filtered = df[df["risk_level"].isin(risk_filter)]
        st.dataframe(
            filtered[[
                "full_name", "age_years", "gender", "blood_type",
                "risk_level", "composite_risk_score",
                "avg_systolic_bp", "avg_diastolic_bp",
                "avg_heart_rate", "avg_spo2_pct",
                "active_medications", "last_reading_at"
            ]],
            use_container_width=True,
            hide_index=True
        )

        if st.button("🔄 Refresh Dashboard"):
            load_doctor_dashboard.clear()
            st.rerun()

    # ── Tab 2: Patient Vitals Drill-Down ───────────────────────────────────────
    with tab_vitals_drill:
        df = load_doctor_dashboard()

        if not df.empty:
            selected_patient = st.selectbox(
                "Select a patient:",
                options=df["patient_id"].tolist(),
                format_func=lambda pid: df[
                    df["patient_id"] == pid
                ]["full_name"].values[0]
            )

            if selected_patient:
                vitals_df = load_vitals_timeseries(selected_patient)

                if vitals_df.empty:
                    st.info("No vitals recorded for this patient.")
                else:
                    fig_bp = go.Figure()
                    fig_bp.add_trace(go.Scatter(
                        x=vitals_df["recorded_at"],
                        y=vitals_df["systolic_bp"],
                        name="Systolic",
                        line=dict(color="#E24B4A", width=2)
                    ))
                    fig_bp.add_trace(go.Scatter(
                        x=vitals_df["recorded_at"],
                        y=vitals_df["diastolic_bp"],
                        name="Diastolic",
                        line=dict(color="#378ADD", width=2)
                    ))
                    fig_bp.update_layout(
                        title="Blood Pressure Over Time",
                        margin=dict(t=40, b=20)
                    )
                    st.plotly_chart(
                        fig_bp, use_container_width=True
                    )

                    col_a, col_b = st.columns(2)
                    with col_a:
                        fig_spo2 = px.line(
                            vitals_df,
                            x="recorded_at",
                            y="spo2_pct",
                            title="SpO₂ (%)"
                        )
                        fig_spo2.add_hline(
                            y=94,
                            line_dash="dash",
                            line_color="#E24B4A",
                            annotation_text="Warning"
                        )
                        st.plotly_chart(
                            fig_spo2, use_container_width=True
                        )
                    with col_b:
                        fig_hr = px.line(
                            vitals_df,
                            x="recorded_at",
                            y="heart_rate",
                            title="Heart Rate (bpm)"
                        )
                        st.plotly_chart(
                            fig_hr, use_container_width=True
                        )

    # ── Tab 3: Ingestion Monitor (NEW) ─────────────────────────────────────────
    with tab_monitor:
        st.subheader("Ingestion Monitor")

        # ── KPI row ────────────────────────────────────────────────────────────
        stats_df = load_ingestion_stats()

        if not stats_df.empty:
            total_loaded   = stats_df["total_rows_loaded"].sum()
            total_rejected = stats_df["total_rows_rejected"].sum()
            rejection_rate = (
                total_rejected / (total_loaded + total_rejected) * 100
                if (total_loaded + total_rejected) > 0 else 0
            )

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Active Sources",
                        len(stats_df[stats_df["is_active"] == True]))
            col2.metric("Total Rows Loaded",   f"{total_loaded:,}")
            col3.metric("Total Rows Rejected", f"{total_rejected:,}")
            col4.metric("Rejection Rate",
                        f"{rejection_rate:.1f}%")

            st.divider()

            # ── Source stats table ─────────────────────────────────────────────
            st.markdown("**Source Status**")
            st.dataframe(
                stats_df.rename(columns={
                    "source_name":      "Source",
                    "file_format":      "Format",
                    "total_rows_loaded":   "Loaded",
                    "total_rows_rejected": "Rejected",
                    "last_file_loaded": "Last File",
                    "last_ingested_at": "Last Run",
                    "is_active":        "Active"
                }),
                use_container_width=True,
                hide_index=True
            )

            st.divider()

            # ── Rejected records browser ───────────────────────────────────────
            st.markdown("**Rejected Records**")

            source_options = ["ALL"] + stats_df[
                "source_name"
            ].tolist()
            selected_source = st.selectbox(
                "Filter by source:", source_options
            )

            rejected_df = load_rejected_records(selected_source)

            if rejected_df.empty:
                st.success("No rejected records found.")
            else:
                st.warning(
                    f"{len(rejected_df)} rejected record(s) found."
                )
                st.dataframe(
                    rejected_df,
                    use_container_width=True,
                    hide_index=True
                )

                # Export rejected records
                st.download_button(
                    label="⬇️ Export Rejected Records",
                    data=rejected_df.to_csv(index=False),
                    file_name=f"rejected_records_{selected_source}.csv",
                    mime="text/csv"
                )

        if st.button("🔄 Refresh Monitor"):
            load_ingestion_stats.clear()
            load_rejected_records.clear()
            st.rerun()