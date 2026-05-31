# app.py — Healthcare Data Platform
# Modules: Patient Portal + Doctor Dashboard + AI Insights

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from databricks import sql
from datetime import datetime, timezone
import uuid
import os
import streamlit as st

# نحاول استيراد dbutils، إذا فشل (محلياً)، نستخدم نسخة وهمية أو نتخطاها
try:
    from databricks.sdk.runtime import dbutils
    # إذا نجح الاستيراد، نحن داخل Databricks
    is_databricks = True
except ImportError:
    # إذا فشل، نحن نعمل محلياً
    is_databricks = False
    dbutils = None

# لاحقاً في الكود، استخدمي المتغير للتحقق
if is_databricks:
    # استخدمي dbutils هنا إذا لزم الأمر
    pass
else:
    # الكود الخاص بالعمل المحلي
    st.warning(" Databricks is not available We are working locally ")

# ── Warm up warehouse connection at app start ──────────────────────────────────
@st.cache_resource(show_spinner=False)
def warm_up_connection():
    """
    Runs once when the app starts.
    Wakes up the SQL Warehouse so user queries are instant.
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return True
    except Exception:
        return False

# Call at startup — cached so it only runs once per app session
_warmed_up = warm_up_connection()
port = int(os.environ.get("STREAMLIT_SERVER_PORT", 8501))

from utils.ingestion_utils import (
    validate_national_id,
    ingest_csv_streamlit,
    register_patient,
    load_ingestion_stats,
    load_rejected_records,
)
from utils.ml_utils import (
    load_risk_predictions,
    load_anomalies,
    load_model_metrics,
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
    from databricks import sql
    import os

    host      = os.environ.get("DATABRICKS_HOST")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH")
    token     = os.environ.get("DATABRICKS_TOKEN")

    if not host or not http_path:
        try:
            import streamlit as st
            host      = st.secrets["databricks"]["server_hostname"]
            http_path = st.secrets["databricks"]["http_path"]
            token     = st.secrets["databricks"]["access_token"]
        except Exception:
            raise RuntimeError(
                "Missing DATABRICKS_HOST and DATABRICKS_HTTP_PATH. "
                "Set them in app.yaml environment variables."
            )

    connect_args = {
        "server_hostname": host,
        "http_path":       http_path,
        "_socket_timeout": 30,
    }
    if token:
        connect_args["access_token"] = token

    return sql.connect(**connect_args)
# ── Cached queries ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner=False)
def load_doctor_dashboard() -> pd.DataFrame:
    """
    Fetches the doctor dashboard data from the database.
    Includes error handling to identify connectivity issues.
    """
    try:
        # Establish connection using the custom connection utility
        with get_connection() as conn:
            with conn.cursor() as cur:
                # Execute the query for the doctor dashboard view
                cur.execute("SELECT * FROM healthcare_platform.vw_doctor_dashboard")
                
                # Fetch results and get column names
                data = cur.fetchall()
                columns = [d[0] for d in cur.description]
                
                # Return data as a pandas DataFrame
                return pd.DataFrame(data, columns=columns)
                
    except Exception as e:
        # Log the error to the console for debugging
        print(f"DEBUG ERROR: Failed to fetch dashboard data: {e}")
        # Show a user-friendly error message in the Streamlit UI
        st.error(f"❌ Could not load dashboard data: {e}")
        # Return an empty DataFrame to prevent app crash
        return pd.DataFrame()

# Usage in app.py
with st.spinner("Fetching dashboard data…"):
    df = load_doctor_dashboard()
    
# Display data if successfully loaded
if not df.empty:
    st.dataframe(df)
else:
    st.warning("No data available or connection issue occurred.")

@st.cache_data(ttl=300, show_spinner="Loading vitals…")
def load_vitals_timeseries(patient_id: str | None = None) -> pd.DataFrame:
    with get_connection() as conn:
        with conn.cursor() as cur:
            query = """
                SELECT * FROM
                healthcare_platform.vw_patient_vitals_timeseries
            """
            if patient_id:
                query += f" WHERE patient_id = '{patient_id}'"
                query += " ORDER BY recorded_at"
            cur.execute(query)
            return pd.DataFrame(
                cur.fetchall(),
                columns=[d[0] for d in cur.description]
            )

@st.cache_data(ttl=30, show_spinner=False)
def patient_exists(patient_id: str) -> bool:
    """
    Fast existence check — returns True/False within seconds.
    Uses COUNT which is optimised on Delta tables.
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT COUNT(1) AS n
                    FROM healthcare_platform.patients
                    WHERE patient_id = '{patient_id}'
                """)
                result = cur.fetchone()
                return result[0] > 0
    except Exception as e:
        st.error(f"Connection error: {e}")
        return False

def insert_vitals(patient_id: str, vitals: dict) -> bool:
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO healthcare_platform.vitals
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

# ── Quality gate queries ───────────────────────────────────────────────────────
@st.cache_data(ttl=120, show_spinner=False)
def load_quality_health() -> pd.DataFrame:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM healthcare_platform.vw_quality_health
                LIMIT 10
            """)
            return pd.DataFrame(
                cur.fetchall(),
                columns=[d[0] for d in cur.description]
            )

@st.cache_data(ttl=120, show_spinner=False)
def load_latest_quality() -> pd.DataFrame:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    layer,
                    rule_name,
                    check_type,
                    records_checked,
                    records_failed,
                    failure_rate_pct,
                    status,
                    message,
                    checked_at
                FROM healthcare_platform.vw_latest_quality
                ORDER BY
                    CASE status
                        WHEN 'FAIL'  THEN 1
                        WHEN 'ERROR' THEN 2
                        WHEN 'WARN'  THEN 3
                        ELSE 4
                    END
            """)
            return pd.DataFrame(
                cur.fetchall(),
                columns=[d[0] for d in cur.description]
            )

# ── Authentication ─────────────────────────────────────────────────────────────
def authenticate(role: str, password: str) -> bool:
    """
    Checks role password against environment variable (cloud)
    or st.secrets (local). Works in both environments.
    """
    # Cloud: passwords injected as env vars by Databricks Apps
    env_key  = f"{role.upper()}_PASSWORD"
    expected = os.environ.get(env_key)

    # Local fallback
    if not expected:
        try:
            import streamlit as st
            expected = st.secrets["roles"].get(role)
        except Exception:
            return False

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

    # ── Tab 1: Submit Vitals ───────────────────────────────────────────────────
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
                        st.success("✅ Vitals submitted successfully!")
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
                reg_first_name = st.text_input(
                    "First Name *",
                    placeholder="Ahmed"
                )
                reg_last_name = st.text_input(
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
                with st.spinner("Checking registration…"):
                    try:
                        already_exists = patient_exists(
                            reg_national_id.strip()
                        )
                    except Exception as e:
                        st.error(f"Database error: {e}")
                        st.stop()

                if already_exists:
                    st.warning(
                        "⚠️ A patient with this National ID "
                        "is already registered."
                    )
                else:
                    patient_data = {
                        "national_id":   reg_national_id.strip(),
                        "first_name":    reg_first_name.strip(),
                        "last_name":     reg_last_name.strip(),
                        "date_of_birth": str(reg_dob),
                        "gender":        reg_gender,
                        "blood_type":    reg_blood,
                        "contact_email": reg_email.strip(),
                    }
                    with st.spinner("Registering…"):
                        try:
                            register_patient(patient_data)
                            st.success(
                                f"✅ **{reg_first_name} {reg_last_name}** "
                                f"registered successfully!\n\n"
                                f"Patient ID: `{reg_national_id.strip()}`"
                            )
                            st.balloons()
                        except Exception as e:
                            st.error(f"Registration failed: {e}")

    # ── Tab 3: Bulk CSV Upload ─────────────────────────────────────────────────
    with tab_upload:
        st.subheader("Bulk Patient Upload via CSV")

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
            preview_df = pd.read_csv(
                uploaded_file, dtype=str, nrows=5
            )
            uploaded_file.seek(0)

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

                        st.success("✅ Ingestion complete!")
                        col1, col2, col3 = st.columns(3)
                        col1.metric("Total Rows",  result["rows_total"])
                        col2.metric("✅ Loaded",   result["rows_loaded"])
                        col3.metric("❌ Rejected", result["rows_rejected"])

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
                        load_ingestion_stats.clear()

                    except Exception as e:
                        st.error(f"Ingestion failed: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# DOCTOR ROLE
# ══════════════════════════════════════════════════════════════════════════════
elif st.session_state.role == "doctor":
    st.title("Clinical Dashboard")

    tab_dashboard, tab_vitals_drill, tab_monitor, tab_quality, tab_ai = st.tabs([
        "📊 Dashboard",
        "🩺 Patient Vitals",
        "🔍 Ingestion Monitor",
        "🏥 Quality Gates",
        "🤖 AI Insights"
    ])

    # ── Tab 1: Main Dashboard ──────────────────────────────────────────────────
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
            "HIGH":   "#E24B4A",
            "MEDIUM": "#EF9F27",
            "LOW":    "#1D9E75"
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
                showlegend=True, margin=dict(t=20, b=10)
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
                showlegend=False, margin=dict(t=10, b=10)
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

    # ── Tab 3: Ingestion Monitor ───────────────────────────────────────────────
    with tab_monitor:
        st.subheader("Ingestion Monitor")

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
            st.markdown("**Source Status**")
            st.dataframe(
                stats_df.rename(columns={
                    "source_name":         "Source",
                    "file_format":         "Format",
                    "total_rows_loaded":   "Loaded",
                    "total_rows_rejected": "Rejected",
                    "last_file_loaded":    "Last File",
                    "last_ingested_at":    "Last Run",
                    "is_active":           "Active"
                }),
                use_container_width=True,
                hide_index=True
            )

            st.divider()
            st.markdown("**Rejected Records**")

            source_options  = ["ALL"] + stats_df["source_name"].tolist()
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
                st.download_button(
                    label="⬇️ Export Rejected Records",
                    data=rejected_df.to_csv(index=False),
                    file_name=f"rejected_{selected_source}.csv",
                    mime="text/csv"
                )

        if st.button("🔄 Refresh Monitor"):
            load_ingestion_stats.clear()
            load_rejected_records.clear()
            st.rerun()

    # ── Tab 4: Quality Gates ───────────────────────────────────────────────────
    with tab_quality:
        st.subheader("Data Quality Gates")

        health_df = load_quality_health()

        if not health_df.empty:
            latest = health_df.iloc[0]
            status_color = {
                "PASSED":  "green",
                "WARNING": "orange",
                "FAILED":  "red",
                "ERROR":   "red"
            }.get(latest["overall_status"], "gray")

            st.markdown(
                f"**Latest run:** `{latest['run_id']}` — "
                f":{status_color}[{latest['overall_status']}]"
            )

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Checks", int(latest["total_checks"]))
            col2.metric("✅ Passed",    int(latest["passed"]))
            col3.metric("⚠️ Warned",    int(latest["warned"]))
            col4.metric("❌ Failed",    int(latest["failed"]))

            st.divider()
            st.markdown("**Latest result per rule:**")

            rules_df = load_latest_quality()

            def color_status(val):
                colors = {
                    "PASS":  "background-color: #d4edda; color: #155724",
                    "WARN":  "background-color: #fff3cd; color: #856404",
                    "FAIL":  "background-color: #f8d7da; color: #721c24",
                    "ERROR": "background-color: #f8d7da; color: #721c24",
                }
                return colors.get(val, "")

            styled = rules_df.style.map(
                color_status, subset=["status"]
            )
            st.dataframe(
                styled, use_container_width=True, hide_index=True
            )

            st.divider()
            st.markdown("**Run history (last 10):**")
            st.dataframe(
                health_df.rename(columns={
                    "run_id":         "Run ID",
                    "run_started_at": "Started At",
                    "total_checks":   "Checks",
                    "passed":         "Pass",
                    "warned":         "Warn",
                    "failed":         "Fail",
                    "overall_status": "Status"
                }),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info(
                "No quality check results yet. "
                "Run notebook 04_data_quality_gates.py first."
            )

        if st.button("🔄 Refresh Quality Data"):
            load_quality_health.clear()
            load_latest_quality.clear()
            st.rerun()

    # ── Tab 5: AI Insights ─────────────────────────────────────────────────────
    with tab_ai:
        st.subheader("AI-Powered Health Insights")

        ai_tab1, ai_tab2, ai_tab3 = st.tabs([
            "🎯 Risk Predictions",
            "🚨 Anomaly Alerts",
            "📈 Model Performance"
        ])

        # ── Risk Predictions ───────────────────────────────────────────────────
        with ai_tab1:
            predictions_df = load_risk_predictions()

            if predictions_df.empty:
                st.info(
                    "No predictions yet. "
                    "Run notebook 05c_score_patients.py first."
                )
            else:
                col1, col2, col3 = st.columns(3)
                col1.metric(
                    "🔴 High Risk",
                    len(predictions_df[
                        predictions_df["predicted_risk"] == "HIGH"
                    ])
                )
                col2.metric(
                    "🟡 Medium Risk",
                    len(predictions_df[
                        predictions_df["predicted_risk"] == "MEDIUM"
                    ])
                )
                col3.metric(
                    "🟢 Low Risk",
                    len(predictions_df[
                        predictions_df["predicted_risk"] == "LOW"
                    ])
                )

                st.divider()

                fig = px.bar(
                    predictions_df.head(20),
                    x="full_name",
                    y=["pct_high", "pct_medium", "pct_low"],
                    title="Risk Prediction Confidence per Patient",
                    labels={"value": "Confidence %", "full_name": ""},
                    color_discrete_map={
                        "pct_high":   "#E24B4A",
                        "pct_medium": "#EF9F27",
                        "pct_low":    "#1D9E75"
                    },
                    barmode="stack"
                )
                fig.update_layout(
                    xaxis_tickangle=-45,
                    legend_title="Risk Level"
                )
                st.plotly_chart(fig, use_container_width=True)

                st.dataframe(
                    predictions_df[[
                        "full_name", "predicted_risk",
                        "pct_high", "pct_medium", "pct_low",
                        "age_years", "gender", "scored_at"
                    ]].rename(columns={
                        "full_name":      "Patient",
                        "predicted_risk": "Predicted Risk",
                        "pct_high":       "High %",
                        "pct_medium":     "Medium %",
                        "pct_low":        "Low %",
                        "age_years":      "Age",
                        "gender":         "Gender",
                        "scored_at":      "Scored At"
                    }),
                    use_container_width=True,
                    hide_index=True
                )

        # ── Anomaly Alerts ─────────────────────────────────────────────────────
        with ai_tab2:
            severity_filter = st.selectbox(
                "Filter by severity:",
                ["ALL", "CRITICAL", "WARNING"]
            )

            anomalies_df = load_anomalies(severity_filter)

            if anomalies_df.empty:
                st.success(
                    "✅ No anomalies detected. "
                    "Run notebook 05b_anomaly_detection.py first."
                )
            else:
                critical_count = len(
                    anomalies_df[anomalies_df["severity"] == "CRITICAL"]
                )
                warning_count = len(
                    anomalies_df[anomalies_df["severity"] == "WARNING"]
                )

                col1, col2 = st.columns(2)
                col1.metric("🚨 Critical", critical_count)
                col2.metric("⚠️ Warning",  warning_count)

                st.divider()

                def highlight_severity(row):
                    if row["severity"] == "CRITICAL":
                        return ["background-color: #f8d7da"] * len(row)
                    elif row["severity"] == "WARNING":
                        return ["background-color: #fff3cd"] * len(row)
                    return [""] * len(row)

                styled = anomalies_df.style.apply(
                    highlight_severity, axis=1
                )
                st.dataframe(
                    styled,
                    use_container_width=True,
                    hide_index=True
                )

                st.download_button(
                    "⬇️ Export Anomalies",
                    data=anomalies_df.to_csv(index=False),
                    file_name="anomaly_alerts.csv",
                    mime="text/csv"
                )

        # ── Model Performance ──────────────────────────────────────────────────
        with ai_tab3:
            metrics_df = load_model_metrics()

            if metrics_df.empty:
                st.info(
                    "No model metrics yet. "
                    "Run notebook 05a_train_risk_model.py first."
                )
            else:
                latest = metrics_df.iloc[0]
                col1, col2, col3 = st.columns(3)
                col1.metric("Accuracy",  f"{latest['accuracy_pct']}%")
                col2.metric("F1 Score",  f"{latest['f1_pct']}%")
                col3.metric(
                    "Training Rows",
                    f"{int(latest['training_rows']):,}"
                )

                st.divider()
                st.markdown("**Model training history:**")
                st.dataframe(
                    metrics_df,
                    use_container_width=True,
                    hide_index=True
                )

        if st.button("🔄 Refresh AI Data"):
            load_risk_predictions.clear()
            load_anomalies.clear()
            load_model_metrics.clear()
            st.rerun()