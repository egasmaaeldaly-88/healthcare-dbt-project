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
import re
import streamlit as st
from utils.diagnostics_utils import (
    DIAGNOSTIC_TYPES,
    RESULT_STATUSES,
    encode_file_to_base64,
    decode_base64_to_bytes, 
    insert_diagnostic,
    insert_lab_values,
    load_patient_diagnostics,
    load_all_diagnostics,
    load_lab_results,
    load_file_content,
    load_diagnostic_stats,
)


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
    load_all_patients,
    insert_surgery,
    get_patient_history,
    _build_medical_history,
    
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
@st.cache_data(ttl=300, show_spinner="Fetching dashboard data…")
def load_doctor_dashboard() -> pd.DataFrame:
    with get_connection() as conn:
        with conn.cursor() as cur:
            # نقوم بعمل JOIN لجلب الـ national_id من جدول المرضى
            query = """
                SELECT g.*, p.national_id 
                FROM workspace.gold.gold_doctor_dashboard g
                JOIN workspace.healthcare_platform.patients p ON g.patient_id = p.patient_id
            """
            cur.execute(query)
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
                workspace.silver.silver_vitals
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
# التعديل النهائي الموصى به لدالة البحث
def patient_exists(national_id: str) -> bool:
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # دالة TRIM تزيل أي مسافات زائدة من الجانبين
                sql = "SELECT COUNT(1) FROM workspace.healthcare_platform.patients WHERE TRIM(national_id) = TRIM(?)"
                cur.execute(sql, (str(national_id),))
                row = cur.fetchone()
                return row[0] > 0
    except Exception as e:
        st.error(f"Error checking patient: {e}")
        return False

def insert_vitals(national_id: str, vitals: dict) -> bool:
    try:
        import uuid
        from datetime import datetime, timezone
        
        with get_connection() as conn:
            with conn.cursor() as cur:
                # 1. البحث عن الـ patient_id المرتبط بالرقم القومي (هذا الجزء ضروري للربط)
                cur.execute(f"SELECT patient_id FROM workspace.healthcare_platform.patients WHERE national_id = '{national_id}'")
                result = cur.fetchone()
                
                if not result:
                    st.error("❌ Patient not found with this National ID.")
                    return False
                
                patient_id = result[0] # استخراج الـ ID الصحيح للربط

                # 2. إدخال البيانات (تم حذف national_id من الأعمدة والقيم لأن الجدول لا يدعمه)
                sql = """
                    INSERT INTO workspace.healthcare_platform.vitals
                        (vital_id, patient_id, recorded_at, systolic_bp, diastolic_bp, 
                         heart_rate, temperature_c, spo2_pct, weight_kg, source_system)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                cur.execute(sql, (
                    str(uuid.uuid4()),
                    patient_id,
                    datetime.now(timezone.utc).isoformat(),
                    vitals['systolic_bp'],
                    vitals['diastolic_bp'],
                    vitals['heart_rate'],
                    vitals['temperature_c'],
                    vitals['spo2_pct'],
                    vitals['weight_kg'],
                    'streamlit_patient_portal'
                ))
        
        # تنظيف الكاش لتحديث الرسوم البيانية
        if 'load_vitals_timeseries' in globals():
            load_vitals_timeseries.clear()
            
        return True
    except Exception as e:
        st.error(f"Insert failed: {e}")
        return False
def insert_medication(patient_id: str, med_data: dict) -> bool:
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO workspace.healthcare_platform.medications
                        (med_id, patient_id, drug_name, dosage_mg,
                         frequency, prescribing_doc, prescribed_at)
                    VALUES (
                        '{str(uuid.uuid4())[:8]}',
                        '{patient_id}',
                        '{med_data['drug_name']}',
                        {med_data['dosage_mg']},
                        '{med_data['frequency']}',
                        '{med_data['prescribing_doc']}',
                        '{med_data['prescribed_at']}'
                    )
                """)
        return True
    except Exception as e:
        st.error(f"Medication insert failed: {e}")
        return False    

# ── Quality gate queries ───────────────────────────────────────────────────────
@st.cache_data(ttl=120, show_spinner=False)
def load_quality_health() -> pd.DataFrame:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM workspace.healthcare_platform.vw_quality_health
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
                FROM workspace.healthcare_platform.vw_latest_quality
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

    tab_vitals, tab_register, tab_upload, tab_diagnostics= st.tabs([
        "📋 Submit Vitals",
        "🆕 Register",
        "📤 Bulk Upload",
        "🔬 My Diagnostics"
        
    ])

    # ── Tab 1: Submit Vitals ───────────────────────────────────────────────────
    with tab_vitals:
        st.subheader("🩺 Submit Daily Vitals")

        with st.form("vitals_form", clear_on_submit=True):
            patient_nid = st.text_input("National ID *", placeholder="Enter 14-digit National ID")
            st.divider()
            col1, col2, col3 = st.columns(3)
            with col1:
                systolic = st.number_input("Systolic BP (mmHg)", 60, 250, 120)
                diastolic = st.number_input("Diastolic BP (mmHg)", 40, 150, 80)
            with col2:
                heart_rate = st.number_input("Heart Rate (bpm)", 30, 220, 72)
                temperature = st.number_input("Temperature (°C)", 34.0, 43.0, 37.0, step=0.1)
            with col3:
                spo2 = st.number_input("SpO₂ (%)", 70.0, 100.0, 98.0, step=0.1)
                weight = st.number_input("Weight (kg)", 20.0, 300.0, 70.0, step=0.5)

            submitted = st.form_submit_button("🚀 Submit Readings", type="primary")

        if submitted:
            # التحقق من المدخلات
            if len(patient_nid.strip()) != 14:
                st.error("❌ National ID must be exactly 14 digits.")
            elif systolic <= diastolic:
                st.error("❌ Systolic BP must be greater than Diastolic BP.")
            else:
                with st.spinner("Verifying patient and saving..."):
                    # التحقق من وجود المريض
                    if patient_exists(patient_nid.strip()):
                        vitals = {
                            "systolic_bp": systolic, "diastolic_bp": diastolic,
                            "heart_rate": heart_rate, "temperature_c": temperature,
                            "spo2_pct": spo2, "weight_kg": weight
                        }
                        # إرسال البيانات
                        success = insert_vitals(patient_nid.strip(), vitals)
                        if success:
                            st.success("✅ Vitals submitted successfully!")
                            st.balloons()
                        else:
                            st.error("❌ Failed to save data. Please check logs.")
                    else:
                        st.error("❌ Patient not found. Please register the patient first.")
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
            # التحقق من المدخلات الأساسية
            if not reg_first_name.strip():
                errors.append("First name is required.")
            if not reg_last_name.strip():
                errors.append("Last name is required.")

            if errors:
                for err in errors:
                    st.error(err)
            else:
                with st.spinner("Registering patient…"):
                    try:
                        # بما أننا لا نعتمد على الرقم القومي، نقوم بالتسجيل مباشرة
                        # أو يمكنك فحص التكرار بناءً على (الاسم + تاريخ الميلاد) إذا أردتِ
                        patient_data = {
                            "national_id":   reg_national_id.strip(),
                            "first_name":    reg_first_name.strip(),
                            "last_name":     reg_last_name.strip(),
                            "date_of_birth": str(reg_dob),
                            "gender":        reg_gender,
                            "blood_type":    reg_blood,
                            "contact_email": reg_email.strip(),
                        }
                        
                        # استدعاء دالة التسجيل (تأكدي أنها تضيف الـ patient_id في الـ DB)
                        new_uuid = register_patient(patient_data)
                        
                        st.success(
                            f"✅ **{reg_first_name} {reg_last_name}** "
                            f"registered successfully!\n\n"
                            f"📌 **Important:** Save your System Patient ID for logins and vitals:\n"
                            f"`{new_uuid}`"
                        )
                        
                        # تنظيف الكاش
                        if "load_doctor_dashboard" in globals():
                            load_doctor_dashboard.clear()
                            
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
                        col1, col2, col3 , col4= st.columns(4)
                        col1.metric("Total Rows",  result["rows_total"])
                        col2.metric("✅ Loaded",   result["rows_loaded"])
                        col3.metric("❌ Rejected", result["rows_rejected"])
                        col4.metric("👥 In Patients", result.get("rows_merged", 0))

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
# ── Tab 4: Patient Diagnostics ─────────────────────────────────────────────
    with tab_diagnostics:
        st.subheader("My Diagnostic Records")
        st.info(
            "Upload and manage your lab reports, X-rays, scans, "
            "and other diagnostic results."
        )

        diag_sub1, diag_sub2 = st.tabs([
            "➕ Add New Diagnostic",
            "📂 View My Records"
        ])

        # ── Add new diagnostic ─────────────────────────────────────────────────
        with diag_sub1:
            st.markdown("**Fill in the diagnostic details below:**")

            col1, col2 = st.columns(2)
            with col1:
                diag_patient_id = st.text_input(
                    "Your Patient ID (National ID) *",
                    placeholder="14-digit number",
                    key="diag_pid"
                )
                diag_type = st.selectbox(
                    "Diagnostic Type *",
                    list(DIAGNOSTIC_TYPES.keys()),
                    key="diag_type"
                )
                diag_date = st.date_input(
                    "Diagnostic Date *",
                    value=datetime.today(),
                    key="diag_date"
                )
                ordering_doc = st.text_input(
                    "Ordering Doctor",
                    placeholder="Dr. Ahmed Hassan",
                    key="diag_doc"
                )

            with col2:
                diag_name = st.selectbox(
                    "Diagnostic Name *",
                    DIAGNOSTIC_TYPES.get(
                        st.session_state.get("diag_type", "LAB"),
                        ["Other"]
                    ),
                    key="diag_name"
                )
                result_status = st.selectbox(
                    "Result Status *",
                    RESULT_STATUSES,
                    key="diag_status"
                )
                performing_lab = st.text_input(
                    "Lab / Facility Name",
                    placeholder="Cairo University Hospital Lab",
                    key="diag_lab"
                )

            result_summary = st.text_area(
                "Result Summary / Findings",
                placeholder="Enter the main findings or summary "
                            "from the diagnostic report...",
                height=100,
                key="diag_summary"
            )

            notes = st.text_area(
                "Additional Notes",
                placeholder="Any additional notes or comments...",
                height=80,
                key="diag_notes"
            )

            st.divider()
            st.markdown("**Upload Report File (Optional — max 4MB)**")
            uploaded_diag_file = st.file_uploader(
                "Upload report (PDF, JPG, PNG)",
                type=["pdf", "jpg", "jpeg", "png"],
                key="diag_file"
            )

            # Show file preview if image
            if uploaded_diag_file:
                fext = uploaded_diag_file.name.split(".")[-1].lower()
                if fext in ("jpg", "jpeg", "png"):
                    st.image(
                        uploaded_diag_file,
                        caption=uploaded_diag_file.name,
                        width=300
                    )
                else:
                    st.info(
                        f"📄 File ready: {uploaded_diag_file.name} "
                        f"({uploaded_diag_file.size / 1024:.1f} KB)"
                    )

            st.divider()

            # Structured lab values
            st.markdown("**Structured Lab Values (optional)**")
            add_lab_values = st.checkbox(
                "Add individual lab test values with reference ranges",
                value=False,
                key="add_lab_vals"
            )

            if add_lab_values:
                if "lab_rows" not in st.session_state:
                    st.session_state.lab_rows = [
                        {"name": "", "value": 0.0,
                         "unit": "", "ref_min": 0.0, "ref_max": 0.0}
                    ]

                for i, row in enumerate(st.session_state.lab_rows):
                    cols = st.columns([3, 2, 1, 1, 1])
                    with cols[0]:
                        row["name"] = st.text_input(
                            "Test Name",
                            value=row["name"],
                            key=f"ln_{i}",
                            placeholder="e.g. Hemoglobin"
                        )
                    with cols[1]:
                        row["value"] = st.number_input(
                            "Value",
                            value=float(row["value"]),
                            key=f"lv_{i}",
                            step=0.01
                        )
                    with cols[2]:
                        row["unit"] = st.text_input(
                            "Unit",
                            value=row["unit"],
                            key=f"lu_{i}",
                            placeholder="g/dL"
                        )
                    with cols[3]:
                        row["ref_min"] = st.number_input(
                            "Min",
                            value=float(row["ref_min"]),
                            key=f"lmin_{i}",
                            step=0.01
                        )
                    with cols[4]:
                        row["ref_max"] = st.number_input(
                            "Max",
                            value=float(row["ref_max"]),
                            key=f"lmax_{i}",
                            step=0.01
                        )

                if st.button("➕ Add another test row"):
                    st.session_state.lab_rows.append(
                        {"name": "", "value": 0.0,
                         "unit": "", "ref_min": 0.0, "ref_max": 0.0}
                    )
                    st.rerun()

            st.divider()

            # Submit button
            if st.button("💾 Save Diagnostic Record", type="primary",
                         key="save_diag_btn"):

                errors = []
                id_valid, id_reason = validate_national_id(
                    diag_patient_id.strip()
                )
                if not id_valid:
                    errors.append(id_reason)
                if not diag_name:
                    errors.append("Diagnostic name is required.")

                if errors:
                    for err in errors:
                        st.error(err)
                else:
                    with st.spinner("Saving diagnostic record…"):
                        try:
                            diag_id_temp = str(uuid.uuid4())
                            file_content = None
                            file_name    = None
                            file_type    = None
                            file_size_kb = 0.0

                            if uploaded_diag_file:
                                file_content, file_size_kb, file_type = \
                                    encode_file_to_base64(uploaded_diag_file)
                                file_name = uploaded_diag_file.name

                            record = {
                                "diagnostic_id":   diag_id_temp,
                                "patient_id":      diag_patient_id.strip(),
                                "diagnostic_type": diag_type,
                                "diagnostic_name": diag_name,
                                "diagnostic_date": str(diag_date),
                                "result_summary":  result_summary or None,
                                "result_status":   result_status,
                                "ordering_doctor": ordering_doc or None,
                                "performing_lab":  performing_lab or None,
                                "notes":           notes or None,
                                "file_name":       file_name,
                                "file_path":       None,
                                "file_type":       file_type,
                                "file_size_kb":    file_size_kb,
                                "file_content":    file_content,
                                "created_by":      "patient",
                            }

                            diagnostic_id = insert_diagnostic(record)

                            # Save lab values if entered
                            if add_lab_values and \
                               "lab_rows" in st.session_state:
                                valid_lab_rows = [
                                    r for r in st.session_state.lab_rows
                                    if r.get("name")
                                ]
                                if valid_lab_rows:
                                    insert_lab_values(
                                        diagnostic_id,
                                        diag_patient_id.strip(),
                                        valid_lab_rows
                                    )

                            load_patient_diagnostics.clear()
                            load_diagnostic_stats.clear()

                            st.success(
                                f"✅ Diagnostic record saved!\n\n"
                                f"Record ID: `{diagnostic_id}`"
                            )
                            st.balloons()

                            # Reset lab rows
                            if "lab_rows" in st.session_state:
                                del st.session_state["lab_rows"]

                        except Exception as e:
                            st.error(f"Failed to save: {e}")

        # ── View my records ────────────────────────────────────────────────────
        with diag_sub2:
            st.markdown("**Enter your Patient ID to view your records:**")

            view_patient_id = st.text_input(
                "Patient ID",
                placeholder="14-digit National ID",
                key="view_diag_pid"
            )

            if view_patient_id:
                is_valid, _ = validate_national_id(
                    view_patient_id.strip()
                )
                if not is_valid:
                    st.error("Please enter a valid 14-digit National ID.")
                else:
                    with st.spinner("Loading your records…"):
                        diag_df = load_patient_diagnostics(
                            view_patient_id.strip()
                        )

                    if diag_df.empty:
                        st.info(
                            "No diagnostic records found for this ID. "
                            "Add your first record in the "
                            "'Add New Diagnostic' tab."
                        )
                    else:
                        # KPI row
                        col1, col2, col3, col4 = st.columns(4)
                        col1.metric("Total Records",  len(diag_df))
                        col2.metric(
                            "Abnormal / Critical",
                            len(diag_df[diag_df["result_status"].isin(
                                ["ABNORMAL", "CRITICAL"]
                            )])
                        )
                        col3.metric(
                            "Pending",
                            len(diag_df[
                                diag_df["result_status"] == "PENDING"
                            ])
                        )
                        col4.metric(
                            "Diagnostic Types",
                            diag_df["diagnostic_type"].nunique()
                        )

                        st.divider()

                        # Color code status
                        def color_diag_status(val):
                            colors = {
                                "NORMAL":
                                    "color: #1D9E75; font-weight:500",
                                "ABNORMAL":
                                    "color: #EF9F27; font-weight:500",
                                "CRITICAL":
                                    "color: #E24B4A; font-weight:500",
                                "PENDING":
                                    "color: #888888; font-weight:500",
                            }
                            return colors.get(val, "")

                        styled = diag_df.style.map(
                            color_diag_status,
                            subset=["result_status"]
                        )
                        st.dataframe(
                            styled,
                            use_container_width=True,
                            hide_index=True
                        )

                        st.download_button(
                            "⬇️ Export My Records",
                            data=diag_df.to_csv(index=False),
                            file_name=f"my_diagnostics.csv",
                            mime="text/csv"
                        )

                        # ── Download a specific report file ────────────────────
                        st.divider()
                        st.markdown("**Download a Report File**")

                        # Show only records that have files
                        records_with_files = diag_df[
                            diag_df["file_name"].notna()
                        ]

                        if records_with_files.empty:
                            st.info(
                                "No files attached to your records yet."
                            )
                        else:
                            selected_record = st.selectbox(
                                "Select a record to download:",
                                options=records_with_files[
                                    "diagnostic_id"
                                ].tolist(),
                                format_func=lambda did: (
                                    records_with_files[
                                        records_with_files[
                                            "diagnostic_id"
                                        ] == did
                                    ].iloc[0]["diagnostic_name"]
                                    + " — "
                                    + str(records_with_files[
                                        records_with_files[
                                            "diagnostic_id"
                                        ] == did
                                    ].iloc[0]["diagnostic_date"])
                                ),
                                key="dl_record_select"
                            )

                            if selected_record:
                                with st.spinner("Loading file…"):
                                    content_b64, fname, ftype = \
                                        load_file_content(selected_record)

                                if content_b64:
                                    file_bytes = decode_base64_to_bytes(
                                        content_b64
                                    )

                                    # Show image preview
                                    if ftype in ("JPG", "JPEG", "PNG"):
                                        st.image(
                                            file_bytes,
                                            caption=fname,
                                            use_column_width=True
                                        )

                                    mime_map = {
                                        "PDF":  "application/pdf",
                                        "JPG":  "image/jpeg",
                                        "JPEG": "image/jpeg",
                                        "PNG":  "image/png",
                                    }
                                    mime = mime_map.get(
                                        ftype,
                                        "application/octet-stream"
                                    )

                                    st.download_button(
                                        label=f"⬇️ Download {fname}",
                                        data=file_bytes,
                                        file_name=fname,
                                        mime=mime,
                                        key="dl_btn"
                                    )
                                else:
                                    st.info(
                                        "No file attached to "
                                        "this record."
                                    )  


                        


# ══════════════════════════════════════════════════════════════════════════════
# DOCTOR ROLE
# ══════════════════════════════════════════════════════════════════════════════
elif st.session_state.role == "doctor":
    st.title("Clinical Dashboard")

    tab_dashboard, tab_vitals_drill,tab_meds, tab_monitor, tab_quality, tab_ai,tab_diag,tab_surgeries, tab_history = st.tabs([
        "📊 Dashboard",
        "🩺 Patient Vitals",
        "💊 Prescribe Medication",
        "🔍 Ingestion Monitor",
        "🏥 Quality Gates",
        "🤖 AI Insights",
        "🔬 Diagnostics",
        "🩻 Surgeries",
        "🏥 Patient History"
        
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
        # أضيفي هذا السطر قبل سطر الـ filtered[...]


        filtered = df[df["risk_level"].isin(risk_filter)]
        st.dataframe(
            filtered[[
                "full_name", "age_years", "gender", "blood_type",
                "risk_level", "composite_risk_score",
                "avg_systolic_bp", "avg_diastolic_bp",
                "avg_heart_rate", "avg_spo2_pct",
                "active_medications", "latest_recorded_at"
            ]],
            use_container_width=True,
            hide_index=True
        )

        if st.button("🔄 Refresh Dashboard"):
            load_doctor_dashboard.clear()
            st.rerun()

    # ── Tab 2: Patient Vitals Drill-Down ───────────────────────────────────────
    with tab_vitals_drill:
        # 1. جلب البيانات المحدثة
        df = load_doctor_dashboard() 

        if not df.empty and "national_id" in df.columns:
            # 2. القائمة المنسدلة تظهر الاسم فقط، لكنها تحمل الـ national_id كقيمة
            selected_nid = st.selectbox(
                "Select a patient:",
                options=df["national_id"].unique().tolist(),
                format_func=lambda nid: df[df['national_id'] == nid]['full_name'].values[0]
            )

            # 3. إضافة زر تحديث للقياسات الحيوية
            if st.button("🔄 Refresh Vitals Cache"):
                load_vitals_timeseries.clear()
                st.rerun()

            if selected_nid:
                # 4. الربط الصحيح: استخراج الـ patient_id (الـ UUID) باستخدام الـ national_id
                patient_id_row = df[df['national_id'] == selected_nid]
                selected_patient_id = patient_id_row['patient_id'].values[0]

                # 5. جلب القياسات باستخدام الـ patient_id وليس الرقم القومي
                vitals_df = load_vitals_timeseries(selected_patient_id)

                if vitals_df.empty:
                    st.info(f"No vitals recorded for this patient.")
                else:
                    # 6. عرض الرسوم البيانية
                    fig_bp = go.Figure()
                    fig_bp.add_trace(go.Scatter(
                        x=vitals_df["recorded_at"], y=vitals_df["systolic_bp"],
                        name="Systolic", line=dict(color="#E24B4A", width=2)
                    ))
                    fig_bp.add_trace(go.Scatter(
                        x=vitals_df["recorded_at"], y=vitals_df["diastolic_bp"],
                        name="Diastolic", line=dict(color="#378ADD", width=2)
                    ))
                    fig_bp.update_layout(title="Blood Pressure Over Time", margin=dict(t=40, b=20))
                    st.plotly_chart(fig_bp, use_container_width=True)
                    
                    # عرض البيانات كجدول إضافي إذا لزم الأمر
                    st.dataframe(vitals_df, use_container_width=True)

        else:
            st.warning("No dashboard data available. Please check data ingestion.")
                    # باقي كود الرسم البياني (SpO2 و Heart Rate)...
    # ── Tab 3: Prescribe Medication ─────────────────────────────────────────────
    with tab_meds:
        st.subheader("💊 Prescribe New Medication")
        df_docs = load_doctor_dashboard()
        
        if df_docs.empty:
            st.warning("No patient data available to prescribe medication.")
        else:
            with st.form("prescribe_med_form", clear_on_submit=True):
                # قائمة منسدلة لاختيار المريض بناءً على المرضى المسجلين فعلياً
                selected_patient_id = st.selectbox(
                    "Select Patient:",
                    options=df_docs["patient_id"].tolist(),
                    format_func=lambda pid: df_docs[df_docs["patient_id"] == pid]["full_name"].values[0]
                )
                
                drug_name = st.text_input("Drug Name:", placeholder="e.g., Metformin, Lipitor")
                dosage_mg = st.number_input("Dosage (mg):", min_value=1, max_value=2000, value=500, step=50)
                frequency = st.selectbox(
                    "Frequency:",
                    ["once_daily", "twice_daily", "three_times_daily", "four_times_daily", "as_needed", "unknown"]
                )
                prescribing_doc = st.text_input("Prescribing Doctor Name:", value="Dr. Ahmed")
                
                submit_med = st.form_submit_button("🚀 Submit Prescription", type="primary")
                
            if submit_med:
                if not drug_name.strip():
                    st.error("❌ Drug Name cannot be empty.")
                else:
                    med_data = {
                        "drug_name": drug_name.strip(),
                        "dosage_mg": dosage_mg,
                        "frequency": frequency,
                        "prescribing_doc": prescribing_doc.strip(),
                        "prescribed_at": datetime.now(timezone.utc).isoformat()
                    }
                    with st.spinner("Saving prescription to Databricks..."):
                        success = insert_medication(selected_patient_id, med_data)
                    if success:
                        st.success(f"✅ Prescription for '{drug_name}' saved successfully!")
                        st.balloons()                    

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
    df_patients = load_all_patients()                
    with tab_surgeries:
        
        # نموذج إدخال بيانات المريض والعملية
        # داخل tab_surgeries في app.py
        st.subheader("🩻 Surgery Records")
        
        with st.form("surgery_manual_form", clear_on_submit=True):
            patient_name = st.text_input("Patient Full Name:")
            patient_nid = st.text_input("Patient National ID:")
            surgery_name = st.text_input("Surgery Name:")
            surgery_date = st.date_input("Date of Surgery:")
            surgeon_name = st.text_input("Surgeon Name:")
            notes = st.text_area("Notes:")
            uploaded_report = st.file_uploader("Upload Report", type=['pdf', 'jpg', 'png'])
            
            submit_surgery = st.form_submit_button("🚀 Save Surgery Record", type="primary")

        if submit_surgery:
            if len(patient_nid) != 14:
                st.error("❌ National ID must be 14 digits.")
            else:
                # منطق حفظ الملف (إذا وجد)
                file_path = None
                if uploaded_report:
                    file_path = f"/Volumes/workspace/healthcare_platform/my_model_storage/{uploaded_report.name}"
                    with open(file_path, "wb") as f:
                        f.write(uploaded_report.getbuffer())
                
                surgery_data = {
                    "patient_name": patient_name,
                    "surgery_name": surgery_name,
                    "surgery_date": str(surgery_date),
                    "surgeon_name": surgeon_name,
                    "notes": notes
                }
                
                with st.spinner("Saving data..."):
                    success = insert_surgery(patient_nid, surgery_data, file_path)
                
                if success:
                    st.success("✅ Surgery record saved successfully!")
                    st.balloons()
                    # استدعاء إعادة تحميل الصفحة لعرض البيانات الجديدة
                    st.rerun()
     # ── Tab 8: Patient history View ────────────────────────────────────────             

    with tab_history:

        st.subheader("🔍 Patient History")

        national_id_input = st.text_input(
            label="National ID",
            placeholder="Enter 14-digit National ID",
            max_chars=14,
        )

        if national_id_input:

            # ── Basic format guard ────────────────────────────────────────────────────
            if not re.fullmatch(r"\d{14}", national_id_input):
                st.error("National ID must be exactly 14 digits.")

            else:
                with st.spinner("Fetching patient records…"):
                    history = get_patient_history(national_id_input)

                # ── Patient not found ─────────────────────────────────────────────────
                if history is None:
                    st.warning(
                        f"No patient found for National ID **{national_id_input}**. "
                        "Please verify the ID and try again."
                    )

                # ── Patient found ─────────────────────────────────────────────────────
                else:
                    st.success(f"Patient UUID: `{history['patient_id']}`")

                    # Last 3 Vitals
                    st.markdown("#### 🩺 Last 3 Vitals")
                    if history["vitals"].empty:
                        st.info("No vital readings recorded.")
                    else:
                        st.dataframe(
                            history["vitals"],
                            use_container_width=True,
                            hide_index=True,
                        )

                    st.divider()

                    # Combined Medical History
                    st.markdown("#### 📋 Medical History")
                    medical_history_df = _build_medical_history(history)

                    if medical_history_df.empty:
                        st.info("No medical history records found.")
                    else:
                        # Colour-code by record type
                        def _highlight_type(row):
                            colours = {
                                "Diagnostic": "background-color: #e8f4fd",
                                "Surgery":    "background-color: #fdecea",
                                "Medication": "background-color: #e9fbe9",
                            }
                            return [colours.get(row["Type"], "")] * len(row)

                        st.dataframe(
                            medical_history_df.style.apply(_highlight_type, axis=1),
                            use_container_width=True,
                            hide_index=True,
                        )

                        # Download button
                        st.download_button(
                            label="⬇️ Export Medical History as CSV",
                            data=medical_history_df.to_csv(index=False).encode("utf-8"),
                            file_name=f"medical_history_{national_id_input}.csv",
                            mime="text/csv",
                        )                                        
# ── Tab 6: Doctor Diagnostics View ────────────────────────────────────────
    with tab_diag:
        st.subheader("Patient Diagnostics")

        diag_doc1, diag_doc2, diag_doc3 = st.tabs([
            "📊 Overview",
            "🔍 Browse Records",
            "➕ Add for Patient"
        ])

        # ── Tab: Overview ──────────────────────────────────────────────────────
        with diag_doc1:
            stats_df = load_diagnostic_stats()

            if stats_df.empty:
                st.info(
                    "No diagnostic records yet. "
                    "Add records using the other tabs."
                )
            else:
                total_diag     = int(stats_df["total"].sum())
                total_abnormal = int(stats_df["abnormal_count"].sum())
                total_pending  = int(stats_df["pending_count"].sum())

                col1, col2, col3 = st.columns(3)
                col1.metric("Total Diagnostics",   f"{total_diag:,}")
                col2.metric("Abnormal / Critical", f"{total_abnormal:,}")
                col3.metric("Pending Results",     f"{total_pending:,}")

                st.divider()

                # Chart by type
                fig_diag = px.bar(
                    stats_df,
                    x="diagnostic_type",
                    y="total",
                    color="abnormal_count",
                    title="Diagnostics by Type",
                    labels={
                        "diagnostic_type": "Type",
                        "total":           "Total Records",
                        "abnormal_count":  "Abnormal Count"
                    },
                    color_continuous_scale="Reds"
                )
                fig_diag.update_layout(margin=dict(t=40, b=20))
                st.plotly_chart(fig_diag, use_container_width=True)

                st.dataframe(
                    stats_df.rename(columns={
                        "diagnostic_type":  "Type",
                        "total":            "Total",
                        "abnormal_count":   "Abnormal",
                        "pending_count":    "Pending",
                        "latest_date":      "Latest Date"
                    }),
                    use_container_width=True,
                    hide_index=True
                )

        # ── Tab: Browse Records ────────────────────────────────────────────────
        with diag_doc2:
            col1, col2 = st.columns(2)
            with col1:
                type_filter = st.selectbox(
                    "Filter by diagnostic type:",
                    ["ALL"] + list(DIAGNOSTIC_TYPES.keys()),
                    key="doc_type_filter"
                )
            with col2:
                status_filter = st.selectbox(
                    "Filter by result status:",
                    ["ALL"] + RESULT_STATUSES,
                    key="doc_status_filter"
                )

            with st.spinner("Loading records…"):
                all_diag_df = load_all_diagnostics(
                    type_filter, status_filter
                )

            if all_diag_df.empty:
                st.info("No records match the selected filters.")
            else:
                st.markdown(
                    f"Showing **{len(all_diag_df):,}** records"
                )

                # Color code status
                def color_doc_status(val):
                    colors = {
                        "NORMAL":   "color: #1D9E75; font-weight:500",
                        "ABNORMAL": "color: #EF9F27; font-weight:500",
                        "CRITICAL": "color: #E24B4A; font-weight:500",
                        "PENDING":  "color: #888888; font-weight:500",
                    }
                    return colors.get(val, "")

                styled_all = all_diag_df[[
                    "full_name", "diagnostic_type",
                    "diagnostic_name", "diagnostic_date",
                    "result_status", "result_summary",
                    "ordering_doctor", "performing_lab",
                    "diagnostic_id"
                ]].style.map(
                    color_doc_status,
                    subset=["result_status"]
                )

                st.dataframe(
                    styled_all,
                    use_container_width=True,
                    hide_index=True
                )

                st.download_button(
                    "⬇️ Export All Records",
                    data=all_diag_df.to_csv(index=False),
                    file_name="all_diagnostics.csv",
                    mime="text/csv",
                    key="export_all_diag"
                )

                st.divider()

                # ── Drill into a specific record ───────────────────────────────
                st.markdown("**Drill into a Patient Record**")

                records_with_files = all_diag_df[
                    all_diag_df["file_name"].notna()
                ]

                col_left, col_right = st.columns(2)

                with col_left:
                    # View lab values
                    st.markdown("*View structured lab values:*")
                    lab_diag_id = st.text_input(
                        "Enter Diagnostic ID for lab values:",
                        placeholder="Paste from table above",
                        key="lab_diag_id_input"
                    )
                    if lab_diag_id:
                        with st.spinner("Loading lab values…"):
                            lab_df = load_lab_results(
                                lab_diag_id.strip()
                            )
                        if lab_df.empty:
                            st.info(
                                "No structured lab values "
                                "for this record."
                            )
                        else:
                            def highlight_abnormal(row):
                                if row.get("is_abnormal"):
                                    return [
                                        "background-color: #fff3cd"
                                    ] * len(row)
                                return [""] * len(row)

                            styled_lab = lab_df.style.apply(
                                highlight_abnormal, axis=1
                            )
                            st.dataframe(
                                styled_lab,
                                use_container_width=True,
                                hide_index=True
                            )

                with col_right:
                    # Download report file
                    st.markdown("*Download attached report:*")

                    if records_with_files.empty:
                        st.info("No records have attached files.")
                    else:
                        dl_record = st.selectbox(
                            "Select record:",
                            options=records_with_files[
                                "diagnostic_id"
                            ].tolist(),
                            format_func=lambda did: (
                                records_with_files[
                                    records_with_files[
                                        "diagnostic_id"
                                    ] == did
                                ].iloc[0]["full_name"]
                                + " — "
                                + records_with_files[
                                    records_with_files[
                                        "diagnostic_id"
                                    ] == did
                                ].iloc[0]["diagnostic_name"]
                                + " ("
                                + str(records_with_files[
                                    records_with_files[
                                        "diagnostic_id"
                                    ] == did
                                ].iloc[0]["diagnostic_date"])
                                + ")"
                            ),
                            key="doc_dl_select"
                        )

                        if dl_record:
                            with st.spinner("Loading file…"):
                                content_b64, fname, ftype = \
                                    load_file_content(dl_record)

                            if content_b64:
                                file_bytes = decode_base64_to_bytes(
                                    content_b64
                                )

                                if ftype in ("JPG", "JPEG", "PNG"):
                                    st.image(
                                        file_bytes,
                                        caption=fname,
                                        use_column_width=True
                                    )

                                mime_map = {
                                    "PDF":  "application/pdf",
                                    "JPG":  "image/jpeg",
                                    "JPEG": "image/jpeg",
                                    "PNG":  "image/png",
                                }
                                mime = mime_map.get(
                                    ftype,
                                    "application/octet-stream"
                                )
                                st.download_button(
                                    label=f"⬇️ Download {fname}",
                                    data=file_bytes,
                                    file_name=fname,
                                    mime=mime,
                                    key="doc_dl_btn"
                                )
                            else:
                                st.info("No file for this record.")

        # ── Tab: Add for Patient ───────────────────────────────────────────────
        with diag_doc3:
            st.markdown(
                "Add a diagnostic record on behalf of a patient."
            )

            col1, col2 = st.columns(2)

            with col1:
                doc_patient_id = st.text_input(
                    "Patient ID *",
                    placeholder="14-digit National ID",
                    key="doc_diag_pid"
                )
                doc_diag_type = st.selectbox(
                    "Diagnostic Type *",
                    list(DIAGNOSTIC_TYPES.keys()),
                    key="doc_diag_type"
                )
                doc_diag_date = st.date_input(
                    "Diagnostic Date *",
                    value=datetime.today(),
                    key="doc_diag_date"
                )
                doc_ordering = st.text_input(
                    "Ordering Doctor",
                    placeholder="Dr. Sara Khalid",
                    key="doc_ordering"
                )

            with col2:
                doc_diag_name = st.selectbox(
                    "Diagnostic Name *",
                    DIAGNOSTIC_TYPES.get(
                        st.session_state.get(
                            "doc_diag_type", "LAB"
                        ),
                        ["Other"]
                    ),
                    key="doc_diag_name"
                )
                doc_status = st.selectbox(
                    "Result Status *",
                    RESULT_STATUSES,
                    key="doc_result_status"
                )
                doc_lab = st.text_input(
                    "Lab / Facility",
                    placeholder="Hospital Laboratory",
                    key="doc_lab_name"
                )

            doc_summary = st.text_area(
                "Findings / Result Summary",
                height=120,
                placeholder="Enter clinical findings...",
                key="doc_summary"
            )

            doc_notes = st.text_area(
                "Clinical Notes",
                height=80,
                placeholder="Additional clinical notes...",
                key="doc_notes"
            )

            st.markdown("**Attach Report File (Optional — max 4MB)**")
            doc_file = st.file_uploader(
                "Upload PDF or image",
                type=["pdf", "jpg", "jpeg", "png"],
                key="doc_diag_file"
            )

            if doc_file:
                fext = doc_file.name.split(".")[-1].lower()
                if fext in ("jpg", "jpeg", "png"):
                    st.image(
                        doc_file,
                        caption=doc_file.name,
                        width=300
                    )
                else:
                    st.info(
                        f"📄 {doc_file.name} "
                        f"({doc_file.size / 1024:.1f} KB)"
                    )

            st.divider()

            # Doctor lab values entry
            st.markdown(
                "**Add Structured Lab Values (optional)**"
            )
            doc_add_lab = st.checkbox(
                "Add individual test values",
                value=False,
                key="doc_add_lab"
            )

            if doc_add_lab:
                if "doc_lab_rows" not in st.session_state:
                    st.session_state.doc_lab_rows = [
                        {"name": "", "value": 0.0,
                         "unit": "", "ref_min": 0.0,
                         "ref_max": 0.0}
                    ]

                for i, row in enumerate(
                    st.session_state.doc_lab_rows
                ):
                    cols = st.columns([3, 2, 1, 1, 1])
                    with cols[0]:
                        row["name"] = st.text_input(
                            "Test Name",
                            value=row["name"],
                            key=f"dln_{i}",
                            placeholder="e.g. WBC"
                        )
                    with cols[1]:
                        row["value"] = st.number_input(
                            "Value",
                            value=float(row["value"]),
                            key=f"dlv_{i}",
                            step=0.01
                        )
                    with cols[2]:
                        row["unit"] = st.text_input(
                            "Unit",
                            value=row["unit"],
                            key=f"dlu_{i}",
                            placeholder="10³/µL"
                        )
                    with cols[3]:
                        row["ref_min"] = st.number_input(
                            "Min",
                            value=float(row["ref_min"]),
                            key=f"dlmin_{i}",
                            step=0.01
                        )
                    with cols[4]:
                        row["ref_max"] = st.number_input(
                            "Max",
                            value=float(row["ref_max"]),
                            key=f"dlmax_{i}",
                            step=0.01
                        )

                if st.button(
                    "➕ Add another test row",
                    key="doc_add_lab_row"
                ):
                    st.session_state.doc_lab_rows.append(
                        {"name": "", "value": 0.0,
                         "unit": "", "ref_min": 0.0,
                         "ref_max": 0.0}
                    )
                    st.rerun()

            st.divider()

            if st.button(
                "💾 Save Diagnostic Record",
                type="primary",
                key="doc_save_diag"
            ):
                if not doc_patient_id.strip():
                    st.error("Patient ID is required.")
                else:
                    with st.spinner("Saving…"):
                        try:
                            diag_id_temp = str(uuid.uuid4())
                            file_content = None
                            file_name    = None
                            file_type    = None
                            file_size_kb = 0.0

                            if doc_file:
                                file_content, file_size_kb, \
                                    file_type = encode_file_to_base64(
                                        doc_file
                                    )
                                file_name = doc_file.name

                            record = {
                                "diagnostic_id":   diag_id_temp,
                                "patient_id":      doc_patient_id.strip(),
                                "diagnostic_type": doc_diag_type,
                                "diagnostic_name": doc_diag_name,
                                "diagnostic_date": str(doc_diag_date),
                                "result_summary":  doc_summary or None,
                                "result_status":   doc_status,
                                "ordering_doctor": doc_ordering or None,
                                "performing_lab":  doc_lab or None,
                                "notes":           doc_notes or None,
                                "file_name":       file_name,
                                "file_path":       None,
                                "file_type":       file_type,
                                "file_size_kb":    file_size_kb,
                                "file_content":    file_content,
                                "created_by":      "doctor",
                            }

                            diagnostic_id = insert_diagnostic(record)

                            # Save lab values if entered
                            if doc_add_lab and \
                               "doc_lab_rows" in st.session_state:
                                valid_rows = [
                                    r for r in
                                    st.session_state.doc_lab_rows
                                    if r.get("name")
                                ]
                                if valid_rows:
                                    insert_lab_values(
                                        diagnostic_id,
                                        doc_patient_id.strip(),
                                        valid_rows
                                    )

                            load_all_diagnostics.clear()
                            load_diagnostic_stats.clear()

                            st.success(
                                f"✅ Record saved!\n\n"
                                f"Diagnostic ID: `{diagnostic_id}`"
                            )

                            if "doc_lab_rows" in st.session_state:
                                del st.session_state["doc_lab_rows"]

                        except Exception as e:
                            st.error(f"Failed to save: {e}")

        # ── Refresh button ─────────────────────────────────────────────────────
        if st.button("🔄 Refresh Diagnostics", key="refresh_diag"):
            load_all_diagnostics.clear()
            load_diagnostic_stats.clear()
            load_patient_diagnostics.clear()
            load_file_content.clear()
            st.rerun()          

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
