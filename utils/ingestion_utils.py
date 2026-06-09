# utils/ingestion_utils.py
import json
import re
import os
from datetime import datetime, timezone
from databricks import sql
import pandas as pd
import streamlit as st
import uuid
 
SCHEMA = "workspace.healthcare_platform"


# ── Connection ─────────────────────────────────────────────────────────────────
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
                "Missing DATABRICKS_HOST and DATABRICKS_HTTP_PATH."
            )

    connect_args = {
        "server_hostname": host,
        "http_path":       http_path,
        "_socket_timeout": 30,
    }
    if token:
        connect_args["access_token"] = token

    return sql.connect(**connect_args)



@st.cache_data(ttl=10, show_spinner=False) # خفضنا الـ ttl إلى 10 ثوانٍ للتجربة
def load_source_config(source_name: str) -> dict:
    """Load one source config row from metadata_config using safe parameters."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            # استخدام الفاصلة (؟) لتجنب SQL Injection
            cur.execute("""
                SELECT * FROM workspace.healthcare_platform.metadata_config
                WHERE source_name = %s 
                  AND is_active = true
            """, (source_name,))
            
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]

    if not rows:
        return {} # إرجاع قاموس فارغ بدلاً من إيقاف البرنامج بالكامل

    config = dict(zip(cols, rows[0]))

    # معالجة حقول JSON
    json_fields = ["expected_columns", "optional_columns", "coalesce_fields", "column_mapping"]
    for json_col in json_fields:
        raw = config.get(json_col)
        try:
            config[json_col] = json.loads(raw) if raw else ([] if json_col != "column_mapping" else {})
        except:
            config[json_col] = [] if json_col != "column_mapping" else {}

    return config
# ── National ID validator (single value) ──────────────────────────────────────
def validate_national_id(value: str, length: int = 14) -> tuple[bool, str]:
    """
    معدلة: تقبل الـ UUID (المعرف الفريد) أو الرقم القومي المكون من 14 رقماً.
    """
    # التحقق إذا كان المعرف هو UUID (طوله 36 ويحتوي على شرطات)
    if len(value) == 36 and "-" in value:
        return True, ""
        
    # التحقق التقليدي للأرقام فقط
    if not value.isdigit():
        return False, "National ID must contain digits only."
    if len(value) != length:
        return False, f"National ID must be exactly {length} digits."
        
    return True, ""

# utils/ingestion_utils.py

def patient_exists(national_id: str) -> bool:
    """
    تتحقق ما إذا كان المريض مسجلاً مسبقاً بناءً على الرقم القومي.
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # تغيير البحث ليتم عبر national_id بدلاً من patient_id
                sql = "SELECT COUNT(1) FROM workspace.healthcare_platform.patients WHERE national_id = ?"
                cur.execute(sql, (national_id,))
                row = cur.fetchone()
                return row[0] > 0
    except Exception as e:
        import streamlit as st
        st.error(f"Connection error: {e}")
        return False
    


# ── CSV ingestion (Streamlit-side, pandas-based) ───────────────────────────────
def ingest_csv_streamlit(
    uploaded_file,
    source_name: str = "patients_csv"
) -> dict:
    """
    Ingests an uploaded CSV file from Streamlit:
    1. Reads with pandas
    2. Applies column mapping
    3. Validates National ID
    4. Writes valid rows to bronze_ingestion table via SQL INSERT
    5. Writes rejected rows to rejected_records
    6. Updates metadata_config
    Returns a summary dict.
    """
    config        = load_source_config(source_name)
    expected_cols = config.get("expected_columns", [])
    optional_cols = config.get("optional_columns", [])
    column_mapping = config.get("column_mapping", {})
    id_col        = config.get("national_id_col", "national_id")
    id_length     = config.get("national_id_length", 14)
    do_filter     = config.get("national_id_filter", False)
    delimiter     = config.get("csv_delimiter", ",")
    file_name     = uploaded_file.name
    run_id        = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # ── Read CSV ───────────────────────────────────────────────────────────────
    df = pd.read_csv(uploaded_file, delimiter=delimiter, dtype=str)
    df.columns = df.columns.str.strip()
   

    # ── Apply column mapping ───────────────────────────────────────────────────
    df = df.rename(columns=column_mapping)

    # ── Add missing optional columns as empty ─────────────────────────────────
    for col in optional_cols:
        if col not in df.columns:
            df[col] = None

    # ── Check required columns ────────────────────────────────────────────────
    required_cols   = [c for c in expected_cols if c not in optional_cols]
    missing         = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # ── Keep only expected columns ────────────────────────────────────────────
    all_allowed = list(dict.fromkeys(expected_cols + optional_cols))
    df = df[[c for c in all_allowed if c in df.columns]]

    # ── National ID filter (الطريقة السريعة) ──────────────────────────────────
    if do_filter and id_col in df.columns:
        # التأكد من أن العمود نصي قبل التحقق
        df[id_col] = df[id_col].astype(str).str.strip()
        
        # شرط الصحة: أرقام فقط + الطول المطلوب
        is_valid = df[id_col].str.isdigit() & (df[id_col].str.len() == id_length)
        
        valid_df = df[is_valid].copy()
        rejected_df = df[~is_valid].copy()
        
        # إضافة تفاصيل الرفض للجدول المرفوض
        if not rejected_df.empty:
            rejected_df["rejection_reason"] = f"Invalid ID: Must be {id_length} digits"
            rejected_df["rejection_id"] = run_id + "_" + rejected_df.index.astype(str)
            rejected_df["source_name"] = source_name
            rejected_df["file_name"] = file_name
            rejected_df["row_number"] = rejected_df.index
            rejected_df["national_id_value"] = rejected_df[id_col]
            rejected_df["raw_data"] = rejected_df.apply(lambda x: x.to_json(), axis=1)
            rejected_df["rejected_at"] = datetime.now(timezone.utc).isoformat()
    else:
        valid_df = df.copy()
        rejected_df = pd.DataFrame()

    

    # ── National ID filter ────────────────────────────────────────────────────
    """ valid_rows    = []
    rejected_rows = []

    for idx, row in df.iterrows():
        if do_filter and id_col in df.columns:
            id_val = str(row.get(id_col, "")).strip()
            is_valid, reason = validate_national_id(id_val, id_length)
            if not is_valid:
                rejected_rows.append({
                    "rejection_id":     f"{run_id}_{idx}",
                    "source_name":      source_name,
                    "file_name":        file_name,
                    "row_number":       idx,
                    "national_id_value": id_val,
                    "rejection_reason": reason,
                    "raw_data":         row.to_json(),
                    "rejected_at":      datetime.now(timezone.utc).isoformat()
                })
                continue
        valid_rows.append(row)

    valid_df    = pd.DataFrame(valid_rows)
    rejected_df = pd.DataFrame(rejected_rows)"""

    rows_loaded   = 0
    rows_rejected = len(rejected_df)
    # ── احذفي السطور القديمة التي كانت تحول القوائم إلى DataFrames ──
    # valid_df    = pd.DataFrame(valid_rows)  <-- احذفي هذا
    # rejected_df = pd.DataFrame(rejected_rows) <-- احذفي هذا

    # ── استخدمي المتغيرات الجاهزة (التي تم تعريفها في خطوة الفلترة السريعة) ──
    # تأكدي فقط من التأكد من وجودها قبل المتابعة
    if 'valid_df' not in locals():
        valid_df = df.copy()
    if 'rejected_df' not in locals():
        rejected_df = pd.DataFrame()

    rows_loaded   = len(valid_df)
    rows_rejected = len(rejected_df)
    if not valid_df.empty:
        # أضيفي هذا الكود في نفس المكان الذي حذفتِ منه السطور السابقة:
        with get_connection() as conn:
            # 1. جلب المرضى الحاليين من قاعدة البيانات
            existing_patients = pd.read_sql("SELECT national_id, patient_id FROM workspace.healthcare_platform.patients", conn)

        # 2. ربط الـ patient_id الموجود مع البيانات الجديدة بناءً على national_id
        valid_df = valid_df.merge(existing_patients, on='national_id', how='left')

        # 3. إذا كان المريض جديداً (لا يوجد له patient_id)، ولدي له واحد جديد
        valid_df['patient_id'] = valid_df['patient_id'].apply(lambda x: str(uuid.uuid4()) if pd.isna(x) else x)
    
        
        valid_df["_source_file"]      = file_name
        valid_df["_source_name"]      = source_name
        valid_df["_ingested_at"]      = datetime.now(timezone.utc).isoformat()
        valid_df["_ingestion_run_id"] = run_id

        # 2. تحديد الترتيب النهائي للأعمدة (يجب أن يطابق ترتيب الأعمدة في الجدول)
        # هذا الترتيب هو ما سيضمن عدم حدوث خطأ Arity Mismatch
        expected_columns = [
            'national_id', 'first_name', 'last_name', 'date_of_birth', 'gender', 
            'blood_type', 'contact_email', '_source_file', '_source_name', 
            '_ingested_at', '_ingestion_run_id', 'patient_id'
        ]
        
        # التأكد من إعادة ترتيب الـ DataFrame ليتطابق مع القائمة أعلاه
        valid_df = valid_df[expected_columns]

        target_table = f"workspace.healthcare_platform.bronze_ingestion_{source_name}"

        with get_connection() as conn:
            with conn.cursor() as cur:
                # Create table if needed
                cols_ddl = ",\n".join([f"`{c}` STRING" for c in expected_columns])
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {target_table} ({cols_ddl})
                    USING DELTA
                """)

                # Insert rows (الآن أصبحنا واثقين أن الترتيب هو نفسه دائماً)
                for _, row in valid_df.iterrows():
                    vals = ", ".join([
                        "NULL" if pd.isna(v) or v == 'None' 
                        else f"'{str(v).replace(chr(39), chr(39)*2)}'"
                        for v in row.values
                    ])
                    cur.execute(f"INSERT INTO {target_table} VALUES ({vals})")
                
                rows_loaded = len(valid_df)

    # ── Write rejected rows ────────────────────────────────────────────────────
    if not rejected_df.empty:
        with get_connection() as conn:
            with conn.cursor() as cur:
                for _, row in rejected_df.iterrows():
                    raw = row["raw_data"].replace("'", "''")
                    cur.execute(f"""
                        INSERT INTO workspace.healthcare_platform.rejected_records
                            (rejection_id, source_name, file_name, row_number,
                             national_id_value, rejection_reason,
                             raw_data, rejected_at)
                        VALUES (
                            '{row["rejection_id"]}',
                            '{row["source_name"]}',
                            '{row["file_name"]}',
                            {row["row_number"]},
                            '{row["national_id_value"]}',
                            '{row["rejection_reason"].replace("'", "''")}',
                            '{raw}',
                            current_timestamp()
                        )
                    """)

    # ── Update metadata ────────────────────────────────────────────────────────
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE workspace.healthcare_platform.metadata_config
                SET
                    last_ingested_at    = current_timestamp(),
                    last_file_loaded    = '{file_name.replace("'", "''")}',
                    total_rows_loaded   = total_rows_loaded   + {rows_loaded},
                    total_rows_rejected = total_rows_rejected + {rows_rejected},
                    last_updated        = current_timestamp()
                WHERE source_name = '{source_name}'
            """)
    # ── Merge valid rows into main patients table ──────────────────────────────
    merged_count = 0
    if rows_loaded > 0:
        try:
            merged_count = merge_csv_to_patients(source_name)
        except Exception as e:
            merged_count = 0

    return {
        "status":        "SUCCESS",
        "file":          file_name,
        "rows_total":    len(df),
        "rows_loaded":   rows_loaded,
        "rows_rejected": rows_rejected,
        "rows_merged":   merged_count,      # ← new
        "rejected_df":   rejected_df
    }        
            

    return {
        "status":        "SUCCESS",
        "file":          file_name,
        "rows_total":    len(df),
        "rows_loaded":   rows_loaded,
        "rows_rejected": rows_rejected,
        "rejected_df":   rejected_df
    }


# ── Patient registration (single row insert) ───────────────────────────────────
def register_patient(patient: dict) -> str | None: # تغيير النوع هنا
    import uuid
    
    # التحقق من وجود الرقم القومي قبل البدء
    if "national_id" not in patient:
        return None 

    generated_patient_id = str(uuid.uuid4())
    
    # استخدام Parameterized Query لتجنب الأخطاء الأمنية
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                sql = """
                    INSERT INTO workspace.healthcare_platform.patients 
                    (patient_id, national_id, first_name, last_name, date_of_birth, gender, blood_type, contact_email, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, current_timestamp())
                """
                cur.execute(sql, (
                    generated_patient_id,
                    patient["national_id"],
                    patient["first_name"],
                    patient["last_name"],
                    patient["date_of_birth"],
                    patient["gender"],
                    patient["blood_type"],
                    patient["contact_email"]
                ))
        return generated_patient_id # ارجاع المعرف
    except Exception as e:
        print(f"Database Error: {e}")
        return None
    
# ── Ingestion monitor queries ──────────────────────────────────────────────────
@st.cache_data(ttl=60, show_spinner=False)
def load_ingestion_stats() -> pd.DataFrame:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    source_name,
                    file_format,
                    total_rows_loaded,
                    total_rows_rejected,
                    last_file_loaded,
                    last_ingested_at,
                    is_active
                FROM workspace.healthcare_platform.metadata_config
                ORDER BY last_ingested_at DESC NULLS LAST
            """)
            return pd.DataFrame(
                cur.fetchall(),
                columns=[d[0] for d in cur.description]
            )


@st.cache_data(ttl=60, show_spinner=False)
def load_rejected_records(source_filter: str = "ALL") -> pd.DataFrame:
    with get_connection() as conn:
        with conn.cursor() as cur:
            where = (
                f"WHERE source_name = '{source_filter}'"
                if source_filter != "ALL" else ""
            )
            cur.execute(f"""
                SELECT
                    rejection_id,
                    source_name,
                    file_name,
                    national_id_value,
                    rejection_reason,
                    rejected_at
                FROM workspace.healthcare_platform.rejected_records
                {where}
                ORDER BY rejected_at DESC
                LIMIT 500
            """)
            return pd.DataFrame(
                cur.fetchall(),
                columns=[d[0] for d in cur.description]
            )
def merge_csv_to_patients(source_name: str = "patients_csv") -> int:
    staging_table = f"workspace.healthcare_platform.bronze_ingestion_{source_name}"
    target_table  = "workspace.healthcare_platform.patients"

    with get_connection() as conn:
        with conn.cursor() as cur:
            # تنفيذ الـ MERGE مع الأعمدة المفصولة
            cur.execute(f"""
                MERGE INTO {target_table} AS target
                USING (
                    SELECT DISTINCT
                        patient_id,
                        national_id,
                        first_name,
                        last_name,
                        date_of_birth,
                        gender,
                        blood_type,
                        contact_email,
                        current_timestamp() AS created_at
                    FROM {staging_table}
                    WHERE patient_id IS NOT NULL
                ) AS source
                ON target.patient_id = source.patient_id
                WHEN NOT MATCHED THEN
                    INSERT (
                        patient_id, 
                        national_id,
                        first_name,
                        last_name,
                        date_of_birth,
                        gender,
                        blood_type,
                        contact_email,
                        created_at
                    )
                    VALUES (
                        source.patient_id,
                        source.national_id,
                        source.first_name,
                        source.last_name,
                        source.date_of_birth,
                        source.gender,
                        source.blood_type,
                        source.contact_email,
                        source.created_at
                    )
            """)
            
            # التحقق من عدد السجلات المدمجة
            cur.execute(f"SELECT COUNT(*) FROM {staging_table} WHERE patient_id IN (SELECT patient_id FROM {target_table})")
            return cur.fetchone()[0]


def insert_surgery(national_id, surgery_data, file_path):
    """
    إدخال بيانات الجراحة بأمان في Databricks باستخدام Parameterized Query.
    """
    try:
        # تأكد من استخدام get() لتجنب KeyError
        p_name = surgery_data.get('patient_name', 'Unknown')
        
        with get_connection() as conn:
            with conn.cursor() as cur:
                # تأكد من استخدام الأعمدة الموجودة فعلياً في الجدول
                sql = """
                    INSERT INTO workspace.healthcare_platform.surgeries (
                        national_id, surgery_name, surgery_date, 
                        surgeon_name, notes, file_path
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """
                # لاحظ أننا حذفنا 'patient_name' لأن الجدول لا يعرفه
                cur.execute(sql, (
                    national_id,
                    surgery_data['surgery_name'],
                    surgery_data['surgery_date'],
                    surgery_data['surgeon_name'],
                    surgery_data['notes'],
                    file_path
))
        return True
    except Exception as e:
        st.error(f"❌ Error inserting into database: {e}")
        return False
    
@st.cache_data(ttl=60)
def load_all_patients():
    """جلب قائمة المرضى مع دمج الاسم الأول والأخير"""
    with get_connection() as conn:
        with conn.cursor() as cur:
            # نقوم بدمج العمودين باستخدام CONCAT
            cur.execute("""
                SELECT national_id, CONCAT(first_name, ' ', last_name) as full_name 
                FROM workspace.healthcare_platform.patients
            """)
            return pd.DataFrame(cur.fetchall(), columns=['national_id', 'full_name'])

# ── Patient History ───────────────────────────────────────────────────────────

def get_patient_history(national_id: str) -> dict | None:
    """
    Retrieves the full patient history for a given 14-digit national_id.

    Returns a dict with keys:
        patient_id  – UUID str
        vitals      – DataFrame (last 3 readings, newest first)
        diagnostics – DataFrame
        surgeries   – DataFrame
        medications – DataFrame

    Returns None if the national_id is not found in the patients table.
    """
    conn = get_connection()

    try:
        with conn.cursor() as cur:

            # ── Step A: resolve UUID from patients table ──────────────────────
            cur.execute(
                f"""
                SELECT patient_id
                FROM   {SCHEMA}.patients
                WHERE  national_id = ?
                LIMIT  1
                """,
                [national_id],
            )
            row = cur.fetchone()

        if row is None:
            return None          # patient not found

        patient_uuid = row[0]

        with conn.cursor() as cur:

            # ── Step B-1: Vitals (UUID-keyed, last 3) ────────────────────────
            cur.execute(
                f"""
                SELECT recorded_at, systolic_bp, diastolic_bp, heart_rate
                FROM   {SCHEMA}.vitals
                WHERE  patient_id = ?
                ORDER  BY recorded_at DESC
                LIMIT  3
                """,
                [patient_uuid],
            )
            vitals_df = _cursor_to_df(
                cur,
                ["Recorded At", "Systolic BP", "Diastolic BP", "Heart Rate"],
            )

            # ── Step B-2: Medications (UUID-keyed) ───────────────────────────
            cur.execute(
                f"""
                SELECT prescribed_at, drug_name
                FROM   {SCHEMA}.medications
                WHERE  patient_id = ?
                ORDER  BY prescribed_at DESC
                """,
                [patient_uuid],
            )
            medications_df = _cursor_to_df(cur, ["Date", "Drug Name"])
            medications_df["Type"] = "Medication"

            # ── Step C-1: Diagnostics (national_id-keyed) ────────────────────
            cur.execute(
                f"""
                SELECT diagnostic_date, diagnostic_name, result_status
                FROM   {SCHEMA}.diagnostic_records
                WHERE  patient_id = ?
                ORDER  BY diagnostic_date DESC
                """,
                [national_id],
            )
            diagnostics_df = _cursor_to_df(
                cur, ["Date", "Diagnostic Name", "Result Status"]
            )
            diagnostics_df["Type"] = "Diagnostic"

            # ── Step C-2: Surgeries (national_id-keyed) ───────────────────────
            cur.execute(
                f"""
                SELECT surgery_date, surgery_name
                FROM   {SCHEMA}.surgeries
                WHERE  national_id = ?
                ORDER  BY surgery_date DESC
                """,
                [national_id],
            )
            surgeries_df = _cursor_to_df(cur, ["Date", "Surgery Name"])
            surgeries_df["Type"] = "Surgery"

    finally:
        conn.close()

    return {
        "patient_id":  patient_uuid,
        "vitals":      vitals_df,
        "diagnostics": diagnostics_df,
        "surgeries":   surgeries_df,
        "medications": medications_df,
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _cursor_to_df(cursor, columns: list[str]) -> pd.DataFrame:
    """Converts a Databricks cursor result to a tidy DataFrame."""
    rows = cursor.fetchall()
    if rows:
        return pd.DataFrame(rows, columns=columns)
    return pd.DataFrame(columns=columns)


def _build_medical_history(history: dict) -> pd.DataFrame:
    """
    Merges Diagnostics, Surgeries, and Medications into a single
    chronological 'Medical History' table, sorted newest-first.
    """
    diag = history["diagnostics"][["Date", "Type", "Diagnostic Name"]].rename(
        columns={"Diagnostic Name": "Description"}
    ).copy()

    surg = history["surgeries"][["Date", "Type", "Surgery Name"]].rename(
        columns={"Surgery Name": "Description"}
    ).copy()

    meds = history["medications"][["Date", "Type", "Drug Name"]].rename(
        columns={"Drug Name": "Description"}
    ).copy()

    # ── Normalise dates BEFORE concat to avoid mixed-type errors ──────────────
    # utc=True handles tz-aware Spark Timestamps; convert_dtypes handles
    # edge cases where Databricks returns date objects instead of strings.
    for df in (diag, surg, meds):
        df["Date"] = pd.to_datetime(
            df["Date"].astype(str),   # stringify first — flattens all types
            errors="coerce",
            utc=True,
        ).dt.tz_localize(None)        # strip tz so all three are tz-naive

    combined = pd.concat([diag, surg, meds], ignore_index=True)
    combined.sort_values("Date", ascending=False, inplace=True)
    combined["Date"] = combined["Date"].dt.strftime("%Y-%m-%d")
    return combined[["Date", "Type", "Description"]].reset_index(drop=True)       
