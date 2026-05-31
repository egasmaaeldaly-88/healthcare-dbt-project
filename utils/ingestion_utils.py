# utils/ingestion_utils.py
import json
import re
import os
from datetime import datetime, timezone
from databricks import sql
import pandas as pd
import streamlit as st


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


# ── Metadata loader ────────────────────────────────────────────────────────────
@st.cache_data(ttl=120, show_spinner=False)
def load_source_config(source_name: str) -> dict:
    """Load one source config row from metadata_config."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT * FROM workspace.healthcare_platform.metadata_config
                WHERE source_name = '{source_name}'
                  AND is_active = true
            """)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]

    if not rows:
        raise ValueError(f"Source '{source_name}' not found or inactive.")

    config = dict(zip(cols, rows[0]))

    for json_col in ["expected_columns", "optional_columns",
                     "coalesce_fields", "column_mapping"]:
        raw = config.get(json_col)
        if raw:
            try:
                config[json_col] = json.loads(raw)
            except Exception:
                config[json_col] = [] if json_col != "column_mapping" else {}
        else:
            config[json_col] = [] if json_col != "column_mapping" else {}

    return config


# ── National ID validator (single value) ──────────────────────────────────────
def validate_national_id(value: str, length: int = 14) -> tuple[bool, str]:
    """
    Returns (is_valid, reason).
    Used for single-row form validation in the registration form.
    """
    if not value or value.strip() == "":
        return False, "National ID cannot be empty."
    if not value.isdigit():
        return False, "National ID must contain digits only."
    if len(value) != length:
        return False, f"National ID must be exactly {length} digits (got {len(value)})."
    return True, ""

# utils/ingestion_utils.py

def patient_exists(patient_id: str) -> bool:
    """
    تتحقق ما إذا كان المريض مسجلاً مسبقاً في قاعدة البيانات.
    """
    try:
        # ملاحظة: إذا كنتِ قمتِ بتعريف get_db_connection (مع cache_resource)،
        # استخدميها هنا لضمان السرعة.
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT COUNT(1) AS n
                    FROM healthcare_platform.patients
                    WHERE patient_id = '{patient_id}'
                """)
                row = cur.fetchone()
                return row[0] > 0
    except Exception as e:
        # إظهار خطأ في واجهة Streamlit إذا فشل الاتصال
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

    # ── Write valid rows ───────────────────────────────────────────────────────
    if not valid_df.empty:
        valid_df["_source_file"]      = file_name
        valid_df["_source_name"]      = source_name
        valid_df["_ingested_at"]      = datetime.now(timezone.utc).isoformat()
        valid_df["_ingestion_run_id"] = run_id

        target_table = f"workspace.healthcare_platform.bronze_ingestion_{source_name}"

        with get_connection() as conn:
            with conn.cursor() as cur:
                # Create table on first run if needed
                cols_ddl = ",\n".join(
                    [f"`{c}` STRING" for c in valid_df.columns]
                )
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {target_table} ({cols_ddl})
                    USING DELTA
                """)

                # Insert rows
                for _, row in valid_df.iterrows():
                    vals = ", ".join([
                        "NULL" if pd.isna(v)
                        else f"'{str(v).replace(chr(39), chr(39)*2)}'"
                        for v in row.values
                    ])
                    cur.execute(
                        f"INSERT INTO {target_table} VALUES ({vals})"
                    )
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

    return {
        "status":        "SUCCESS",
        "file":          file_name,
        "rows_total":    len(df),
        "rows_loaded":   rows_loaded,
        "rows_rejected": rows_rejected,
        "rejected_df":   rejected_df
    }


# ── Patient registration (single row insert) ───────────────────────────────────
def register_patient(patient: dict) -> bool:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO healthcare_platform.patients
                    (patient_id, first_name, last_name,
                     date_of_birth, gender, blood_type,
                     contact_email, created_at)
                VALUES (
                    '{patient["national_id"]}',
                    '{patient["first_name"].replace("'", "''")}',
                    '{patient["last_name"].replace("'", "''")}',
                    '{patient["date_of_birth"]}',
                    '{patient["gender"]}',
                    '{patient["blood_type"]}',
                    '{patient["contact_email"].replace("'", "''")}',
                    current_timestamp()
                )
            """)
    return True


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