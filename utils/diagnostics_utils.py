# utils/diagnostics_utils.py
import os
import uuid
import base64
import pandas as pd
import streamlit as st
from datetime import datetime, timezone


def get_connection():
    from databricks import sql
    host      = os.environ.get("DATABRICKS_HOST")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH")
    token     = os.environ.get("DATABRICKS_TOKEN")

    if not host or not http_path:
        try:
            host      = st.secrets["databricks"]["server_hostname"]
            http_path = st.secrets["databricks"]["http_path"]
            token     = st.secrets["databricks"]["access_token"]
        except Exception:
            raise RuntimeError("Missing connection config.")

    connect_args = {
        "server_hostname": host,
        "http_path":       http_path,
        "_socket_timeout": 30,
    }
    if token:
        connect_args["access_token"] = token

    from databricks import sql as dbsql
    return dbsql.connect(**connect_args)


# ── Diagnostic types ───────────────────────────────────────────────────────────
DIAGNOSTIC_TYPES = {
    "LAB": [
        "Complete Blood Count (CBC)",
        "Basic Metabolic Panel",
        "Comprehensive Metabolic Panel",
        "Lipid Panel",
        "Thyroid Function (TSH/T3/T4)",
        "Liver Function Tests",
        "Kidney Function Tests",
        "HbA1c",
        "Blood Glucose",
        "Urine Analysis",
        "Blood Culture",
        "COVID-19 PCR",
        "Pregnancy Test",
        "Other Lab Test"
    ],
    "XRAY": [
        "Chest X-Ray",
        "Abdominal X-Ray",
        "Spine X-Ray",
        "Knee X-Ray",
        "Hip X-Ray",
        "Hand/Wrist X-Ray",
        "Foot/Ankle X-Ray",
        "Shoulder X-Ray",
        "Other X-Ray"
    ],
    "CT": [
        "CT Brain",
        "CT Chest",
        "CT Abdomen & Pelvis",
        "CT Spine",
        "CT Coronary Angiography",
        "CT Sinuses",
        "CT Neck",
        "CT Extremity",
        "Other CT Scan"
    ],
    "MRI": [
        "MRI Brain",
        "MRI Spine (Cervical)",
        "MRI Spine (Lumbar)",
        "MRI Knee",
        "MRI Shoulder",
        "MRI Abdomen",
        "MRI Pelvis",
        "MRI Heart (Cardiac MRI)",
        "MRI Breast",
        "Other MRI"
    ],
    "ULTRASOUND": [
        "Abdominal Ultrasound",
        "Pelvic Ultrasound",
        "Thyroid Ultrasound",
        "Echocardiogram",
        "Carotid Doppler",
        "Renal Ultrasound",
        "Obstetric Ultrasound",
        "Scrotal Ultrasound",
        "Breast Ultrasound",
        "Other Ultrasound"
    ],
    "ECG": [
        "12-Lead ECG",
        "Holter Monitor (24hr)",
        "Stress Test ECG",
        "Other ECG"
    ],
    "PATHOLOGY": [
        "Biopsy Report",
        "Cytology",
        "Histopathology",
        "PAP Smear",
        "Other Pathology"
    ],
    "OTHER": [
        "Pulmonary Function Test",
        "Bone Density (DEXA)",
        "Mammography",
        "Colonoscopy Report",
        "Endoscopy Report",
        "Ophthalmology Report",
        "Audiometry",
        "Other Diagnostic"
    ]
}

RESULT_STATUSES = ["PENDING", "NORMAL", "ABNORMAL", "CRITICAL"]

# Max file size: 4MB (base64 overhead ~33%)
MAX_FILE_SIZE_MB = 4


# ── Encode uploaded file to base64 ────────────────────────────────────────────
def encode_file_to_base64(uploaded_file) -> tuple[str, float, str]:
    """
    Encodes uploaded file to base64 string for storage in Delta.
    Returns (base64_string, file_size_kb, file_type).
    """
    file_bytes   = uploaded_file.getbuffer()
    file_size_kb = round(len(file_bytes) / 1024, 2)
    file_size_mb = file_size_kb / 1024

    if file_size_mb > MAX_FILE_SIZE_MB:
        raise ValueError(
            f"File too large: {file_size_mb:.1f}MB. "
            f"Maximum allowed: {MAX_FILE_SIZE_MB}MB."
        )

    base64_str = base64.b64encode(file_bytes).decode("utf-8")
    file_type  = uploaded_file.name.split(".")[-1].upper()

    return base64_str, file_size_kb, file_type


# ── Decode base64 back to bytes for download ───────────────────────────────────
def decode_base64_to_bytes(base64_str: str) -> bytes:
    return base64.b64decode(base64_str.encode("utf-8"))


# ── Insert diagnostic record ───────────────────────────────────────────────────
def insert_diagnostic(record: dict) -> str:
    """
    Inserts diagnostic record with optional base64 file content.
    Returns the diagnostic_id.
    """
    def esc(val):
        if val is None:
            return "NULL"
        return f"'{str(val).replace(chr(39), chr(39)*2)}'"

    # file_content can be very long — handle separately
    file_content = record.get("file_content")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO workspace.healthcare_platform.diagnostic_records (
                    diagnostic_id, patient_id, diagnostic_type,
                    diagnostic_name, diagnostic_date, result_summary,
                    result_status, ordering_doctor, performing_lab,
                    notes, file_name, file_path, file_type,
                    file_size_kb, file_content,
                    created_at, created_by, last_updated
                ) VALUES (
                    {esc(record.get("diagnostic_id"))},
                    {esc(record.get("patient_id"))},
                    {esc(record.get("diagnostic_type"))},
                    {esc(record.get("diagnostic_name"))},
                    {esc(record.get("diagnostic_date"))},
                    {esc(record.get("result_summary"))},
                    {esc(record.get("result_status", "PENDING"))},
                    {esc(record.get("ordering_doctor"))},
                    {esc(record.get("performing_lab"))},
                    {esc(record.get("notes"))},
                    {esc(record.get("file_name"))},
                    {esc(record.get("file_path"))},
                    {esc(record.get("file_type"))},
                    {record.get("file_size_kb", 0)},
                    {esc(file_content)},
                    current_timestamp(),
                    {esc(record.get("created_by", "patient"))},
                    current_timestamp()
                )
            """)

    return record["diagnostic_id"]


# ── Insert structured lab values ───────────────────────────────────────────────
def insert_lab_values(
    diagnostic_id: str,
    patient_id: str,
    lab_rows: list[dict]
) -> bool:
    with get_connection() as conn:
        with conn.cursor() as cur:
            for row in lab_rows:
                if not row.get("name"):
                    continue
                is_abnormal = (
                    row.get("value") is not None and (
                        (row.get("ref_min") is not None and
                         float(row["value"]) < float(row["ref_min"])) or
                        (row.get("ref_max") is not None and
                         float(row["value"]) > float(row["ref_max"]))
                    )
                )
                val     = row["value"]     if row.get("value")     is not None else "NULL"
                ref_min = row["ref_min"]   if row.get("ref_min")   is not None else "NULL"
                ref_max = row["ref_max"]   if row.get("ref_max")   is not None else "NULL"

                cur.execute(f"""
                    INSERT INTO workspace.healthcare_platform.lab_results (
                        lab_result_id, diagnostic_id, patient_id,
                        test_name, test_value, test_unit,
                        reference_min, reference_max,
                        is_abnormal, recorded_at
                    ) VALUES (
                        '{str(uuid.uuid4())}',
                        '{diagnostic_id}',
                        '{patient_id}',
                        '{row["name"].replace("'", "''")}',
                        {val},
                        '{row.get("unit", "").replace("'", "''")}',
                        {ref_min},
                        {ref_max},
                        {str(is_abnormal).upper()},
                        current_timestamp()
                    )
                """)
    return True


# ── Load file content for download ────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner=False)
def load_file_content(diagnostic_id: str) -> tuple[str, str, str]:
    """
    Returns (base64_content, file_name, file_type) for a diagnostic.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT file_content, file_name, file_type
                FROM workspace.healthcare_platform.diagnostic_records
                WHERE diagnostic_id = '{diagnostic_id}'
                  AND file_content IS NOT NULL
            """)
            row = cur.fetchone()
            if row:
                return row[0], row[1], row[2]
            return None, None, None


# ── Load patient diagnostics ───────────────────────────────────────────────────
@st.cache_data(ttl=60, show_spinner=False)
def load_patient_diagnostics(patient_id: str) -> pd.DataFrame:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT
                    diagnostic_id,
                    diagnostic_type,
                    diagnostic_name,
                    diagnostic_date,
                    result_status,
                    result_summary,
                    ordering_doctor,
                    performing_lab,
                    file_name,
                    file_type,
                    file_size_kb,
                    notes,
                    created_at
                FROM workspace.healthcare_platform.diagnostic_records
                WHERE patient_id = '{patient_id}'
                ORDER BY diagnostic_date DESC, created_at DESC
            """)
            return pd.DataFrame(
                cur.fetchall(),
                columns=[d[0] for d in cur.description]
            )


# ── Load all diagnostics for doctor ───────────────────────────────────────────
@st.cache_data(ttl=60, show_spinner=False)
def load_all_diagnostics(
    diag_type: str = "ALL",
    status: str = "ALL"
) -> pd.DataFrame:
    with get_connection() as conn:
        with conn.cursor() as cur:
            where_clauses = []
            if diag_type != "ALL":
                where_clauses.append(
                    f"d.diagnostic_type = '{diag_type}'"
                )
            if status != "ALL":
                where_clauses.append(
                    f"d.result_status = '{status}'"
                )
            where = (
                "WHERE " + " AND ".join(where_clauses)
                if where_clauses else ""
            )
            cur.execute(f"""
                SELECT * FROM workspace.healthcare_platform.vw_patient_diagnostics
                {where}
                LIMIT 500
            """)
            return pd.DataFrame(
                cur.fetchall(),
                columns=[d[0] for d in cur.description]
            )


# ── Load lab results ───────────────────────────────────────────────────────────
@st.cache_data(ttl=60, show_spinner=False)
def load_lab_results(diagnostic_id: str) -> pd.DataFrame:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT
                    test_name,
                    test_value,
                    test_unit,
                    reference_min,
                    reference_max,
                    is_abnormal,
                    recorded_at
                FROM workspace.healthcare_platform.lab_results
                WHERE diagnostic_id = '{diagnostic_id}'
                ORDER BY test_name
            """)
            return pd.DataFrame(
                cur.fetchall(),
                columns=[d[0] for d in cur.description]
            )


# ── Diagnostic stats ───────────────────────────────────────────────────────────
@st.cache_data(ttl=120, show_spinner=False)
def load_diagnostic_stats() -> pd.DataFrame:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    diagnostic_type,
                    COUNT(*)                                    AS total,
                    COUNT_IF(result_status = 'ABNORMAL'
                          OR result_status = 'CRITICAL')        AS abnormal_count,
                    COUNT_IF(result_status = 'PENDING')         AS pending_count,
                    MAX(diagnostic_date)                        AS latest_date
                FROM workspace.healthcare_platform.diagnostic_records
                GROUP BY diagnostic_type
                ORDER BY total DESC
            """)
            return pd.DataFrame(
                cur.fetchall(),
                columns=[d[0] for d in cur.description]
            )