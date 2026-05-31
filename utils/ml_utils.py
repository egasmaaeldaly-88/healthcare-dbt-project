# utils/ml_utils.py
import pandas as pd
import streamlit as st
import os


def get_connection():
    from databricks import sql
    host      = os.environ.get("DATABRICKS_HOST")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH")
    token     = os.environ.get("DATABRICKS_TOKEN")

    if not host or not http_path:
        host      = st.secrets["databricks"]["server_hostname"]
        http_path = st.secrets["databricks"]["http_path"]
        token     = st.secrets["databricks"]["access_token"]

    connect_args = {
        "server_hostname": host,
        "http_path":       http_path,
    }
    if token:
        connect_args["access_token"] = token

    return sql.connect(**connect_args)


@st.cache_data(ttl=300, show_spinner=False)
def load_risk_predictions() -> pd.DataFrame:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    p.patient_id,
                    p.predicted_risk,
                    ROUND(p.confidence_high   * 100, 1) AS pct_high,
                    ROUND(p.confidence_medium * 100, 1) AS pct_medium,
                    ROUND(p.confidence_low    * 100, 1) AS pct_low,
                    p.scored_at,
                    ps.full_name,
                    ps.age_years,
                    ps.gender
                FROM healthcare_platform.patient_risk_predictions p
                JOIN gold.gold_patient_summary ps
                  ON p.patient_id = ps.patient_id
                ORDER BY p.confidence_high DESC
            """)
            return pd.DataFrame(
                cur.fetchall(),
                columns=[d[0] for d in cur.description]
            )


@st.cache_data(ttl=120, show_spinner=False)
def load_anomalies(severity: str = "ALL") -> pd.DataFrame:
    with get_connection() as conn:
        with conn.cursor() as cur:
            where = (
                f"WHERE af.severity = '{severity}'"
                if severity != "ALL" else ""
            )
            cur.execute(f"""
                SELECT
                    af.patient_id,
                    ps.full_name,
                    af.anomaly_type,
                    af.metric_name,
                    af.metric_value,
                    af.expected_range,
                    af.severity,
                    af.detected_at
                FROM healthcare_platform.anomaly_flags af
                JOIN gold.gold_patient_summary ps
                  ON af.patient_id = ps.patient_id
                {where}
                ORDER BY
                    CASE af.severity
                        WHEN 'CRITICAL' THEN 1
                        ELSE 2
                    END,
                    af.detected_at DESC
                LIMIT 200
            """)
            return pd.DataFrame(
                cur.fetchall(),
                columns=[d[0] for d in cur.description]
            )


@st.cache_data(ttl=600, show_spinner=False)
def load_model_metrics() -> pd.DataFrame:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    run_id,
                    model_name,
                    ROUND(accuracy * 100, 2) AS accuracy_pct,
                    ROUND(f1_score * 100, 2) AS f1_pct,
                    training_rows,
                    trained_at
                FROM healthcare_platform.model_metrics
                ORDER BY trained_at DESC
                LIMIT 10
            """)
            return pd.DataFrame(
                cur.fetchall(),
                columns=[d[0] for d in cur.description]
            )