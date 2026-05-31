-- models/gold/gold_risk_scores.sql
{{
  config(materialized='table', tags=['gold'])
}}

WITH base AS (
    SELECT * FROM {{ ref('gold_patient_summary') }}
)

SELECT
    patient_id,
    full_name,
    age_years,

    -- Simple rule-based risk scoring (extend with ML later)
    CASE
        WHEN avg_systolic_bp > 140 AND avg_diastolic_bp > 90   THEN 'HIGH'
        WHEN avg_systolic_bp > 120 OR  avg_diastolic_bp > 80   THEN 'MEDIUM'
        WHEN avg_spo2_pct < 94                                  THEN 'HIGH'
        WHEN age_years > 65 AND active_medications > 3         THEN 'MEDIUM'
        ELSE 'LOW'
    END                                                         AS risk_level,

    ROUND(
        (COALESCE(avg_systolic_bp, 120) - 80) / 2.0 +
        GREATEST(0, COALESCE(age_years, 0) - 50) * 0.5 +
        (COALESCE(active_medications, 0) * 2)
    , 1)                                                        AS composite_risk_score,

    avg_systolic_bp,
    avg_diastolic_bp,
    avg_spo2_pct,
    active_medications,
    last_reading_at,
    _gold_loaded_at

FROM base