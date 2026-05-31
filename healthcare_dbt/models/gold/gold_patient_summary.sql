-- models/gold/gold_patient_summary.sql
{{
  config(materialized='table', tags=['gold'])
}}

WITH patients AS (
    SELECT * FROM {{ ref('silver_patients') }}
),

vitals_agg AS (
    SELECT
        patient_id,
        COUNT(*)                                AS total_readings,
        ROUND(AVG(systolic_bp), 1)              AS avg_systolic_bp,
        ROUND(AVG(diastolic_bp), 1)             AS avg_diastolic_bp,
        ROUND(AVG(heart_rate), 1)               AS avg_heart_rate,
        ROUND(AVG(spo2_pct), 2)                 AS avg_spo2_pct,
        MAX(recorded_at)                        AS last_reading_at
    FROM {{ ref('silver_vitals') }}
    GROUP BY patient_id
),

meds_agg AS (
    SELECT
        patient_id,
        COUNT(DISTINCT drug_name)               AS active_medications,
        COLLECT_LIST(drug_name)                 AS medication_list
    FROM {{ ref('silver_medications') }}
    GROUP BY patient_id
)

SELECT
    p.patient_id,
    p.full_name,
    p.age_years,
    p.gender,
    p.blood_type,
    v.total_readings,
    v.avg_systolic_bp,
    v.avg_diastolic_bp,
    v.avg_heart_rate,
    v.avg_spo2_pct,
    v.last_reading_at,
    COALESCE(m.active_medications, 0)           AS active_medications,
    COALESCE(m.medication_list, ARRAY())        AS medication_list,
    current_timestamp()                         AS _gold_loaded_at

FROM patients p
LEFT JOIN vitals_agg  v ON p.patient_id = v.patient_id
LEFT JOIN meds_agg    m ON p.patient_id = m.patient_id