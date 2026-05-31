{{ config(materialized='table', schema='silver') }}

WITH bronze AS (
    SELECT * FROM workspace.bronze.bronze_vitals
),

validated AS (
    SELECT
        vital_id,
        patient_id,
        recorded_at,
        CASE WHEN systolic_bp  BETWEEN 60  AND 250 THEN systolic_bp  END AS systolic_bp,
        CASE WHEN diastolic_bp BETWEEN 40  AND 150 THEN diastolic_bp END AS diastolic_bp,
        CASE WHEN heart_rate   BETWEEN 30  AND 220 THEN heart_rate   END AS heart_rate,
        CASE WHEN temperature_c BETWEEN 34 AND 43  THEN temperature_c END AS temperature_c,
        CASE WHEN spo2_pct     BETWEEN 70  AND 100 THEN spo2_pct     END AS spo2_pct,
        weight_kg,
        source_system,
        _bronze_loaded_at,
        current_timestamp() AS _silver_loaded_at
    FROM bronze
)

SELECT * FROM validated