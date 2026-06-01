-- models/bronze/bronze_vitals.sql
{{
  config(
    materialized = 'table',
    tags = ['bronze', 'vitals'],
    incremental_strategy = 'merge',
    unique_key = 'vital_id'
  )
}}

WITH source AS (
    SELECT * FROM {{ source('healthcare_platform', 'vitals') }}
    {% if is_incremental() %}
    WHERE recorded_at > (SELECT MAX(_bronze_loaded_at) FROM {{ this }})
    {% endif %}
)

SELECT
    vital_id,
    patient_id,
    recorded_at,

    -- Defend against impossible/null readings with COALESCE + NULLIF
    COALESCE(NULLIF(systolic_bp,  0), NULL)                     AS systolic_bp,
    COALESCE(NULLIF(diastolic_bp, 0), NULL)                     AS diastolic_bp,
    COALESCE(NULLIF(heart_rate,   0), NULL)                     AS heart_rate,
    COALESCE(temperature_c, 37.0)                               AS temperature_c,
    COALESCE(spo2_pct,      98.0)                               AS spo2_pct,
    weight_kg,
    COALESCE(source_system, 'manual')                           AS source_system,

    current_timestamp()                                         AS _bronze_loaded_at,
    '{{ invocation_id }}'                                       AS _dbt_run_id

FROM source