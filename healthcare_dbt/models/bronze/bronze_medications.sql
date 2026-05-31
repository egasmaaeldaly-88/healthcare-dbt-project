-- models/bronze/bronze_medications.sql
{{
  config(
    materialized = 'table',
    tags = ['bronze', 'medications'],
    incremental_strategy = 'merge',
    unique_key = 'med_id'
  )
}}

WITH source AS (
    SELECT * FROM healthcare_platform.medications
    {% if is_incremental() %}
    WHERE prescribed_at > (SELECT MAX(_bronze_loaded_at) FROM {{ this }})
    {% endif %}
)

SELECT
    med_id,
    patient_id,

    -- Defend against blank/null drug names at ingestion time
    COALESCE(
        NULLIF(TRIM(drug_name), ''),
        'UNKNOWN'
    )                                                               AS drug_name,

    -- Defend against null dosage — keep NULL rather than defaulting
    -- (a wrong dosage default is clinically dangerous)
    dosage_mg,

    COALESCE(
        NULLIF(TRIM(frequency), ''),
        'unknown'
    )                                                               AS frequency,

    prescribed_at,

    COALESCE(
        NULLIF(TRIM(prescribing_doc), ''),
        'UNASSIGNED'
    )                                                               AS prescribing_doc,

    current_timestamp()                                             AS _bronze_loaded_at,
    '{{ invocation_id }}'                                           AS _dbt_run_id

FROM source