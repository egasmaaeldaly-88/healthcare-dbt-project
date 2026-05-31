-- models/silver/silver_medications.sql
{{
  config(
    materialized = 'table',
    tags = ['silver']
  )
}}

WITH bronze AS (
    SELECT * FROM {{ ref('bronze_medications') }}
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY med_id
            ORDER BY _bronze_loaded_at DESC
        ) AS rn
    FROM bronze
),

cleaned AS (
    SELECT
        med_id,
        patient_id,

        -- Normalize drug name: trim, title-case, defend against blank
        INITCAP(
            COALESCE(NULLIF(TRIM(drug_name), ''), 'UNKNOWN')
        )                                                           AS drug_name,

        -- Clamp negative or zero dosages to NULL — physiologically invalid
        CASE
            WHEN dosage_mg > 0 THEN dosage_mg
            ELSE NULL
        END                                                         AS dosage_mg,

        -- Normalize frequency values to a controlled vocabulary
        CASE UPPER(TRIM(COALESCE(frequency, '')))
            WHEN 'ONCE DAILY'    THEN 'once_daily'
            WHEN 'DAILY'         THEN 'once_daily'
            WHEN 'QD'            THEN 'once_daily'
            WHEN 'TWICE DAILY'   THEN 'twice_daily'
            WHEN 'BID'           THEN 'twice_daily'
            WHEN 'THREE TIMES'   THEN 'three_times_daily'
            WHEN 'TID'           THEN 'three_times_daily'
            WHEN 'FOUR TIMES'    THEN 'four_times_daily'
            WHEN 'QID'           THEN 'four_times_daily'
            WHEN 'AS NEEDED'     THEN 'as_needed'
            WHEN 'PRN'           THEN 'as_needed'
            ELSE LOWER(TRIM(COALESCE(frequency, 'unknown')))
        END                                                         AS frequency,

        prescribed_at,

        -- Normalize prescribing doctor name
        INITCAP(
            COALESCE(NULLIF(TRIM(prescribing_doc), ''), 'UNASSIGNED')
        )                                                           AS prescribing_doc,

        _bronze_loaded_at,
        current_timestamp()                                         AS _silver_loaded_at

    FROM deduped
    WHERE rn = 1
)

SELECT * FROM cleaned