-- models/bronze/bronze_patients.sql
-- Ingests raw patients; applies COALESCE simulation for null/blank defense

{{
  config(
    materialized = 'table',
    tags = ['bronze', 'patients']
  )
}}

WITH source AS (
    SELECT * FROM {{ source('healthcare_platform', 'patients') }}
),

coalesced AS (
    SELECT
        patient_id,
        national_id,

        -- COALESCE chain: prefer first_name, fall back to 'UNKNOWN'
        COALESCE(
            NULLIF(TRIM(first_name), ''),
            'UNKNOWN'
        )                                                       AS first_name,

        COALESCE(
            NULLIF(TRIM(last_name), ''),
            'UNKNOWN'
        )                                                       AS last_name,

        date_of_birth,

        COALESCE(
            NULLIF(UPPER(TRIM(gender)), ''),
            'UNSPECIFIED'
        )                                                       AS gender,

        COALESCE(
            NULLIF(TRIM(blood_type), ''),
            'UNKNOWN'
        )                                                       AS blood_type,

        contact_email,
        created_at,

        -- Audit columns for lineage
        current_timestamp()                                     AS _bronze_loaded_at,
        '{{ source("healthcare_platform", "patients") }}'            AS _source_table,
        '{{ invocation_id }}'                                   AS _dbt_run_id

    FROM source
)

SELECT * FROM coalesced