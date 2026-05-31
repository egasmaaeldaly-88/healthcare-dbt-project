-- models/silver/silver_patients.sql
{{
  config(
    materialized = 'table',
    tags = ['silver']
  )
}}

WITH bronze AS (
    SELECT * FROM {{ ref('bronze_patients') }}
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY patient_id
            ORDER BY _bronze_loaded_at DESC
        ) AS rn
    FROM bronze
),

cleaned AS (
    SELECT
        patient_id,
        INITCAP(first_name)                                     AS first_name,
        INITCAP(last_name)                                      AS last_name,
        CONCAT(INITCAP(first_name), ' ', INITCAP(last_name))   AS full_name,
        date_of_birth,
        DATEDIFF(current_date(), date_of_birth) / 365           AS age_years,
        
        -- توحيد وتنظيف قيم الجنس لتتوافق مع معايير الـ Silver والاختبارات
        CASE 
            WHEN UPPER(TRIM(gender)) = 'MALE' THEN 'M'
            WHEN UPPER(TRIM(gender)) = 'FEMALE' THEN 'F'
            ELSE 'UNSPECIFIED'
        END                                                     AS gender,
        
        blood_type,
        LOWER(contact_email)                                    AS contact_email,
        created_at,
        _bronze_loaded_at,
        current_timestamp()                                     AS _silver_loaded_at
    FROM deduped
    WHERE rn = 1
)

SELECT * FROM cleaned