{{
  config(
    materialized = 'incremental',
    unique_key = 'patient_id',
    incremental_strategy = 'merge',
    tags = ['silver']
  )
}}

WITH bronze AS (
    SELECT * FROM {{ ref('bronze_patients') }}
),

deduped AS (
    SELECT *,
        -- فك التكرار التاريخي بناءً على national_id لضمان أخذ أحدث تعديل لبيانات نفس المريض
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(national_id, 'UNKNOWN_ID')
            ORDER BY _bronze_loaded_at DESC
        ) AS rn
    FROM bronze
    
    {% if is_incremental() %}
    -- صمام أمان لـ dbt: معالجة البيانات الجديدة فقط التي دخلت للبرونز بعد آخر تشغيل ناجح
    WHERE _bronze_loaded_at > (SELECT MAX(_silver_loaded_at) FROM {{ this }})
    {% endif %}
),

cleaned AS (
    SELECT
        patient_id,
        COALESCE(national_id, 'UNKNOWN_ID') AS national_id,
        
        INITCAP(first_name)                                   AS first_name,
        INITCAP(last_name)                                    AS last_name,
        CONCAT(INITCAP(first_name), ' ', INITCAP(last_name))  AS full_name,
        date_of_birth,
        -- حساب العمر بدقة أكبر باستخدام FLOOR
        FLOOR(DATEDIFF(current_date(), date_of_birth) / 365.25) AS age_years,
        
        CASE 
            WHEN UPPER(TRIM(gender)) IN ('MALE', 'M') THEN 'M'
            WHEN UPPER(TRIM(gender)) IN ('FEMALE', 'F') THEN 'F'
            ELSE 'UNSPECIFIED'
        END                                                   AS gender,
        
        blood_type,
        LOWER(contact_email)                                  AS contact_email,
        created_at,
        _bronze_loaded_at,
        current_timestamp()                                   AS _silver_loaded_at
    FROM deduped
    WHERE rn = 1
)

SELECT * FROM cleaned