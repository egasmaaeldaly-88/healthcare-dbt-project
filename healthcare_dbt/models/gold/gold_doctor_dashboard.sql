{{
  config(
    materialized = 'table',
    tags = ['gold']
  )
}}

WITH patient_summary AS (
    SELECT * FROM {{ ref('gold_patient_summary') }}
),

risk AS (
    SELECT * FROM {{ ref('gold_risk_scores') }}
),

silver_patients_info AS (
    SELECT 
        patient_id, 
        contact_email 
    FROM {{ ref('silver_patients') }}
),

latest_vitals AS (
    SELECT
        patient_id,
        systolic_bp AS latest_systolic_bp,
        diastolic_bp AS latest_diastolic_bp,
        heart_rate AS latest_heart_rate,
        spo2_pct AS latest_spo2_pct,
        temperature_c AS latest_temperature_c,
        recorded_at AS latest_recorded_at
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY patient_id
                ORDER BY recorded_at DESC
            ) AS rn
        FROM {{ ref('silver_vitals') }}
    )
    WHERE rn = 1  -- إزالة شرط الـ IS NOT NULL الصارم للسماح بمرور القراءات بشكل أسرع
),

latest_medication AS (
    SELECT
        patient_id,
        drug_name AS latest_drug,
        dosage_mg AS latest_dosage_mg,
        frequency AS latest_frequency,
        prescribed_at AS latest_prescribed_at,
        prescribing_doc
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY patient_id
                ORDER BY prescribed_at DESC
            ) AS rn
        FROM {{ ref('silver_medications') }}
    )
    WHERE rn = 1
),

assembled AS (
    SELECT
        -- استخدام COALESCE لضمان أخذ الـ ID من أي جدول متاح لتفادي مشاكل الـ LEFT JOIN مع المرضى الجدد
        COALESCE(ps.patient_id, lv.patient_id, lm.patient_id, sp.patient_id) AS patient_id,
        COALESCE(ps.full_name, 'New Patient / No Summary Yet') AS full_name,
        ps.age_years,
        ps.gender,
        ps.blood_type,
        sp.contact_email,

        r.risk_level,
        COALESCE(r.composite_risk_score, 0) AS composite_risk_score,

        -- استخدام COALESCE لتجنب القيم الفارغة في واجهة التطبيق
        COALESCE(ps.avg_systolic_bp, 0) AS avg_systolic_bp,
        COALESCE(ps.avg_diastolic_bp, 0) AS avg_diastolic_bp,
        COALESCE(ps.avg_heart_rate, 0) AS avg_heart_rate,
        COALESCE(ps.avg_spo2_pct, 0) AS avg_spo2_pct,
        COALESCE(ps.total_readings, 0) AS total_readings,

        lv.latest_systolic_bp,
        lv.latest_diastolic_bp,
        lv.latest_heart_rate,
        lv.latest_spo2_pct,
        lv.latest_temperature_c,
        lv.latest_recorded_at,

        ps.active_medications,
        ps.medication_list,
        lm.latest_drug,
        lm.latest_dosage_mg,
        lm.latest_frequency,
        lm.latest_prescribed_at,
        lm.prescribing_doc,

        -- flags الآمنة والمحدثة
        CASE WHEN r.risk_level = 'HIGH' THEN true ELSE false END AS flag_high_risk,
        CASE WHEN lv.latest_spo2_pct < 94 THEN true ELSE false END AS flag_low_spo2,
        CASE WHEN lv.latest_systolic_bp > 140 OR lv.latest_diastolic_bp > 90 THEN true ELSE false END AS flag_high_bp,
        CASE WHEN ps.total_readings = 0 OR ps.total_readings IS NULL THEN true ELSE false END AS flag_no_readings,

        ps._gold_loaded_at,
        current_timestamp() AS _dashboard_built_at

    -- جعل التجميع يبدأ من جدول معلومات المرضى لضمان عدم سقوط أي مريض جديد مضاف في النظام
    FROM silver_patients_info sp
    LEFT JOIN patient_summary ps ON sp.patient_id = ps.patient_id
    LEFT JOIN risk r ON sp.patient_id = r.patient_id
    LEFT JOIN latest_vitals lv ON sp.patient_id = lv.patient_id
    LEFT JOIN latest_medication lm ON sp.patient_id = lm.patient_id
)

SELECT * FROM assembled
ORDER BY composite_risk_score DESC NULLS LAST