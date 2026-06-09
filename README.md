# Healthcare Data Engineering Project (dbt & Modern Data Stack)

## Overview
This project focuses on building a robust, automated healthcare data pipeline. It leverages **dbt (data build tool)** to transform raw medical records into clean, analytical-ready data, following the Medallion Architecture (Bronze, Silver, Gold layers).

## Key Features
- **Metadata-Driven Pipeline:** Dynamic ingestion process ensuring scalability.
- **Data Quality Gates:** Automated validation checks (e.g., `validate_national_id`) to maintain data integrity.
- **Medallion Architecture:** 
    - **Bronze:** Raw data ingestion.
    - **Silver:** Cleaned and standardized medical records.
    - **Gold:** Aggregated healthcare KPIs and patient risk scoring.
- **Orchestration:** Integrated with scheduling and monitoring for proactive anomaly detection.
- ## Web Application
The project includes a **real-time embedded dashboard** built with **Streamlit** and connected to the transformation layer.
- **URL:** [Healthcare Dashboard](https://healthcare-dbt-project-ercpyzvqn3cwoncvqybd34.streamlit.app/)
- **Functionality:** Visualizes healthcare KPIs, patient risk scoring, and data quality metrics in real-time.
- **Integration:** Directly reflects the output of our dbt transformation models, providing an end-to-end view of patient health data.

## Technology Stack
- **Data Transformation:** dbt (PostgreSQL/T-SQL)
- **Orchestration:** Automated Pipeline Runs
- **Analytics:** Risk Scoring & Anomaly Detection
- **Cloud/Environment:** Databricks & Streamlit (for visualization)

## Project Structure
```text
├── models/
│   ├── staging/      # Initial cleaning and type casting
│   ├── marts/        # Aggregated business logic for healthcare KPIs
│   └── docs/         # Documentation and schema definitions
├── tests/            # Data quality tests (schema & uniqueness)
└── dbt_project.yml   # Configuration and orchestration settings
