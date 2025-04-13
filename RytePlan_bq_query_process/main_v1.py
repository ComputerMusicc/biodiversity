# rp_bq_process_production

import functions_framework
from google.cloud import bigquery
from google.cloud import storage
from firebase_admin import firestore
import json
import time

# init Clients
bq_client = bigquery.Client()
fs_client = firestore.Client()
storage_client = storage.Client()

project_id = bq_client.project

MAX_WAIT_TIME = 90 # Maximum time to wait for tables to be created
CHECK_INTERVAL = 5  # Time between checks for table creation
BUCKET_NAME = "new_data_delivery_bucket" # GCS bucket name where new_uploads_ug_uf_YYYYMMDD is stored. Replace with actual bucket name.


# CloudEvent function to be triggered by an Eventarc Cloud Audit Logging trigger
# Note: this is NOT designed for second-party (Cloud Audit Logs -> Pub/Sub) triggers!
@functions_framework.cloud_event
def hello_auditlog(cloudevent):
    # Print out the CloudEvent's (required) `type` property
    # See https://github.com/cloudevents/spec/blob/v1.0.1/spec.md#type
    print(f"Event type: {cloudevent['type']}")

    # Print out the CloudEvent's (optional) `subject` property
    # See https://github.com/cloudevents/spec/blob/v1.0.1/spec.md#subject
    if 'subject' in cloudevent:
        # CloudEvent objects don't support `get` operations.
        # Use the `in` operator to verify `subject` is present.
        print(f"Subject: {cloudevent['subject']}")

    payload = cloudevent.data.get("protoPayload", {})
    resource_name = payload.get('resourceName', "")


# Print out details from the `protoPayload`
    # This field encapsulates a Cloud Audit Logging entry
    # See https://cloud.google.com/logging/docs/audit#audit_log_entry_structure

    if payload:
        print(f"API method: {payload.get('methodName')}")
        print(f"Resource name: {payload.get('resourceName')}")
        print(f"Principal: {payload.get('authenticationInfo', dict()).get('principalEmail')}")

    if not resource_name.startswith(f"projects/{project_id}/datasets/"):
        print("Invalid dataset creation event.")
        return
    
    dataset_id = resource_name.split("/")[-1]  # Extract dataset name
    print(f"Detected new dataset name: {dataset_id}")

    # Deduplication Check: Prevent Function from Running Multiple Times per Dataset
    if is_dataset_processed(dataset_id):
        print(f"Dataset {dataset_id} already processed. Skipping...")
        return

    
    # Get the expected table count dynamically from the GCS folder
    expected_table_count = count_gcs_files(dataset_id)

    if wait_for_tables(dataset_id, expected_table_count):
        print(f"All expected tables detected in {dataset_id}. Running queries...")
        run_all_queries(dataset_id)
        mark_dataset_processed(dataset_id)
        return

    else:
        print(f"Timeout: Not all tables detected in {dataset_id}. Aborting query execution.")
        return

    

def count_gcs_files(dataset_id):
    """Counts the number of uploaded CSV files in the GCS folder corresponding to the dataset."""
    blobs = storage_client.list_blobs(BUCKET_NAME, prefix=f"{dataset_id}/")
    csv_files = [blob.name for blob in blobs if blob.name.endswith(".csv")]
    print(f"Detected {len(csv_files)} CSV files in GCS for dataset {dataset_id}.")
    return len(csv_files)


def run_bigquery_query(query):
    try: 
        query_job = bq_client.query(query)
        query_job.result()  # Wait for the job to complete

        if query_job.destination:
            print(f"Query results loaded to table {query_job.destination}")
            
        else:
            print("Final tables completed successfully.")
            return


    except Exception as e:
        print(f"Error running query: {query}. Error: str{e}")
        raise


def run_all_queries(dataset_id):
    dataset_date = dataset_id.split("_")[-1]  # Extract dataset date
    ### define query jobs
    ### institutions_query must have 1 added backslash to whitespace regex to properly identify in a Python string (\s --> \\s)
    institutions_query = f"""
        CREATE OR REPLACE TABLE `{project_id}.rp_partial_joins.institutions_{dataset_date}` AS
        SELECT 
            inst.INUN_ID,
            inst.NAME,
            -- creating URI column by removing special characters, converting to lowercase, fixing double hyphens
            REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(inst.NAME), r'[^a-z0-9\\s-]', ''), r'\\s+', '-'), r'-+', '-')  
            AS URI,
            inst.LINE1,
            inst.LINE2,
            inst.LINE3,
            inst.CITY,
            -- removing state codes when country code is not USA
            CASE 
            WHEN inst.COUNTRY_CODE != 'USA' THEN ''
            ELSE inst.STATE_CODE 
            END AS STATE_CODE,
            --
            -- cleaning state names for US territories
            CASE
            WHEN inst.STATE_CODE = 'PR' THEN ''
            ELSE state_ref.DESCR
            END AS STATE_CLEANED,
            --
            inst.COUNTRY_CODE,
            country_ref.LONG_NAME as COUNTRY_NAME,
            -- create DOM_INT from cen_country_types.long_name
            CASE 
                WHEN country_ref.DESCR != 'United States' THEN 'International'
                ELSE country_ref.DESCR
            END AS DOM_INT,
            --
            inst.ZIPCODE,
            inst.OFFICIAL_AREA_CODE,
            inst.OFFICIAL_PHONE,
            inst.OFFICIAL_EXT,
            inst.AFFIL_CODE,
            affil_ref.DESCR as AFFIL_DESC,
            inst.DENOM_CODE,
            denom_ref.DESCR as DENOM_DESC,
            inst.URL_ADDRESS,
            inst.MAIN_FUNCTION_TYPE,
            inst.MAIN_INST_CONTROL,
            -- clean main inst control description 
            CASE
                WHEN main_inst_cont_codes.MAIN_INST_CONTROL_DESC LIKE '%Public%' THEN 'Public'
                WHEN main_inst_cont_codes.MAIN_INST_CONTROL_DESC LIKE '%Private%' THEN 'Private'
                ELSE main_inst_cont_codes.MAIN_INST_CONTROL_DESC
            END AS MAIN_INST_CONTROL_DESC,
            -- converting main calendar from abbreviations
            CASE 
                WHEN inst.MAIN_CALENDAR = 'SEM' THEN 'Semester'
                WHEN inst.MAIN_CALENDAR = 'QTR' THEN 'Quarter'
                WHEN inst.MAIN_CALENDAR = 'CON' THEN 'Continuous'
                WHEN inst.MAIN_CALENDAR = 'OTH' THEN 'Other'
                WHEN inst.MAIN_CALENDAR = 'TRI' THEN 'Trimesters'
                ELSE COALESCE(inst.MAIN_CALENDAR, '')  -- Keep the original value if no match
            END AS MAIN_CALENDAR,
            --
            inst.AD_EMAIL,
            inst.DIV_EQ_INC_URL
        FROM `{project_id}.{dataset_id}.ux_inst` inst
        LEFT JOIN `{project_id}.{dataset_id}.cen_state_types` state_ref 
            ON inst.STATE_CODE = state_ref.STATE_CODE
        LEFT JOIN `{project_id}.{dataset_id}.cen_country_types` country_ref 
            ON inst.COUNTRY_CODE = country_ref.COUNTRY_CODE
        LEFT JOIN `{project_id}.{dataset_id}.cen_affiliation_types` affil_ref 
            ON inst.AFFIL_CODE = affil_ref.AFFIL_CODE
        LEFT JOIN `{project_id}.{dataset_id}.cen_denomination_types` denom_ref 
            ON inst.DENOM_CODE = denom_ref.DENOM_CODE
        LEFT JOIN `{project_id}.{dataset_id}.main_inst_cont_codes` main_inst_cont_codes
            ON inst.MAIN_INST_CONTROL = main_inst_cont_codes.Code
        WHERE inst.DATASET_YR = 2024  -- filter by desired dataset year
        ORDER BY inst.INUN_ID
    """

    admissions_query = f"""
    -- pivot ug_admis_factor_asgns on AD_FACTOR_CODE values. New column values are derived from AD_FACTOR_LEVEL
    CREATE OR REPLACE TABLE `{project_id}.rp_partial_joins.admissions_{dataset_date}` AS
        WITH admissions_factor_pivot AS (
        SELECT *
        FROM (
        SELECT 
            INUN_ID,
            AD_FACTOR_CODE,
            COALESCE(key.UPDATED, asgns.AD_FACTOR_LEVEL) AS AD_FACTOR_LEVEL
        FROM `{project_id}.{dataset_id}.ug_admis_factor_asgns` asgns
        LEFT JOIN `{project_id}.rp_key_replace_tables.admin_consideration_factors_key` key
            ON asgns.AD_FACTOR_LEVEL = key.ORIGINAL
        )
        PIVOT (
            MAX(AD_FACTOR_LEVEL) 
            FOR AD_FACTOR_CODE IN (
                'ACTIV', 'ALUM', 'APINT', 'CHAR', 'ESSAY', 'FIRST', 'GEOG', 
                'GPA', 'IVIEW', 'MINOR', 'RANK', 'RECOM', 'RELIG', 'RIGOR', 
                'STATE', 'TALNT', 'TSTSC', 'VOLUN', 'WORK'
            )
        )
    ),

        -- pivot ug_expense_asgns on ACAD_YR. New column values are derived from aggregations per year
        ug_expense_asgns_pivot AS (
            SELECT 
                INUN_ID,
                ACAD_YR,

                -- Ensure only one row per (INUN_ID, ACAD_YR) before pivoting
                SUM(TUIT_NRES_1ST_FT_D) AS TUIT_NRES_1ST_FT_D,
                SUM(TUIT_STATE_1ST_FT_D) AS TUIT_STATE_1ST_FT_D,
                SUM(TUIT_NRES_FT_D) AS TUIT_NRES_FT_D,
                SUM(TUIT_STATE_FT_D) AS TUIT_STATE_FT_D,
                SUM(TUIT_INTL_FT_D) AS TUIT_INTL_FT_D,

                -- Maximum aggregations (Ensure unique values per INUN_ID)
                MAX(TOT_RES_D) AS TOT_RES_D,
                MAX(URL_ADDRESS_PRICE_CALC) AS URL_ADDRESS_PRICE_CALC,
                MAX(CURRENCY_CODE) AS CURRENCY_CODE

            FROM `{project_id}.{dataset_id}.ug_expense_asgns`
            GROUP BY INUN_ID, ACAD_YR
        ),

        ug_expense_pivoted AS (
            SELECT 
            INUN_ID,

                -- Pivoting: Ensure one row per INUN_ID
                MAX(CASE WHEN ACAD_YR = 2022 THEN TUIT_NRES_1ST_FT_D END) AS TUIT_NRES_1ST_FT_D_2022,
                MAX(CASE WHEN ACAD_YR = 2023 THEN TUIT_NRES_1ST_FT_D END) AS TUIT_NRES_1ST_FT_D_2023,
                MAX(CASE WHEN ACAD_YR = 2024 THEN TUIT_NRES_1ST_FT_D END) AS TUIT_NRES_1ST_FT_D_2024,
                MAX(CASE WHEN ACAD_YR = 2025 THEN TUIT_NRES_1ST_FT_D END) AS TUIT_NRES_1ST_FT_D_2025,

                MAX(CASE WHEN ACAD_YR = 2022 THEN TUIT_NRES_FT_D END) AS TUIT_NRES_FT_D_2022,
                MAX(CASE WHEN ACAD_YR = 2023 THEN TUIT_NRES_FT_D END) AS TUIT_NRES_FT_D_2023,
                MAX(CASE WHEN ACAD_YR = 2024 THEN TUIT_NRES_FT_D END) AS TUIT_NRES_FT_D_2024,
                MAX(CASE WHEN ACAD_YR = 2025 THEN TUIT_NRES_FT_D END) AS TUIT_NRES_FT_D_2025,

                MAX(CASE WHEN ACAD_YR = 2022 THEN TUIT_STATE_1ST_FT_D END) AS TUIT_STATE_1ST_FT_D_2022,
                MAX(CASE WHEN ACAD_YR = 2023 THEN TUIT_STATE_1ST_FT_D END) AS TUIT_STATE_1ST_FT_D_2023,
                MAX(CASE WHEN ACAD_YR = 2024 THEN TUIT_STATE_1ST_FT_D END) AS TUIT_STATE_1ST_FT_D_2024,
                MAX(CASE WHEN ACAD_YR = 2025 THEN TUIT_STATE_1ST_FT_D END) AS TUIT_STATE_1ST_FT_D_2025,

                MAX(CASE WHEN ACAD_YR = 2022 THEN TUIT_STATE_FT_D END) AS TUIT_STATE_FT_D_2022,
                MAX(CASE WHEN ACAD_YR = 2023 THEN TUIT_STATE_FT_D END) AS TUIT_STATE_FT_D_2023,
                MAX(CASE WHEN ACAD_YR = 2024 THEN TUIT_STATE_FT_D END) AS TUIT_STATE_FT_D_2024,
                MAX(CASE WHEN ACAD_YR = 2025 THEN TUIT_STATE_FT_D END) AS TUIT_STATE_FT_D_2025,

                MAX(CASE WHEN ACAD_YR = 2022 THEN TUIT_INTL_FT_D END) AS TUIT_INTL_FT_D_2022,
                MAX(CASE WHEN ACAD_YR = 2023 THEN TUIT_INTL_FT_D END) AS TUIT_INTL_FT_D_2023,
                MAX(CASE WHEN ACAD_YR = 2024 THEN TUIT_INTL_FT_D END) AS TUIT_INTL_FT_D_2024,
                MAX(CASE WHEN ACAD_YR = 2025 THEN TUIT_INTL_FT_D END) AS TUIT_INTL_FT_D_2025,

                MAX(CASE WHEN ACAD_YR = 2022 THEN TOT_RES_D END) AS TOT_RES_D_2022,
                MAX(CASE WHEN ACAD_YR = 2023 THEN TOT_RES_D END) AS TOT_RES_D_2023,
                MAX(CASE WHEN ACAD_YR = 2024 THEN TOT_RES_D END) AS TOT_RES_D_2024,
                MAX(CASE WHEN ACAD_YR = 2025 THEN TOT_RES_D END) AS TOT_RES_D_2025,

                -- URL and CURRENCY_CODE (Maximum Aggregation)
                MAX(CASE WHEN ACAD_YR = 2022 THEN URL_ADDRESS_PRICE_CALC END) AS URL_ADDRESS_PRICE_CALC_2022,
                MAX(CASE WHEN ACAD_YR = 2023 THEN URL_ADDRESS_PRICE_CALC END) AS URL_ADDRESS_PRICE_CALC_2023,
                MAX(CASE WHEN ACAD_YR = 2024 THEN URL_ADDRESS_PRICE_CALC END) AS URL_ADDRESS_PRICE_CALC_2024,
                MAX(CASE WHEN ACAD_YR = 2025 THEN URL_ADDRESS_PRICE_CALC END) AS URL_ADDRESS_PRICE_CALC_2025,

                MAX(CASE WHEN ACAD_YR = 2022 THEN CURRENCY_CODE END) AS CURRENCY_CODE_2022,
                MAX(CASE WHEN ACAD_YR = 2023 THEN CURRENCY_CODE END) AS CURRENCY_CODE_2023,
                MAX(CASE WHEN ACAD_YR = 2024 THEN CURRENCY_CODE END) AS CURRENCY_CODE_2024,
                MAX(CASE WHEN ACAD_YR = 2025 THEN CURRENCY_CODE END) AS CURRENCY_CODE_2025

            FROM ug_expense_asgns_pivot
            GROUP BY INUN_ID
        ),

        -- gather ug_admis columns

        adm_manip AS (
            SELECT 
            INUN_ID,
            AP_RECD_1ST_N,
            AP_ADMT_1ST_N,
            -- converting month number values to string names
            COALESCE(month_table.NAME, CAST(ug_admis.AP_DL_FRSH_MON AS STRING)) AS AP_DL_FRSH_MON,
                CAST(AP_DL_FRSH_DAY AS STRING) AS AP_DL_FRSH_DAY,
            COALESCE(month_table2.NAME, CAST(AP_DL_EDEC_2_MON AS STRING)) AS AP_DL_EDEC_2_MON,
                CAST(AP_DL_EDEC_2_DAY AS STRING) AS AP_DL_EDEC_2_DAY,
            COALESCE(month_table3.NAME, CAST(AP_DL_EDEC_1_MON AS STRING)) AS AP_DL_EDEC_1_MON,
                CAST(AP_DL_EDEC_1_DAY AS STRING) AS AP_DL_EDEC_1_DAY,
            COALESCE(month_table4.NAME, CAST(AP_DL_EACT_MON AS STRING)) AS AP_DL_EACT_MON,
                CAST(AP_DL_EACT_DAY AS STRING) AS AP_DL_EACT_DAY,
            COALESCE(month_table5.NAME, CAST(AP_DL_PRIO_MON AS STRING)) AS AP_DL_PRIO_MON,
                CAST(AP_DL_PRIO_DAY AS STRING) AS AP_DL_PRIO_DAY,
            -- --
            AP_RECD_EACT_N,
            AP_ADMT_EACT_N,
            AP_RECD_EDEC_N,
            AP_ADMT_EDEC_N,
            AD_VIDEO,
            COALESCE(admiss_diff_table.UPDATED, ug_admis.AD_DIFF_ALL) AS AD_DIFF_ALL,
            AD_TEST_POLICY_T,
            AP_EACT_RESTRICT,
            WAITLIST_RANK,
            AP_ADMT_1ST_N/AP_RECD_1ST_N AS ADMIT_RATE
            FROM `{project_id}.{dataset_id}.ug_admis` ug_admis
            LEFT JOIN `{project_id}.rp_key_replace_tables.admissions_difficulty_factor_key` admiss_diff_table
                ON ug_admis.AD_DIFF_ALL = admiss_diff_table.ORIGINAL
            LEFT JOIN `{project_id}.rp_key_replace_tables.month_number_to_name` month_table
                ON ug_admis.AP_DL_FRSH_MON = month_table.NUMBER
            LEFT JOIN `{project_id}.rp_key_replace_tables.month_number_to_name` month_table2
                ON ug_admis.AP_DL_EDEC_2_MON = month_table2.NUMBER
            LEFT JOIN `{project_id}.rp_key_replace_tables.month_number_to_name` month_table3
                ON ug_admis.AP_DL_EDEC_1_MON = month_table3.NUMBER
            LEFT JOIN `{project_id}.rp_key_replace_tables.month_number_to_name` month_table4
                ON ug_admis.AP_DL_EACT_MON = month_table4.NUMBER
            LEFT JOIN `{project_id}.rp_key_replace_tables.month_number_to_name` month_table5
                ON ug_admis.AP_DL_PRIO_MON = month_table5.NUMBER
        ),

        -- define a current year variable to string together app deadline dates. gather ug_admis columns
        current_year_string AS (
            SELECT FORMAT_DATE('%Y', CURRENT_DATE()) AS year_value
        ),

        adm AS (
            SELECT adm_manip.*,

            CONCAT(AP_DL_FRSH_MON," " ,AP_DL_FRSH_DAY,", ", (SELECT year_value FROM current_year_string)) AS REG_DEC_DEAD,
            
            CONCAT(AP_DL_EDEC_2_MON," " ,AP_DL_EDEC_2_DAY,", ", (SELECT year_value FROM current_year_string))
            AS EARLY_DECISION_2_DEAD,

            CONCAT(AP_DL_EDEC_1_MON," " ,AP_DL_EDEC_1_DAY,", ", (SELECT year_value FROM current_year_string))
            AS EARLY_DECISION_1_DEAD,

            CONCAT(AP_DL_EACT_MON," " ,AP_DL_EACT_DAY,", ", (SELECT year_value FROM current_year_string))
            AS EARLY_ACTION_DEADLINE,

            CONCAT(AP_DL_PRIO_MON," " ,AP_DL_PRIO_DAY,", ", (SELECT year_value FROM current_year_string))
            AS FALL_FRSH_PRIO

            FROM adm_manip
        )

    -- final admissions sub-table join  
    SELECT 
        ug_expense_pivoted.*, 
        adm.* EXCEPT(INUN_ID), 
        admissions_factor_pivot.* EXCEPT(INUN_ID),
        ug_entr_exams.SAT1_COMP_MEAN,
        ug_entr_exams.ACT_MEAN,
        ug_entr_exams.SUBMIT_SAT1_P,
        ug_entr_exams.SUBMIT_ACT_P,
        ug_entr_exams.SAT1_VERB_25TH_P,
        ug_entr_exams.SAT1_VERB_50TH_P,
        ug_entr_exams.SAT1_VERB_75TH_P,
        ug_entr_exams.SAT1_MATH_25TH_P,
        ug_entr_exams.SAT1_MATH_50TH_P,
        ug_entr_exams.SAT1_MATH_75TH_P,
        ug_entr_exams.SAT1_VERB_50TH_P + ug_entr_exams.SAT1_MATH_50TH_P AS SAT1_COMBINED_50TH,
        ug_entr_exams.ACT_COMP_25TH_P,
        ug_entr_exams.ACT_COMP_75TH_P,
        ug_entr_exams.ACT_COMP_50TH_P,
        ug_entr_exams.ADMS_OPTIONAL
    FROM ug_expense_pivoted
    LEFT JOIN adm ON adm.INUN_ID = ug_expense_pivoted.INUN_ID
    LEFT JOIN admissions_factor_pivot ON admissions_factor_pivot.INUN_ID = ug_expense_pivoted.INUN_ID
    LEFT JOIN `{project_id}.{dataset_id}.ug_entr_exams` ug_entr_exams
    ON ug_expense_pivoted.INUN_ID = ug_entr_exams.INUN_ID
    ORDER BY INUN_ID;
    """

    enrollment_query = f"""
        CREATE OR REPLACE TABLE `{project_id}.rp_partial_joins.enrollment_{dataset_date}` AS

            -- create total columns for demographics
            WITH math_ug_enroll AS (
            SELECT ug_enroll.*,
            EN_TOT_FT_MEN_N + EN_TOT_FT_WMN_N+ EN_TOT_PT_MEN_N + EN_TOT_PT_WMN_N AS UNDERGRAD_ENROLL_TOTAL,
            EN_GRAD_FT_MEN_N + EN_GRAD_FT_WMN_N + EN_GRAD_PT_MEN + EN_GRAD_PT_WMN_N AS GRADUATE_ENROLL_TOTAL,
            EN_ASIAN_NONHISPANIC_N + EN_BLACK_NONHISPANIC_N + EN_HISPANIC_ETHNICITY_N + EN_ISLANDER_NONHISPANIC_N + 
            EN_MULTIRACE_NONHISPANIC_N + EN_NATIVE_NONHISPANIC_N + EN_RACE_ETHNICITY_UNKNOWN_N + EN_WHITE_NONHISPANIC_N AS ETHNICITY_POPULATION_TOTAL
            FROM `{project_id}.{dataset_id}.ug_enroll` ug_enroll
            
            ),
            -- convert exam_codes from abbreviations
            entr_exam_asgns AS (
            SELECT * EXCEPT(EXAM_CODE), 
            -- CASE/WHEN conditions create a duplicate column unless columns are manually selected entered here. using an EXCEPT to avoid redundant typing
            CASE
                WHEN EXAM_CODE = 'ELS' THEN 'ELS'
                WHEN EXAM_CODE = 'OTHT' THEN 'Other standardized tests'
                WHEN EXAM_CODE = 'S1A' THEN 'SAT or ACT'
                WHEN EXAM_CODE = 'S1S2A' THEN 'SAT and SAT Subject Tests or ACT'
                WHEN EXAM_CODE = 'ACT' THEN 'ACT'
                WHEN EXAM_CODE = 'SAT1' THEN 'SAT'
                WHEN EXAM_CODE = 'SAT2' THEN 'SAT Subject Tests'
                WHEN EXAM_CODE = 'SAT2W' THEN 'SAT II: Writing Test'
                WHEN EXAM_CODE = 'TOEFL' THEN 'TOEFL'
                WHEN EXAM_CODE = 'TSWE' THEN 'TSWE'
                WHEN EXAM_CODE = 'S1S2' THEN 'SAT and SAT Subject Tests'
                ELSE EXAM_CODE
            END AS EXAM_CODE
            FROM `{project_id}.{dataset_id}.ug_entr_exam_asgns`
            ),

            entr_exam_asgns_final AS (
            SELECT
                INUN_ID,
                REPLACE(ARRAY_AGG(ADMS_REQ)[OFFSET(0)], 'X', STRING_AGG(EXAM_CODE, ', ')) AS ADMS_REQ,
                REPLACE(ARRAY_AGG(ADMS_CONSIDER)[OFFSET(0)], 'X', STRING_AGG(EXAM_CODE, ', ')) AS ADMS_CONSIDER,
                REPLACE(ARRAY_AGG(ADMS_NOT_USED)[OFFSET(0)], 'X', STRING_AGG(EXAM_CODE, ', ')) AS ADMS_NOT_USED
            FROM entr_exam_asgns
            GROUP BY INUN_ID
            )
            -- final table output
        SELECT
            math_ug_enroll.INUN_ID,
            math_ug_enroll.EN_FRSH_FT_MEN_N,
            math_ug_enroll.EN_FRSH_FT_WMN_N,
            math_ug_enroll.EN_GRAD_FT_MEN_N,
            math_ug_enroll.EN_GRAD_FT_WMN_N,
            math_ug_enroll.GRS_BACH_INIT_N,
            math_ug_enroll.GRS_4YR_N,
            math_ug_enroll.GRS_BACH_TOT_P,
            math_ug_enroll.EN_UG_FT_MEN_N,
            math_ug_enroll.EN_UG_FT_WMN_N,
            math_ug_enroll.EN_GRAD_PT_MEN,
            math_ug_enroll.EN_GRAD_PT_WMN_N,
            math_ug_enroll.EN_TOT_FT_MEN_N,
            math_ug_enroll.EN_TOT_FT_WMN_N,
            math_ug_enroll.EN_TOT_PT_MEN_N,
            math_ug_enroll.EN_TOT_PT_WMN_N,
            math_ug_enroll.EN_TOT_UG_N,
            math_ug_enroll.EN_NRES_P,
            math_ug_enroll.FRSH_HS_RANK_25_P,
            math_ug_enroll.FRSH_HS_RANK_10_P,
            math_ug_enroll.FRSH_GPA,
            math_ug_enroll.RETENTION_FRSH_P,
            math_ug_enroll.EN_NONRES_ALIEN_N,
            math_ug_enroll.EN_HISPANIC_ETHNICITY_N,
            math_ug_enroll.EN_BLACK_NONHISPANIC_N,
            math_ug_enroll.EN_WHITE_NONHISPANIC_N,
            math_ug_enroll.EN_NATIVE_NONHISPANIC_N,
            math_ug_enroll.EN_ASIAN_NONHISPANIC_N,
            math_ug_enroll.EN_ISLANDER_NONHISPANIC_N,
            math_ug_enroll.EN_MULTIRACE_NONHISPANIC_N,
            math_ug_enroll.EN_RACE_ETHNICITY_UNKNOWN_N,
            math_ug_enroll.GRS_BACH_INIT_PELL_N,
            math_ug_enroll.CLASS_SEC_7,
            math_ug_enroll.EN_1ST_ASIAN_NONHISPANIC_N,
            math_ug_enroll.EN_1ST_BLACK_NONHISPANIC_N,
            math_ug_enroll.EN_1ST_HISPANIC_ETHNICITY_N,
            math_ug_enroll.EN_1ST_ISLANDER_NONHISPANIC_N,
            math_ug_enroll.EN_1ST_MULTIRACE_NONHISPANIC_N,
            math_ug_enroll.EN_1ST_NATIVE_NONHISPANIC_N,
            math_ug_enroll.EN_1ST_NONRES_ALIEN_1ST_N,
            math_ug_enroll.EN_1ST_NRES_P,
            math_ug_enroll.EN_1ST_RACE_ETHNICITY_UNKNWN_N,
            math_ug_enroll.EN_1ST_WHITE_NONHISPANIC_N,
            math_ug_enroll.EN_TOT_GRAD_N,
            math_ug_enroll.UNDERGRAD_ENROLL_TOTAL,
            math_ug_enroll.GRADUATE_ENROLL_TOTAL,
            math_ug_enroll.ETHNICITY_POPULATION_TOTAL,
            entr_exam_asgns_final.* EXCEPT(INUN_ID),
            enroll_b.EN_TOT_NONRES_ALIEN_TOT_N,
            chars.ENDOW,
            REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(chars.ASSN_ATHL_NCAA, 'FALSE'),
                '1', 'NCAA Division 1'),'2', 'NCAA Division 2'),'3', 'NCAA Division 3'),'X', 'corresponding institution') 
            AS ASSN_ATHL_NCAA,
            CASE
                WHEN chars.ASSN_ATHL_NAIA = 'X' THEN 'TRUE'
                ELSE 'FALSE'
            END AS ASSN_ATHL_NAIA,
            CASE
                WHEN chars.ASSN_ATHL_NCCAA = 'X' THEN 'TRUE'
                ELSE 'FALSE'
            END AS ASSN_ATHL_NCCAA,
            INITCAP(REGEXP_REPLACE(campus.CMPS_SETTING, r'(?i)subrb', 'Suburb')) AS CMPS_SETTING,
            campus.CMPS_SIZE_N,
            campus.CMPS_SIZE_UNIT,
            campus.FRAT_P,
            campus.HOUS_1ST_UG_P,
            campus.SORO_P
            FROM math_ug_enroll
        LEFT JOIN `{project_id}.{dataset_id}.ug_enroll_b` enroll_b
            ON math_ug_enroll.INUN_ID = enroll_b.INUN_ID
        LEFT JOIN `{project_id}.{dataset_id}.ug_campus` campus
            ON enroll_b.INUN_ID = campus.INUN_ID
        LEFT JOIN `{project_id}.{dataset_id}.ug_chars` chars
            ON campus.INUN_ID = chars.INUN_ID
        LEFT JOIN entr_exam_asgns_final -- must left join asgns file last because of missing records.
            ON math_ug_enroll.INUN_ID = entr_exam_asgns_final.INUN_ID
        ORDER BY INUN_ID;
    """

    structure_query = f"""
        CREATE OR REPLACE TABLE `{project_id}.rp_partial_joins.structure_{dataset_date}` AS
            WITH fin_aid_filter AS (
                SELECT
                INUN_ID, 
                UG_FT_AVG_NB_GIFT_D,
                UG_FT_NN_NONEED_N,
                UG_FT_NN_NONEED_D
                FROM `{project_id}.{dataset_id}.ug_fin_aid`
            ),

            enroll_asgns_plus_prog_types AS (
                SELECT 
                enroll_asgns.INUN_ID,
                STRING_AGG(prog_types.DESCR, ', ') AS ACAD_PROG_DESCR
                FROM `{project_id}.{dataset_id}.ug_enroll_asgns` enroll_asgns
                LEFT JOIN `{project_id}.{dataset_id}.ug_acad_prog_types` prog_types
                ON enroll_asgns.ACAD_PROG_CODE = prog_types.ACAD_PROG_CODE
                GROUP BY INUN_ID
            ),

            ug_faculty_filter AS (
                SELECT
                INUN_ID,
                FT_N,
                PT_N,
                UG_RATIO,
                TOT_MEN_N,
                TOT_WMN_N
                FROM `{project_id}.{dataset_id}.ug_faculty`
            ),

            ug_progs_filter AS (
                SELECT
                INUN_ID,
                COOP,
                ROTC_ARMY,
                ROTC_NAVY,
                ROTC_AF,
                DEG_3_2_BUS,
                DEG_3_2_ENG,
                DEG_3_2_ENG_T
                FROM `{project_id}.{dataset_id}.ug_progs`
            )

            SELECT 
                fin_aid_filter.*,
                eappt.* EXCEPT(INUN_ID),
                ug_faculty_filter.* EXCEPT(INUN_ID),
                ug_progs_filter.* EXCEPT(INUN_ID)
            FROM fin_aid_filter
            LEFT JOIN enroll_asgns_plus_prog_types eappt
                ON fin_aid_filter.INUN_ID = eappt.INUN_ID
            LEFT JOIN ug_faculty_filter
                ON eappt.INUN_ID = ug_faculty_filter.INUN_ID
            LEFT JOIN ug_progs_filter
                ON ug_faculty_filter.INUN_ID = ug_progs_filter.INUN_ID
            ORDER BY fin_aid_filter.INUN_ID;
    """

    # institutions_petersons_query produces the `institutions_petersons_withnulls_YYYYMMDD` AND `insitutions_petersons_processed_YYYYMMDD` tables 
    institutions_petersons_query = f"""
    DECLARE dynamic_sql STRING;

    -- gather sub-join tables into a single table to rp_partial_joins but still has NULL values
    CREATE OR REPLACE TABLE `{project_id}.rp_partial_joins.institutions_petersons_withnulls_{dataset_date}` AS 
        SELECT inst.*,
            adm.* EXCEPT(INUN_ID),
            enroll.* EXCEPT(INUN_ID),
            structure.* EXCEPT(INUN_ID)
        FROM `{project_id}.rp_partial_joins.institutions_{dataset_date}` inst
        LEFT JOIN `{project_id}.rp_partial_joins.admissions_{dataset_date}` adm
            ON inst.INUN_ID = adm.INUN_ID
        LEFT JOIN `{project_id}.rp_partial_joins.enrollment_{dataset_date}` enroll
            ON adm.INUN_ID = enroll.INUN_ID
        LEFT JOIN `{project_id}.rp_partial_joins.structure_{dataset_date}` structure
        ON enroll.INUN_ID = structure.INUN_ID
        ORDER BY inst.INUN_ID;

    -- generate a dynamic_sql script that will interpret all column names from the table created above^^^
    -- this script will replace int/string column NULL values with 0 or '-' respectively
    -- then it will output the final table to the insitutions_petersons_processed database
    SET dynamic_sql = (
    SELECT CONCAT(
        'CREATE OR REPLACE TABLE `{project_id}.institutions_petersons_processed.institutions_petersons_processed_{dataset_date}` AS SELECT ', 
        STRING_AGG(
        CASE 
            WHEN data_type IN ("INT64") 
                THEN "COALESCE(CAST(" || column_name || " AS INT64), 0) AS " || column_name
            WHEN data_type in ('FLOAT64', 'NUMERIC', 'BIGNUMERIC')
                THEN "COALESCE(CAST(" || column_name || " AS FLOAT64), 0) AS " || column_name
            WHEN data_type = 'STRING' 
                THEN "COALESCE(CAST(" || column_name|| " AS STRING), '-') AS " || column_name
            ELSE column_name  -- Keep other column types unchanged
        END, ', '
        ),
        ' FROM `{project_id}.rp_partial_joins.institutions_petersons_withnulls_{dataset_date}` ORDER BY INUN_ID'
      )
      FROM `{project_id}.rp_partial_joins.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name = 'institutions_petersons_withnulls_{dataset_date}'
    );

    EXECUTE IMMEDIATE dynamic_sql;
    """


    # run queries in sequence
    run_bigquery_query(institutions_query)
    run_bigquery_query(admissions_query)
    run_bigquery_query(enrollment_query)
    run_bigquery_query(structure_query)
    run_bigquery_query(institutions_petersons_query)
    print(f"""Query results loaded to table {project_id}.rp_partial_joins.institutions_petersons_withnulls_{dataset_date} AND 
          {project_id}.institutions_petersons_processed.institutions_petersons_processed_{dataset_date}""")
        


def wait_for_tables(dataset_id, expected_count):
    """Waits for BigQuery to contain the same number of tables as the uploaded files."""
    start_time = time.time()
    
    while time.time() - start_time < MAX_WAIT_TIME:
        table_count = get_table_count(dataset_id)
        
        if table_count >= (expected_count):
            print(f"All {table_count} expected tables detected in {dataset_id}.")
            return True  # All expected tables are present

        print(f"Waiting... {table_count}/{expected_count} tables found in {dataset_id}.")
        time.sleep(CHECK_INTERVAL)  # Wait before rechecking

    print(f"Timeout reached: Only {table_count}/{expected_count} tables available in {dataset_id}.")
    return False  # Timeout reached



def get_table_count(dataset_id):
    """Returns the number of tables currently in the dataset."""
    query = f"""
    SELECT COUNT(*) AS table_count
    FROM `{bq_client.project}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
    """

    result = bq_client.query(query).result()

    count = next(result).table_count
    print(f"Detected {count} tables in BigQuery dataset {dataset_id}.")
    return count


def is_dataset_processed(dataset_id):
    """Checks Firestore to see if the dataset has already been processed."""
    doc_ref = fs_client.collection("processed_bq_datasets").document(dataset_id)
    doc = doc_ref.get()

    return doc.exists  # If the document exists, the dataset has already been processed.


def mark_dataset_processed(dataset_id):
    """Marks a dataset as processed by writing to Firestore."""
    """Marks a dataset as processed by writing to Firestore."""
    doc_ref = fs_client.collection("processed_bq_datasets").document(dataset_id)
    doc_ref.set({"processed": True, "timestamp": time.time()})
    print(f"Marked {dataset_id} as processed in Firestore.")