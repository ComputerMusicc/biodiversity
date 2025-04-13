# PROJECT RYTEPLAN: PETERSONS RAW DATABASE GOOGLE CLOUD PIPELINE

## CLOUD FUNCTION: BQ_QUERY_PROCESS
  - This is the second function in the pipeline. 
  - Detects the instance of a new dataset named 'new_uploads_ug_uf_YYYYMMDD' in BigQuery.
  - Manipulates this dataset to form one final table 'institutions_petersons_processed_YYYYMMDD' and save it into BigQuery.

## TRIGGER: bq-query-trigger-v1
  - Event Provider: BigQuery
  - Event Type: google.cloud.audit.log.v1.written
  - Event Method: google.cloud.bigquery.v2.DatasetService.InsertDataset
  - Receive Events From: projects/'*project_id*'/datasets/new_uploads_ug_uf_* (us-central1)
  - Service Account: Default Compute Service Account


## RESPONSIBILITIES
  - Counts the number of csv files in the gcs bucket used for raw uploads.
  - Waits until the same number of tables also exist in the BigQuery dataset used for trigger.
  - Runs 5 sequential queries to output the final table to the 'institutions_petersons_processed' BigQuery database.
  - Uses a Firestore collection 'processed_bq_datasets' in order to mark a BigQuery database as processed, indicating a successful run of the function.

## CONFIGURATION
  - Environment Variables/Constants
    - project_id: Derived and authenticated using the service account tied to the event trigger and imported google.cloud library methods.
    - BUCKET_NAME(placeholder displayed in code): The bucket used to store raw data deliveries.

  - requirements.txt
    - Necessary for importing libraries into your cloud function source code.

  - *CONFIG NOTES*
    - *EACH CLOUD FUNCTION HAS ITS OWN SOURCE CODE PAGE IN THE GOOGLE CLOU RUN UI. 'MAIN.PY' IS USED HERE INSTEAD OF 'FUNCTION_NAME.PY'*

## KNOWN ISSUES / CAVEATS
  - Might start executing queries before all tables are accounted for. This happens when the 'gcs_to_bq' function uploads files to BigQuery slower than usual.
    - The function will retry until all tables exist in the dataset and then the queries will run successfully.

## RELATED FUNCTIONS
  - gcs_to_bq
  - send-bq-to-fs-gcs
  - add-to-merge-candidates

## VERSION NOTES
  - main_v1.py "Initial version used in Beta"