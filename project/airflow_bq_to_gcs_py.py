# %%
# Importing PIP Package
#!pip install apache-airflow
#!pip install apache-airflow-providers-google
#!pip install google-cloud-bigquery
#!pip install google-cloud-storage


# %%
# Define Airflow DAG
import airflow
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import bigquery

def execute_query(**kwargs):
    # Define the parameters
    PROJECT_ID = "practical-case-436315-t2"
    DATASET_ID = "surv_source_dataset"
    TABLE_ID = "surv_emp_gb"
    
    # Get the GCP connection ID
    GCP_CONNECTION_ID = 'GCP_CONNECTION_ID'  # Specify your connection ID directly here or use Variable/Connection

    # Define the configuration for the BigQuery job
    query = f"SELECT 'GB', a.* FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` a"
    configuration = {
        "query": {
            "query": query,
            "useLegacySql": False,
            "destinationTable": {
                "projectId": PROJECT_ID,
                "datasetId": DATASET_ID,
                "tableId": "temporary_table",
            },
            "writeDisposition": "WRITE_TRUNCATE",
        }
    }

    # Create an instance of BigQueryInsertJobOperator for the query execution
    bq_query_operator = BigQueryInsertJobOperator(
        task_id='execute_query',  # Task ID for this step
        configuration=configuration,
        gcp_conn_id=GCP_CONNECTION_ID
    )

    # Execute the operator
    return bq_query_operator.execute(context=kwargs)

def extract_to_gcs(**kwargs):
    # Define the parameters
    PROJECT_ID = "practical-case-436315-t2"
    DATASET_ID = "surv_source_dataset"
    GCS_BUCKET = "surv_data_migration"
    CSV_FILE_NAME = "practical-case-436315-t2_surv_source_dataset.surv_emp_gb.csv"

    # Get the GCP connection ID
    GCP_CONNECTION_ID = 'GCP_CONNECTION_ID'  # Specify your connection ID directly here or use Variable/Connection

    # Define the configuration for the BigQuery extract job
    configuration = {
        "extract": {
            "sourceTable": {
                "projectId": PROJECT_ID,
                "datasetId": DATASET_ID,
                "tableId": "temporary_table",
            },
            "destinationUris": [f"gs://{GCS_BUCKET}/{CSV_FILE_NAME}"],
            "destinationFormat": "CSV",
        }
    }

    # Create an instance of BigQueryInsertJobOperator for the extract operation
    bq_extract_operator = BigQueryInsertJobOperator(
        task_id='extract_to_gcs',  # Task ID for this step
        configuration=configuration,
        gcp_conn_id=GCP_CONNECTION_ID
    )

    # Execute the operator
    return bq_extract_operator.execute(context=kwargs)

def gcs_csv_to_target_db(**kwargs):
   
    # Define the parameters
    PROJECT_ID = "practical-case-436315-t2"
    DATASET_ID = "surv_target_dataset"
    TABLE_ID = "surv_emp_all"
    GCS_BUCKET = "surv_data_migration"
    CSV_FILE_NAME = "practical-case-436315-t2_surv_source_dataset.surv_emp_gb.csv"

    # Get the GCP connection ID
    GCP_CONNECTION_ID = 'GCP_CONNECTION_ID'  
    
    uri = f"gs://{GCS_BUCKET}/{CSV_FILE_NAME}"
    load_from_gcs = BigQueryInsertJobOperator(
    task_id="gcs_csv_to_target_db",
    configuration={
        "load": {
            "destinationTable": {
                "projectId": PROJECT_ID
                ,"datasetId": DATASET_ID
                ,"tableId": TABLE_ID
            }
            ,"sourceUris": [uri]
            ,"sourceFormat": "CSV"
            ,"maxBadRecords": 1          
            ,"skipLeadingRows": 1       
            ,"writeDisposition": "WRITE_APPEND",
        }
    }
    ,gcp_conn_id=GCP_CONNECTION_ID
    ,dag=dag
    )
    return load_from_gcs.execute(context=kwargs)

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.now().subtract(days=1),
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

# DAG to Copy data from Source BQ to CSV to Target BQ
dag = DAG(
    dag_id='bq_transfer_dataset',
    description='DAG to Copy data from Source BQ to CSV to Target BQ',
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=pendulum.duration(minutes=20),
)

# Task to execute the query and save results in a temporary table
query_task = PythonOperator(
    task_id='execute_query',
    python_callable=execute_query,
    op_kwargs={},  # No additional kwargs needed
    dag=dag,
)

# Task to extract data from temporary table to CSV in GCS Bucket
extract_data_task = PythonOperator(
    task_id='extract_to_gcs',
    python_callable=extract_to_gcs,
    op_kwargs={},  # No additional kwargs needed
    dag=dag,
)

# Copy CSV data from GCS Bucket to BQ Target Dataset
load_data_task = PythonOperator(
    task_id='load_csv',
    python_callable=gcs_csv_to_target_db,
    op_kwargs={},  # No additional kwargs needed
    dag=dag,
)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

# Define task dependencies
start >> query_task >> extract_data_task >> load_data_task >> end


# %%
