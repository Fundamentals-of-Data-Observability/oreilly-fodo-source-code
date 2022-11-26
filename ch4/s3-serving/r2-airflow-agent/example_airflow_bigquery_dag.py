import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago

# logging.warning("--LIST")
# for currentpath, folders, files in os.walk('.'):
#     print(currentpath, folders, files)
# logging.warning("LIST--")

import os.path
my_path = os.path.abspath(os.path.dirname(__file__))
conf_path = os.path.join(my_path, "conf.ini")

os.environ["CONF_FILE"]=conf_path
logging.warning("CONF_FILE:" + os.environ["CONF_FILE"])

if False: # log config
    # logging.warning("--CONF FILE")
    with open(conf_path, 'r') as f:
        for l in f.readlines():
            logging.warning(l)
    logging.warning("CONF FILE--")
    from kensu.utils.kensu import Kensu
    config = Kensu.build_conf()
    logging.warning("CONF--")
    logging.warning({section: dict(config[section]) for section in config.sections()})
    logging.warning("--CONF")


from kensu.airflow.operators.bash import BashOperator
from kensu.airflow.operators.python import PythonOperator
from kensu.airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from kensu.airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from kensu.airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

import logging

week='week1'
BUCKET='andy_airflow_fodo'



PROJECT_ID = os.environ.get("GCP_PROJECT") # Google Composer Var: https://cloud.google.com/composer/docs/how-to/managing/environment-variables


from google.cloud import storage

def create_bucket(bucket_name,location):
    """Creates a new bucket."""

    storage_client = storage.Client()
    
    bucket = storage_client.create_bucket(bucket_name,location=location)

    print(f"Bucket {bucket.name} created")


storage_client = storage.Client()
buckets = storage_client.list_buckets()

if BUCKET not in [b.name for b in buckets]:
    create_bucket(BUCKET,location="eu")


TC_DATASET = PROJECT_ID+".tripdata_"+BUCKET
DATASET = "tripdata_"+BUCKET

from google.cloud import bigquery
# Construct a BigQuery client object.
client = bigquery.Client()

dataset = bigquery.Dataset(TC_DATASET)

# TODO(developer): Specify the geographic location where the dataset should reside.
dataset.location = "EU"

try:
    dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
except:
    print("Dataset exists")


path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

COLOUR_RANGE = {'yellow': 'tpep_pickup_datetime', 'green': 'lpep_pickup_datetime'}
RAW_INPUT_PREFIX = "raw"
INPUT_FILETYPE = "parquet"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}


def upload_to_gcs_via_pandas(src_file, dst_uri):
    import kensu.pandas as pd
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    date_columns = list([
        date_column
        for color, date_column in COLOUR_RANGE.items()
        if color in src_file
    ])
    df = pd.read_csv(src_file, parse_dates=date_columns, infer_datetime_format=True)
    df.to_parquet(dst_uri)


# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="combined_data_ingestion_and_processing_first",
    schedule_interval="@monthly",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:
    

    for colour, ds_col in COLOUR_RANGE.items():

        # ingestion
        dataset_prefix = colour
        dataset_file = f"{dataset_prefix}_ingestion.csv"


        dataset_url = f"gs://europe-central2-andy-test-a-87eb78a0-bucket/dags/data/{week}/{dataset_prefix}_ingestion.csv"

        parquet_file = dataset_file.replace('.csv', '.parquet')
        gs_raw_parquet_file_uri = f"gs://{BUCKET}/raw/{parquet_file}"
        BIGQUERY_DATASET = "tripdata_"+BUCKET

        #os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

        upload_as_parquet_to_gcs_via_pandas_task = PythonOperator(
            task_id=f"{dataset_prefix}_upload_as_parquet_to_gcs_via_pandas",
            python_callable=upload_to_gcs_via_pandas,
            op_kwargs={
                "src_file": dataset_url,
                "dst_uri": gs_raw_parquet_file_uri
            },
        )

        bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id=f"{dataset_prefix}_bigquery_external_table_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": "external_table",
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [gs_raw_parquet_file_uri],
                },
            },
        ) if colour == 'yellow' else None

        # gcs_to_bq
        move_files_gcs_task = GCSToGCSOperator(
            task_id=f'move_{colour}_{DATASET}_files_task',
            source_bucket=BUCKET,
            source_object=f'{RAW_INPUT_PREFIX}/{colour}_ingestion.{INPUT_FILETYPE}',
            destination_bucket=BUCKET,
            destination_object=f'{colour}/{colour}_tripdata',
            move_object=False  
        )

        etl_bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id=f"bq_{colour}_{DATASET}_external_table_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"{colour}_{DATASET}_external_table",
                },
                "externalDataConfiguration": {
                    "autodetect": "True",
                    "sourceFormat": f"{INPUT_FILETYPE.upper()}",
                    "sourceUris": [
                        f'gs://{BUCKET}/{colour}/{colour}_tripdata'],
                },
            },
        )

        CREATE_BQ_TBL_QUERY = (
            f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{colour}_{DATASET} \
            PARTITION BY DATE({ds_col}) \
            AS \
            SELECT * FROM {BIGQUERY_DATASET}.{colour}_{DATASET}_external_table;"
        )

        # Create a partitioned table from external table
        bq_create_partitioned_table_job = BigQueryInsertJobOperator(
            task_id=f"bq_create_{colour}_{DATASET}_partitioned_table_task",
            configuration={
                "query": {
                    "query": CREATE_BQ_TBL_QUERY,
                    "useLegacySql": False,
                }
            }
        )

        # schedule the jobs below
        # FIXME: the "if" below could be simplified I guess...
        if colour == 'yellow':
            upload_as_parquet_to_gcs_via_pandas_task >> bigquery_external_table_task >> \
            move_files_gcs_task >> etl_bigquery_external_table_task >> bq_create_partitioned_table_job
        else:
            upload_as_parquet_to_gcs_via_pandas_task \
            >> move_files_gcs_task >> etl_bigquery_external_table_task >> bq_create_partitioned_table_job