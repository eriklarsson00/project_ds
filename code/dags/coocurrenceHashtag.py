from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from sqlalchemy import text
from hashtagNetwork import fetch_hashtags, build_cooccurence_network
from DBController import LoadConfig, ConnectDB
config = LoadConfig()
engine = ConnectDB(config)

def extract_batch_names(**kwargs):
    """
    Extracts unique batch names from the database and pushes them to XCom.
    """
    query = "SELECT DISTINCT batch_process FROM batch_table;"
    with engine.connect() as conn:
        result = conn.execute(query)
        batch_names = [row[0] for row in result]
    print("Extracted batch names:", batch_names)

    kwargs['ti'].xcom_push(key='batch_names', value=batch_names)

def extract_data_for_preprocessing(batch_name=None, **kwargs):
    df = fetch_hashtags(batch_process=batch_name)
    if not df.empty:
        build_cooccurence_network(engine, df)



    
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1
}

with DAG(
    'coocurrenceHashtags',
    default_args=default_args,
    description='Cooccurence hashtags',
    schedule_interval='@daily',
    start_date=datetime(2024, 11, 20),
    catchup=False,
    concurrency=10,
    max_active_runs=1,
) as dag:
    
    