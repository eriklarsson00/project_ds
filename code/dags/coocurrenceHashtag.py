from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from hashtagNetwork import fetch_hashtags, build_cooccurence_network
from DBController import LoadConfig, ConnectDB

# Load configuration and connect to the database
config = LoadConfig()
engine = ConnectDB(config)



config = LoadConfig()
engine = ConnectDB(config)


def extract_data_for_preprocessing(batch_name=None, **kwargs):
    """
    Fetches hashtags and builds a cooccurrence network for a given batch.
    """
    df = fetch_hashtags(engine=engine, batch_process=batch_name)
    if not df.empty:
        build_cooccurence_network(engine, df)

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
with DAG(
    'cooccurrence_hashtags',
    default_args=default_args,
    description='Simplified DAG to build cooccurrence hashtag networks',
    schedule_interval='@daily',
    start_date=datetime(2024, 11, 20),
    catchup=False,
) as dag:

    extract_data_task = PythonOperator(
        task_id='populate_coocurrance_table',
        python_callable=extract_data_for_preprocessing,
        op_kwargs={'batch_name': None}, 
    )