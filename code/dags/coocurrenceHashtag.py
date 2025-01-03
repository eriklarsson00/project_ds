from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from hashtagNetwork import fetch_hashtags, build_cooccurence_network
from DBController import LoadConfig, ConnectDB

# Load configuration and connect to the database
config = LoadConfig()
engine = ConnectDB(config)

# Function to extract batch names
def extract_batch_names(**kwargs):
    """
    Extracts unique batch names from the database and pushes them to XCom.
    """
    query = "SELECT DISTINCT batch_process FROM batch_table;"
    with engine.connect() as conn:
        result = conn.execute(query)
        batch_names = [row[0] for row in result]
    print("Extracted batch names:", batch_names)

    # Push batch names to XCom
    kwargs['ti'].xcom_push(key='batch_names', value=batch_names)

# Function to preprocess data for a batch
def extract_data_for_preprocessing(batch_name=None, **kwargs):
    """
    Fetches hashtags and builds a cooccurrence network for a given batch.
    """
    df = fetch_hashtags(batch_process=batch_name)
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
    'max_active_runs': 1,
}

# DAG definition
with DAG(
    'coocurrenceHashtags',
    default_args=default_args,
    description='Builds cooccurrence hashtag networks for each batch process',
    schedule_interval='@daily',
    start_date=datetime(2024, 11, 20),
    catchup=False,
    concurrency=10,
    max_active_runs=1,
) as dag:

    # Task 1: Extract batch names
    extract_batches = PythonOperator(
        task_id='extract_batch_names',
        python_callable=extract_batch_names,
        provide_context=True
    )

    # Task 2: Process each batch dynamically
    def create_preprocessing_task(batch_name):
        return PythonOperator(
            task_id=f'process_batch_{batch_name}',
            python_callable=extract_data_for_preprocessing,
            op_kwargs={'batch_name': batch_name},
        )

    # Dynamically create tasks for each batch
    def create_dynamic_tasks(**kwargs):
        ti = kwargs['ti']
        batch_names = ti.xcom_pull(task_ids='extract_batch_names', key='batch_names')
        for batch_name in batch_names:
            task = create_preprocessing_task(batch_name)
            extract_batches >> task

    create_tasks = PythonOperator(
        task_id='create_dynamic_tasks',
        python_callable=create_dynamic_tasks,
        provide_context=True
    )

    # Task dependencies
    extract_batches >> create_tasks
