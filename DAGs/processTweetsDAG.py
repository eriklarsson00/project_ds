#!pip install apache-airflow

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys


# Add the path to the 'project_ds' directory to sys.path
sys.path.append('/Users/harivs/Documents/GitHub/project_ds')

from processTweets import process_tweets_file
from convert_json import connect, load_config


def process_tweets_folder(folder_path, **kwargs):
    config = load_config()
    engine = connect(config)
    
    total_processed = 0
    for file in os.listdir(folder_path):
        if file.endswith(".json"):
            file_path = os.path.join(folder_path, file)
            total_processed += process_tweets_file(file_path, engine)
    
    print(f"Processed {total_processed} tweets from {folder_path}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'process_tweets_dag',
    default_args=default_args,
    description='Process Twitter JSON files in batches',
    schedule_interval='@daily',
    start_date=datetime(2024, 11, 20),
    catchup=False,
) as dag:
    
    data_dir = '/Users/harivs/Documents/GitHub/project_ds/tweets'  # Update with the absolute path to the tweets folder
    
    # Dynamically create tasks for each newspaper folder
    tasks = []
    for folder in os.listdir(data_dir):
        folder_path = os.path.join(data_dir, folder)
        if os.path.isdir(folder_path):
            task = PythonOperator(
                task_id=f'process_{folder}',
                python_callable=process_tweets_folder,
                op_kwargs={'folder_path': folder_path},
            )
            tasks.append(task)
