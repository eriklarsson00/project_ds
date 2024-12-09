from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
from processTweets import process_tweets_file  # Updated function to process each tweet file
from attention_capture import process_tweets_batch  # Function for preprocessing and lemmatization
from convert_json import connect, load_config

# Process tweets in batches (Folder level)
def process_newspaper_folder(folder_path, **kwargs):
    config = load_config()
    engine = connect(config)
    
    # Get the unique_id for the current task (e.g., timestamp or unique batch number)
    unique_id = kwargs['unique_id']
    
    total_processed = 0
    for file in os.listdir(folder_path):
        if file.endswith(".json"):
            file_path = os.path.join(folder_path, file)
            total_processed += process_tweets_file(file_path, engine, unique_id)
    
    print(f"Processed {total_processed} tweets from {folder_path}")
    return total_processed  # Returning total processed for logging or downstream tasks

# Preprocessing and Lemmatization Task for each batch
def preprocess_and_lemmatize(batch_size=10000, unique_id=None, **kwargs):
    config = load_config()
    engine = connect(config)
    
    print(f"Preprocessing batch with unique_id: {unique_id} and batch size: {batch_size}")
    
    # Preprocess the tweets from the newly inserted batch using the batch_id and unique_id
    processed_count = process_tweets_batch(engine, unique_id, batch_size)
    
    print(f"Preprocessed and lemmatized {processed_count} tweets from batch {unique_id}")
    return processed_count  # Returning processed count for downstream tasks

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
with DAG(
    'process_tweets_dag_batch',
    default_args=default_args,
    description='Process Twitter JSON files and preprocess batches',
    schedule_interval='@daily',
    start_date=datetime(2024, 11, 20),
    catchup=False,
) as dag:
    
    data_dir = '/opt/airflow/tweets/aftonbladet'  # Update with the absolute path to your data folder
    
    # Dynamically create tasks for each newspaper folder
    tasks = []
    unique_id = 1  # Start from a unique identifier for the first batch (e.g., timestamp, or incrementing counter)
    
    for folder in os.listdir(data_dir):
        folder_path = os.path.join(data_dir, folder)
        if os.path.isdir(folder_path):

            #control how many tweets to process at a time using batch_size
            batch_size = 10000
            unique_id = 1
            
            task_process_folder = PythonOperator(
                task_id=f'process_{folder}_folder',  # Task to process tweets from a folder
                python_callable=process_newspaper_folder,
                op_kwargs={'folder_path': folder_path, 'unique_id': unique_id},
            )
            
            task_preprocess_lemmatize = PythonOperator(
                task_id=f'preprocess_{folder}_tweets_{unique_id}',  # Task for preprocessing and lemmatization
                python_callable=preprocess_and_lemmatize,
                op_kwargs={'unique_id': unique_id, 'batch_size': batch_size},  # Pass batch size and unique_id
            )
            
            # Set task dependencies: preprocessing depends on the completion of folder processing
            task_process_folder >> task_preprocess_lemmatize
            
            # Update unique_id for the next batch
            unique_id += batch_size
            
            tasks.append((task_process_folder, task_preprocess_lemmatize))

    # Optionally, you could add a global dependency to ensure tasks run sequentially for each folder's batch.
    # for task1, task2 in tasks:
    #     task1 >> task2