from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import os
import shutil
import re
from ProcessController import ProcessFolder
from DBController import LoadConfig, ConnectDB, ReadBatchFromDB, InsertWordPairsToDB
from Lemmatization import CleanInputText, ProcessInputText, GetConnections
MaxBatchSize = 20 * 1024 * 1024
AirflowBatchDir = '/opt/airflow/AirflowBatches/'

def ReadBatchData(FolderPath, BatchName, **kwargs):
    config = LoadConfig()
    engine = ConnectDB(config)
    if len(os.listdir(FolderPath)) == 0:
        DataFrame = ReadBatchFromDB(engine, BatchName)
    else:
        DataFrame = ProcessFolder(FolderPath, BatchName, engine, mode="Lemmatize")
    return DataFrame['text'].tolist()

def PreProcessText(InputText):
    CleanedText = CleanInputText(InputText)
    return ProcessInputText(CleanedText)

def WriteConnections(engine, InputText):
    AllConnections = GetConnections(InputText)
    InsertData =  [
            {'window_size': k, 'word1': pair[0], 'word2': pair[1], 'word_count': count}
            for k, connection in AllConnections.items()
            for pair, count in connection.items()
        ]
    InsertWordPairsToDB(engine, InsertData)

# Define the DAG
dag = DAG(
    'Lemmatization of Tweets',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Pipeline to process tweets and insert word pairs',
    schedule_interval='@daily',  # You can change the schedule
    start_date=datetime(2024, 12, 14),
    catchup=False,
)

# Define tasks
ReadTask = PythonOperator(
    task_id='FetchTweets',
    python_callable=ReadBatchData,
    dag=dag,
)

ProcessTask = PythonOperator(
    task_id='PreprocessTweetText',
    python_callable=PreProcessText,
    dag=dag,
)

WriteTask = PythonOperator(
    task_id='ForgeConnections',
    python_callable=WriteConnections,
    dag=dag,
)

# Define task dependencies (ordering)
ReadTask >> ProcessTask >> WriteTask
