from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from ProcessController import ProcessFolder
from DBController import LoadConfig, ConnectDB, ReadBatchFromDB, InsertWordPairsToDB
from Lemmatization import CleanInputText, ProcessInputText, GetConnections, LoadModel
import os

MaxBatchSize = 20 * 1024 * 1024
AirflowBatchDir = '/opt/airflow/AirflowBatches/'

def ReadBatchData(FolderPath, BatchName, **kwargs):
    config = LoadConfig()
    engine = ConnectDB(config)
    DataFrame = ReadBatchFromDB(engine, BatchName)
    return DataFrame['text'].tolist()

def PreProcessText(**kwargs):
    ti = kwargs['ti']
    InputText = ti.xcom_pull(task_ids='FetchTweets')
    stanzaModel = LoadModel()
    CleanedText = CleanInputText(InputText)
    ProcessedText = ProcessInputText(CleanedText, stanzaModel)
    return ProcessedText

def WriteConnections(**kwargs):
    ti = kwargs['ti']
    InputText = ti.xcom_pull(task_ids='PreprocessTweetText')
    config = LoadConfig()
    engine = ConnectDB(config)
    AllConnections = GetConnections(InputText)
    InsertData = [
        {'window_size': k, 'word1': pair[0], 'word2': pair[1], 'word_count': count}
        for k, connection in AllConnections.items()
        for pair, count in connection.items()
    ]
    InsertWordPairsToDB(engine, InsertData)

dag = DAG(
    'Lemmatization_of_Tweets',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Pipeline to process tweets and insert word pairs',
    schedule_interval=None,
    start_date=datetime(2024, 12, 14),
    catchup=False,
)

ReadTask = PythonOperator(
    task_id='FetchTweets',
    python_callable=ReadBatchData,
    op_kwargs={'FolderPath': AirflowBatchDir, 'BatchName': 'Aftonbladet'},
    provide_context=True,
    dag=dag,
    pool='lemmatize_pool',
)

ProcessTask = PythonOperator(
    task_id='PreprocessTweetText',
    python_callable=PreProcessText,
    provide_context=True,
    dag=dag,
    pool='lemmatize_pool',
)

WriteTask = PythonOperator(
    task_id='ForgeConnections',
    python_callable=WriteConnections,
    provide_context=True,
    dag=dag,
    pool='lemmatize_pool',
)

ReadTask >> ProcessTask >> WriteTask
