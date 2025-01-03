from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from ProcessController import ProcessFolder
from DBController import LoadConfig, ConnectDB, ReadBatchFromDB, InsertWordPairsToDB
from Lemmatization import CleanInputText, ProcessInputText, GetConnections, LoadModel
import os

MaxBatchSize = 20 * 1024 * 1024
AirflowBatchDir = '/opt/airflow/AirflowBatches/'


def WritePreProcessedText(BatchName):
    config = LoadConfig()
    engine = ConnectDB(config)
    DataFrame = ReadBatchFromDB(engine, BatchName)
    InputText = DataFrame['text'].tolist()
    stanzaModel = LoadModel()
    CleanedText = CleanInputText(InputText)
    ProcessedText = ProcessInputText(CleanedText, stanzaModel)
    InsertData = GetConnections(ProcessedText)
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

Task1 = PythonOperator(
    task_id='ForgeConnections',
    python_callable=WritePreProcessedText,
    op_kwargs={'BatchName': 'Aftonbladet'},
    provide_context=True,
    dag=dag,
    pool='lemmatize_pool',
)

Task1
