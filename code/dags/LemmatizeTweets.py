from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from DBController import LoadConfig, ConnectDB, ReadNewBatchesFromDB, UpdateBatchStatus, ReadBatchFromDB
from Lemmatization import LoadModel, CleanInputText, ProcessInputText, GetConnections, InsertWordPairsToDB
stanzaModel = None

def WritePreProcessedText(BatchName):
    global stanzaModel
    config = LoadConfig()
    engine = ConnectDB(config)
    DataFrame = ReadBatchFromDB(engine, BatchName)
    InputText = DataFrame['text'].tolist()
    if stanzaModel is None:
        stanzaModel = LoadModel()
    CleanedText = CleanInputText(InputText)
    ProcessedText = ProcessInputText(CleanedText, stanzaModel)
    InsertData = GetConnections(ProcessedText)
    InsertWordPairsToDB(engine, InsertData)


def ForgeConnectionsTask(**kwargs):
    """Process all batches sequentially."""
    config = LoadConfig()
    engine = ConnectDB(config)
    
    # Fetch new batches
    batches = ReadNewBatchesFromDB(engine)  # Fetch batches directly
    
    for batch in batches:
        batch_name = batch['batch_name']
        try:
            UpdateBatchStatus(engine, batch_name, 'PROCESSING')  # Set batch_status to PROCESSING
            WritePreProcessedText(batch_name)  # Process the batch
            UpdateBatchStatus(engine, batch_name, 'SUCCESS')  # Set batch_status to SUCCESS
        except Exception as e:
            UpdateBatchStatus(engine, batch_name, 'FAILED')  # Handle failure
            raise e

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Define the DAG
with DAG(
    'Lemmatization_of_Tweets',
    default_args=default_args,
    description='Pipeline to process tweets and insert word pairs',
    schedule_interval=None,
    start_date=datetime(2024, 12, 14),
    catchup=False,
) as dag:

    # Single task to process all batches
    process_batches = PythonOperator(
        task_id='ForgeConnections',
        python_callable=ForgeConnectionsTask,
        dag=dag,
        pool='lemmatize_pool',
    )
