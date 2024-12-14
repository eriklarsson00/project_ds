from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import os
import shutil
import re
from ProcessController import ProcessFolder
from DBController import LoadConfig, ConnectDB, CreateAllTables

MaxBatchSize = 20 * 1024 * 1024
AirflowBatchDir = '/opt/airflow/AirflowBatches/'

def GetFolderSize(FolderPath):
    TotalSize = 0
    for dirpath, dirnames, filenames in os.walk(FolderPath):
        for file in filenames:
            FilePath = os.path.join(dirpath, file)
            TotalSize += os.path.getsize(FilePath)
    return TotalSize


def CopyFilesToBatch(FolderPath, BatchDir):
    os.makedirs(BatchDir, exist_ok=True)
    Folder = os.path.basename(FolderPath)
    TotalSize = 0
    BatchFolders = []
    BatchIndex = 1
    BatchFolder = os.path.join(BatchDir, f"{Folder}{BatchIndex}")
    os.makedirs(BatchFolder, exist_ok=True)
    BatchFolders.append(BatchFolder)
    try:
        for file in os.listdir(FolderPath):
            FilePath = os.path.join(FolderPath, file)
            if os.path.isfile(FilePath):
                FileSize = os.path.getsize(FilePath)
                if TotalSize + FileSize > MaxBatchSize:
                    BatchIndex += 1
                    BatchFolder = os.path.join(BatchDir, f"{Folder}{BatchIndex}")
                    os.makedirs(BatchFolder, exist_ok=True)
                    BatchFolders.append(BatchFolder)
                    TotalSize = 0
                shutil.copy(FilePath, BatchFolder)
                TotalSize += FileSize
        return BatchIndex, BatchFolders
    except Exception as e:
        print(f"Error while copying files to batch folder: {e}")
        raise


def EnableBatchProcessing(FolderPath, **kwargs):
    os.makedirs(AirflowBatchDir, exist_ok=True)
    NumberOfBatches, BatchFolders = CopyFilesToBatch(FolderPath, AirflowBatchDir)
    kwargs['ti'].xcom_push(key='NumberOfBatches', value=NumberOfBatches)
    kwargs['ti'].xcom_push(key='BatchFolders', value=BatchFolders)


def UploadToDB(FolderPath, BatchName, **kwargs):
    config = LoadConfig()
    engine = ConnectDB(config)
    #CreateAllTables(engine)
    ProcessedCount = ProcessFolder(FolderPath, BatchName, engine)
    print(f"Processed {ProcessedCount} tweets from {BatchName} in {FolderPath}")


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
    'OrchestratorTwitter',
    default_args=default_args,
    description='Orchestrator for processing the Twitter JSON files',
    schedule_interval='@daily',
    start_date=datetime(2024, 11, 20),
    catchup=False,
    concurrency=10,
    max_active_runs=1,
) as dag:
    task_enable_batch = {}
    task_process_batch = {}
    TaskGroups = {}
    DataDir = '/opt/airflow/tweets'
    for folder in os.listdir(DataDir):
        FolderPath = os.path.join(DataDir, folder)
        if os.path.isdir(FolderPath):
            if folder not in TaskGroups:
                TaskGroups[folder] = TaskGroup(group_id=f"group_{folder}")
            task_enable_batch[folder] = PythonOperator(
                task_id=f'Create_Batch_For_{folder}',  
                python_callable=EnableBatchProcessing,
                op_kwargs={'FolderPath': FolderPath},
                provide_context=True,
                pool='file_pool',
                queue='default',
                task_group=TaskGroups[folder]
            )
            
            task_enable_batch[folder]
    
    for folder in os.listdir(AirflowBatchDir):
        FolderPath = os.path.join(AirflowBatchDir, folder)
        if os.path.isdir(FolderPath):
            TweetFolder = re.sub(r'\d+', '', folder)
            task_process_batch[folder] = PythonOperator(
                task_id=f'Process_Batch_For_{folder}',  
                python_callable=UploadToDB,
                op_kwargs={'FolderPath': FolderPath, 'BatchName': folder},
                provide_context=True,
                pool='tweet_pool',
                queue='default',
                task_group=TaskGroups[TweetFolder]
            )
            task_process_batch[folder].set_upstream(task_enable_batch[TweetFolder])