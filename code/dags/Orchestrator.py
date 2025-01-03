from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import os
import re
from DBController import LoadConfig, ConnectDB
from ProcessController import ProcessFolder

MaxBatchSize = 20 * 1024 * 1024
AirflowBatchDir = '/opt/airflow/AirflowBatches/'

import os

def CreateSymlinksToBatch(FolderPath, BatchDir=AirflowBatchDir):
    os.makedirs(BatchDir, exist_ok=True)
    Folder = os.path.basename(FolderPath)
    TotalSize = 0
    BatchIndex = 1
    BatchFolder = os.path.join(BatchDir, f"{Folder}{BatchIndex}")
    os.makedirs(BatchFolder, exist_ok=True)

    for file in os.listdir(FolderPath):
        FilePath = os.path.join(FolderPath, file)
        if os.path.isfile(FilePath):
            FileSize = os.path.getsize(FilePath)
            if TotalSize + FileSize > MaxBatchSize:
                BatchIndex += 1
                BatchFolder = os.path.join(BatchDir, f"{Folder}{BatchIndex}")
                os.makedirs(BatchFolder, exist_ok=True)
                TotalSize = 0
            
            SymlinkPath = os.path.join(BatchFolder, file)
            # Check if the symlink already exists
            if not os.path.exists(SymlinkPath):
                os.symlink(FilePath, SymlinkPath)
                TotalSize += FileSize
            else:
                print(f"Symlink for {file} already exists. Skipping creation.")


def ProcessAllBatches(FolderName, BatchDir=AirflowBatchDir, **kwargs):
    config = LoadConfig()
    engine = ConnectDB(config)
    batch_prefix = f"{FolderName}"
    batch_paths = [
        os.path.join(BatchDir, batch_name)
        for batch_name in os.listdir(BatchDir)
        if batch_name.startswith(batch_prefix)
    ]

    for batch_path in batch_paths:
        if not os.path.exists(batch_path):
            print(f"Skipping non-existent batch path: {batch_path}")
            continue

        print(f"Processing batch: {batch_path}")
        ProcessFolder(engine=engine, BatchName=os.path.basename(batch_path), FolderPath=batch_path)



def GetMaxBatchIndex(batch_dir, folder_name):
    batch_prefix = f"{folder_name}"
    existing_batch_indices = [
        int(re.sub(f"^{batch_prefix}(\d+)$", r"\1", filename))
        for filename in os.listdir(batch_dir)
        if filename.startswith(batch_prefix) and re.match(f"^{batch_prefix}\d+$", filename)
    ]
    return max(existing_batch_indices, default=0)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'OrchestratorTwitter',
    default_args=default_args,
    description='Orchestrator for processing Twitter JSON files',
    schedule_interval='@daily',
    start_date=datetime(2024, 11, 20),
    catchup=False
) as dag:

    DataDir = '/opt/airflow/tweets'
    task_enable_batch = {}
    task_process_all_batches = {}
    task_lemmatize_batch = {}
    TaskGroups = {}

    for folder in os.listdir(DataDir):
        FolderPath = os.path.join(DataDir, folder)
        if os.path.isdir(FolderPath):
            if folder not in TaskGroups:
                TaskGroups[folder] = TaskGroup(group_id=f"group_{folder}")

            # Create batch task
            task_enable_batch[folder] = PythonOperator(
                task_id=f'Create_Batch_For_{folder}',  
                python_callable=CreateSymlinksToBatch,
                op_kwargs={'FolderPath': FolderPath},
                task_group=TaskGroups[folder],
            )

            # Consolidated process task
            task_process_all_batches[folder] = PythonOperator(
                task_id=f'Process_All_Batches_For_{folder}',
                python_callable=ProcessAllBatches,
                op_kwargs={'FolderName': folder},
                task_group=TaskGroups[folder],
                pool='tweet_pool',
            )

            # Lemmatize batch task
            task_lemmatize_batch[folder] = TriggerDagRunOperator(
                task_id=f'Lemmatize_All_Batches_For_{folder}',
                trigger_dag_id='Lemmatization_of_Tweets',
                conf={'FolderName': folder},
                task_group=TaskGroups[folder],
                wait_for_completion=False,
                pool='lemmatize_pool',
            )

            # Set dependencies
            task_enable_batch[folder] >> task_process_all_batches[folder] >> task_lemmatize_batch[folder]