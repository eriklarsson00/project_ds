from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

# Your Python functions here (e.g., CleanInputText, SlidingWindowWithOverlap, etc.)


def LemmatizeFromDB(BatchName, **kwargs):
    config = LoadConfig()
    engine = ConnectDB(config)
    ProcessedCount = 0
    ProcessedCount = InsertConnectionsToDB(engine, BatchName)
    print(f"Preprocessed and lemmatized {ProcessedCount} tweets from batch {BatchName}")

def fetch_tweets_from_db():
    # Function to fetch tweets from DB (based on batch_name if required)
    pass

def clean_tweets():
    # Function to clean tweets
    pass

def lemmatize_tweets():
    # Function to lemmatize tweets
    pass

def tokenize_tweets():
    # Function to tokenize tweets
    pass

def sliding_window_analysis():
    # Function to perform sliding window analysis
    pass

def insert_word_pairs_to_db():
    # Function to insert word pairs into DB
    pass

# Define the DAG
dag = DAG(
    'tweet_word_pairs_pipeline',
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
task1 = PythonOperator(
    task_id='fetch_tweets',
    python_callable=fetch_tweets_from_db,
    dag=dag,
)

task2 = PythonOperator(
    task_id='clean_tweets',
    python_callable=clean_tweets,
    dag=dag,
)

task3 = PythonOperator(
    task_id='lemmatize_tweets',
    python_callable=lemmatize_tweets,
    dag=dag,
)

task4 = PythonOperator(
    task_id='tokenize_tweets',
    python_callable=tokenize_tweets,
    dag=dag,
)

task5 = PythonOperator(
    task_id='sliding_window_analysis',
    python_callable=sliding_window_analysis,
    dag=dag,
)

task6 = PythonOperator(
    task_id='insert_word_pairs',
    python_callable=insert_word_pairs_to_db,
    dag=dag,
)

# Define task dependencies (ordering)
task1 >> task2 >> task3 >> task4 >> task5 >> task6
