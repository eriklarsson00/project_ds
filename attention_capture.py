from sqlalchemy import Table, Column, String, Integer, MetaData, text, inspect
from sqlalchemy.exc import SQLAlchemyError
from collections import defaultdict
import pandas as pd
from connect import connect
from config import load_config
from sliding_window import sliding_window_connections  # Import the sliding window function
from datetime import datetime  
import numpy as np# Import datetime for timestamp
import re
import stanza
stanza.download('sv')  # Download the Swedish model if not already downloaded
nlp = stanza.Pipeline('sv')  # Initialize Stanza pipeline for Swedish
def clean_and_lemmatize_tweet(tweet, remove_urls=True, remove_special_chars=True, remove_digits=True):
    
    tweet = tweet.lower()
    # Remove URLs
   
    if remove_urls:
        tweet = re.sub(r'http\S+|www\S+|https\S+', '', tweet, flags=re.MULTILINE)

    # Remove special characters
    if remove_special_chars:
        tweet = re.sub(r'[^A-Za-zåäöÅÄÖ\s]', '', tweet)

    # Remove digits
    if remove_digits:
        tweet = re.sub(r'\d+', '', tweet)

    # Remove extra spaces
    tweet = re.sub(r'\s+', ' ', tweet).strip()

    # Lemmatize using Stanza
    doc = nlp(tweet)
    lemmatized_words = [word.lemma for sentence in doc.sentences for word in sentence.words if word.lemma]
    return ' '.join(lemmatized_words)

# Function to delete tweet_text_attention table
def delete_tweet_text_attention(engine, window_size):
    table_name = f'word_pairs_window_{window_size}'
    metadata = MetaData()

    # Use SQLAlchemy's inspector to check if the table exists
    inspector = inspect(engine)
    if table_name not in inspector.get_table_names():
        print(f"Table '{table_name}' does not exist, so it cannot be deleted.")
        return  # Exit the function if the table does not exist

    # If the table exists, proceed to delete it
    tweet_text_attention = Table(table_name, metadata, autoload_with=engine)
    try:
        tweet_text_attention.drop(engine)
        print(f"Table '{table_name}' deleted successfully.")
    except SQLAlchemyError as error:
        print(f"Error deleting table '{table_name}': {error}")

# Function to create tweet_text_attention table
def create_tweet_text_attention_table(engine, window_size):
    metadata = MetaData()
    table_name = f'word_pairs_window_{window_size}'
    tweet_text_attention = Table(
        table_name, metadata,
        Column('word1', String, nullable=False),
        Column('word2', String, nullable=False),
        Column('word_count', Integer, nullable=False)
    )
    
    try:
        metadata.create_all(engine)  # Create the table
        print(f"Table: {table_name} created successfully.")
    except SQLAlchemyError as error:
        print(f"Error creating table: {error}")

# Function to remove all rows from tweet_text_attention table
def clear_tweet_text_attention(engine, window_size):
    
    table_name = f'word_pairs_window_{window_size}'
    metadata = MetaData()
    tweet_text_attention = Table(table_name, metadata, autoload_with=engine)

    try:
        with engine.begin() as conn:
            conn.execute(tweet_text_attention.delete()) 
            print(f"All rows removed from '{table_name}'.")
    except SQLAlchemyError as error:
        print(f"Error clearing table '{table_name}': {error}")


def create_tables(engine, window_size):
        for i in range (2, window_size+1):
            delete_tweet_text_attention(engine, i)
            create_tweet_text_attention_table(engine, i)
        #clear_tweet_text_attention(engine)

def is_valid_table_name(table_name, max_window_size=10):
    # Validate that the table name follows the expected pattern
    allowed_tables = {f"word_pairs_window_{i}" for i in range(2, max_window_size + 1)}
    return table_name in allowed_tables


def process_tweet_text(engine, start_unique_id=1, max_window_size=10, batch_size=10000):
    current_unique_id = start_unique_id  # Start processing from this unique_id
    
    while True:
        # Define the range for the batch
        end_unique_id = current_unique_id + batch_size
        
        # Fetch tweets within the current range
        query = text(f"""
            SELECT unique_id, text FROM tweets
            WHERE unique_id >= :current_unique_id AND unique_id < :end_unique_id
            ORDER BY unique_id
        """)
        
        df = pd.read_sql(query, engine, params={"current_unique_id": current_unique_id, "end_unique_id": end_unique_id})
        
        # Exit the loop if no more records are fetched
        if df.empty:
            print(f"No more records found starting from unique_id {current_unique_id}.")
            break
        
        print(f"Processing records with id from {current_unique_id} to {end_unique_id - 1}...")
        
        # Dictionary to store total connections
        total_connections_windows = {size: defaultdict(int) for size in range(2, max_window_size + 1)} 
        
        # Process each tweet in the batch
        for tweet_text in df['text']:
            cleaned_text = clean_and_lemmatize_tweet(tweet_text)
            # Generate connections using sliding window
            connections = sliding_window_connections(cleaned_text, max_window_size)
            
            for k, connection in connections.items():
                for word_pair, count in connection.items():
                    total_connections_windows[k][word_pair] += count
        
        # Prepare data for insertion
        for k, connection in total_connections_windows.items():
            insert_data = [{'word1': pair[0], 'word2': pair[1], 'word_count': count} for pair, count in connection.items()]
            table_name = f'word_pairs_window_{k}'

            if not is_valid_table_name(table_name, max_window_size):
                raise ValueError(f"Invalid table name: {table_name}")
            
            # Insert into the corresponding table
            insert_query = text(f"""
                INSERT INTO {table_name} (word1, word2, word_count)
                VALUES (:word1, :word2, :word_count)
            """)

            try:
                with engine.begin() as conn:
                    conn.execute(insert_query, insert_data)
                    print(f"{len(insert_data)} word pairs inserted successfully into {table_name}.")
            except SQLAlchemyError as error:
                print(f"Error inserting data: {error}")
        
        # Move to the next range of unique_id
        current_unique_id = end_unique_id

# Main logic
if __name__ == '__main__':
    print("numpy version: ", np.__version__)
    config = load_config()  # Assuming you have a function that loads config
    engine = connect(config)  # Assuming this function connects to the DB

    create_tables(engine, window_size=10)
    
    # Example start_unique_id
    start_unique_id = 1  # Replace with the actual starting unique_id you want to process
    
    # Process tweets in batches with lemmatization
    process_tweet_text(engine, start_unique_id, max_window_size=10)
