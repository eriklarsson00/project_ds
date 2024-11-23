from sqlalchemy import Table, Column, String, Integer, MetaData, text, inspect,Index
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
    table_name = 'word_pairs_all_windows'
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

def delete_all_window_size_table(engine):
    table_name = 'word_pairs_all_windows'
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

def create_all_window_size_table(engine):
    metadata = MetaData()
    table_name = 'word_pairs_all_windows'
    table = Table(
        table_name, metadata,
        Column('window_size', Integer, nullable=False),
        Column('word1', String, nullable=False),
        Column('word2', String, nullable=False),
        Column('word_count', Integer, nullable=False),
        Index('ix_sliding_window_size', 'window_size')
    )
    try:
        metadata.create_all(engine)  # Create the table
        print(f"Table: {table_name} created successfully.")
    except SQLAlchemyError as error:
        print(f"Error creating table: {error}")


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
        delete_all_window_size_table(engine)
        for i in range (2, window_size+1):
            delete_tweet_text_attention(engine, i)
            create_tweet_text_attention_table(engine, i)
        #clear_tweet_text_attention(engine)

def is_valid_table_name(table_name, max_window_size=10):
    # Validate that the table name follows the expected pattern
    allowed_tables = {f"word_pairs_window_{i}" for i in range(2, max_window_size + 1)}
    return table_name in allowed_tables

def process_all_tweets(engine, all_connections):
    insert_all_data = [
    {'window_size': k, 'word1': pair[0], 'word2': pair[1], 'word_count': count}
    for k, connection in all_connections.items()
    for pair, count in connection.items()]
    # Insert into tweet_text_attention
    insert_query = text("""
        INSERT INTO word_pairs_all_windows (window_size, word1, word2, word_count)
        VALUES (:window_size,:word1, :word2, :word_count)
    """)
    try:
        with engine.begin() as conn:
            conn.execute(insert_query, insert_all_data)
            print(f"{len(insert_all_data)} word pairs inserted successfully into word_pairs_all_windows'.")
    except SQLAlchemyError as error:
        print(f"Error inserting data: {error}")
        



def process_tweet_text(engine, max_window_size=10):
    query = text("SELECT text FROM tweets")
    df = pd.read_sql(query, engine) # TODO: BATCH PROCESS OR APACHE SPARK??

    # Create tweet_text_attention table


    # Dictionary to store total connections
    total_connections_windows = {size: defaultdict(int) for size in range(2, max_window_size + 1)} 

    
    # Iterate through each tweet's text
    for tweet_text in df['text']:
        cleaned_text = clean_and_lemmatize_tweet(tweet_text)
        # Generate connections using sliding window
        connections = sliding_window_connections(cleaned_text, max_window_size)
        
        for k, connection in connections.items():
            for word_pair, count in connection.items():
                total_connections_windows[k][word_pair] += count
    
    
    # Prepare data for insertion
    
    #Test to process all tweets into one table
    process_all_tweets(engine, total_connections_windows)


    for k, connection in total_connections_windows.items(): 
        insert_data = [{'word1': pair[0], 'word2': pair[1], 'word_count': count} for pair, count in connection.items()]
        table_name = f'word_pairs_window_{k}'

        if not is_valid_table_name(table_name, max_window_size):
            raise ValueError(f"Invalid table name: {table_name}")
    # Insert into tweet_text_attention
        insert_query = text(f"""
            INSERT INTO {table_name} (word1, word2, word_count)
            VALUES (:word1, :word2, :word_count)
        """)

        try:
            with engine.begin() as conn:
                conn.execute(insert_query, insert_data)
                print(f"{len(insert_data)} word pairs inserted successfully into {table_name}'.")
        except SQLAlchemyError as error:
            print(f"Error inserting data: {error}")
        

# Function to save tweet_text_attention as CSV with timestamp

def save_tweet_text_attention_to_csv(engine):
    query = text("SELECT * FROM tweet_text_attention")
    df = pd.read_sql(query, engine)
    
    # Generate timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = f'tweet_text_attention_{timestamp}.csv'

    # Save to CSV
    df.to_csv(file_path, index=False)
    print(f"Table 'tweet_text_attention' saved to {file_path}.")

# Main logic
if __name__ == '__main__':

    print("numpy version: ", np.__version__)
    config = load_config()  # Assuming you have a function that loads config
    engine = connect(config)  # Assuming this function connects to the DB
    
    
    create_tables(engine, window_size=10)
    create_all_window_size_table(engine)

    # Process tweets with lemmatization

    process_tweet_text(engine)
    
    # Save the table to a CSV file with timestamp
    # save_tweet_text_attention_to_csv(engine)
