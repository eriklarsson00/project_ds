from sqlalchemy import Table, Column, String, Integer, MetaData, text
from sqlalchemy.exc import SQLAlchemyError
from collections import defaultdict
import pandas as pd
from connect import connect
from config import load_config
from sliding_window import sliding_window_connections  # Import the sliding window function
import re
import nltk
from nltk.corpus import words
from datetime import datetime  # Import datetime for timestamp

# Assuming 'swedish_words_korp' and 'swedish_words_wiktionary' are loaded from their respective corpora
swedish_words_korp = set()  # Load Swedish words from Språkbanken or other corpus
swedish_words_wiktionary = set()  # Load Swedish words from Wiktionary corpus

# Load Swedish words from NLTK corpus
nltk.download('words')
swedish_words_nltk = set([word.lower() for word in words.words() if word.isalpha()])


# Function to delete tweet_text_attention table
def delete_tweet_text_attention(engine):
    metadata = MetaData()
    tweet_text_attention = Table('tweet_text_attention', metadata, autoload_with=engine)

    try:
        tweet_text_attention.drop(engine)
        print("Table 'tweet_text_attention' deleted successfully.")
    except SQLAlchemyError as error:
        print(f"Error deleting table: {error}")


# Function to create tweet_text_attention table
def create_tweet_text_attention_table(engine):
    metadata = MetaData()
    
    tweet_text_attention = Table(
        'tweet_text_attention', metadata,
        Column('word1', String, nullable=False),
        Column('word2', String, nullable=False),
        Column('count', Integer, nullable=False)
    )
    
    try:
        metadata.create_all(engine)  # Create the table
        print("Table 'tweet_text_attention' created successfully.")
    except SQLAlchemyError as error:
        print(f"Error creating table: {error}")

# Function to remove all rows from tweet_text_attention table
def clear_tweet_text_attention(engine):
    try:
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM tweet_text_attention"))
            print("All rows removed from 'tweet_text_attention'.")
    except SQLAlchemyError as error:
        print(f"Error clearing table: {error}")

# Function to clean tweet text with configurable preprocessing options and corpus selection
def clean_tweet_text(tweet, remove_urls=True, remove_special_chars=True, remove_digits=True,
                     filter_swedish=False, corpus_options=None):
    """
    Preprocess the tweet text based on selected options.

    Args:
    - tweet (str): The tweet text to process.
    - remove_urls (bool): If True, removes URLs.
    - remove_special_chars (bool): If True, removes special characters.
    - remove_digits (bool): If True, removes digits.
    - filter_swedish (bool): If True, filters out non-Swedish words using selected corpus options.
    - corpus_options (dict): A dictionary of corpus sets (e.g., NLTK, Korp, Wiktionary) to be used for Swedish word filtering.
    
    Returns:
    - str: The cleaned tweet text.
    """
    # Remove URLs
    if remove_urls:
        tweet = re.sub(r'http\S+|www\S+|https\S+', '', tweet, flags=re.MULTILINE)

    # Remove special characters
    if remove_special_chars:
        tweet = re.sub(r'[^A-Za-z\s]', '', tweet)

    # Remove digits
    if remove_digits:
        tweet = re.sub(r'\d+', '', tweet)

    # Remove extra spaces
    tweet = re.sub(r'\s+', ' ', tweet).strip()

    # Filter words based on Swedish corpus
    if filter_swedish and corpus_options:
        words_in_tweet = tweet.split()
        combined_swedish_words = set().union(*corpus_options.values())  # Combine all selected corpus sets
        tweet = ' '.join([word for word in words_in_tweet if word.lower() in combined_swedish_words])

    return tweet

# Updated function to process tweet text with configurable cleaning
def process_tweet_text(engine, window_size=2, clean_options=None):
    query = text("SELECT text FROM tweets")
    df = pd.read_sql(query, engine)
    
    # Delete the table if it exists
    delete_tweet_text_attention(engine)

    # Create tweet_text_attention table
    create_tweet_text_attention_table(engine)

    # Clear existing data if necessary in tweet_text_attention
    clear_tweet_text_attention(engine)

    # Dictionary to store total connections
    total_connections = defaultdict(int)  
    
    # Iterate through each tweet's text
    for tweet_text in df['text']:
        connections = sliding_window_connections(window_size, tweet_text)
        
        # Accumulate counts
        for word_pair, count in connections.items():
            total_connections[word_pair] += count

    # Prepare data for insertion
    insert_data = [{'word1': pair[0], 'word2': pair[1], 'count': count} for pair, count in total_connections.items()]
    
    # Insert into tweet_text_attention
    insert_query = text("""
        INSERT INTO tweet_text_attention (word1, word2, count)
        VALUES (:word1, :word2, :count)
    """)

    try:
        with engine.begin() as conn:
            conn.execute(insert_query, insert_data)
            print(f"{len(insert_data)} word pairs inserted successfully into 'tweet_text_attention'.")
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
    config = load_config()  # Assuming you have a function that loads config
    engine = connect(config)  # Assuming this function connects to the DB
    
    # Define preprocessing options
    clean_options = {
        'remove_urls': True,
        'remove_special_chars': True,
        'remove_digits': True,
        'filter_swedish': False,
        'corpus_options': {
            'nltk': swedish_words_nltk,        # NLTK corpus
            'korp': swedish_words_korp,        # Språkbanken corpus
            'wiktionary': swedish_words_wiktionary  # Wiktionary corpus
        }
    }
    
    # Process tweets with the chosen cleaning options
    process_tweet_text(engine, window_size=2, clean_options=clean_options)
    
    # Save the table to a CSV file with timestamp
    # save_tweet_text_attention_to_csv(engine)
