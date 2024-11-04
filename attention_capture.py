from sqlalchemy import Table, Column, String, Integer, MetaData, text
from sqlalchemy.exc import SQLAlchemyError
from collections import defaultdict
import pandas as pd
from connect import connect
from config import load_config
from sliding_window import sliding_window_connections  # Import the sliding window function
import stanza  # Import stanza
import re
from datetime import datetime  # Import datetime for timestamp

# Initialize Stanza for Swedish language
stanza.download('sv')  # Download the Swedish model if not already downloaded
nlp = stanza.Pipeline('sv')  # Initialize Stanza pipeline for Swedish

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

# Function to clean and lemmatize tweet text
def clean_and_lemmatize_tweet(tweet, remove_urls=True, remove_special_chars=True, remove_digits=True):
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

    # Lemmatize using Stanza
    doc = nlp(tweet)
    lemmatized_words = [word.lemma for sentence in doc.sentences for word in sentence.words if word.lemma]

    return ' '.join(lemmatized_words)

# Updated function to process tweet text with lemmatization
def process_tweet_text(engine, window_size=2):
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
        # Clean and lemmatize tweet
        cleaned_text = clean_and_lemmatize_tweet(tweet_text)
        
        # Generate connections using sliding window
        connections = sliding_window_connections(window_size, cleaned_text)
        
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
    
    # Process tweets with lemmatization
    process_tweet_text(engine, window_size=2)
    
    # Save the table to a CSV file with timestamp
    # save_tweet_text_attention_to_csv(engine)
