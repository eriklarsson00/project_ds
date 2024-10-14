from sqlalchemy import Table, Column, String, Integer, MetaData, text
from sqlalchemy.exc import SQLAlchemyError
from collections import defaultdict
import pandas as pd
from connect import connect
from config import load_config
from sliding_window import sliding_window_connections  # Import the sliding window function

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

# Function to query 'tweets' and insert word pair counts into 'tweet_text_attention'
def process_tweet_text(engine, window_size=2):
    query = text("SELECT text FROM tweets")
    df = pd.read_sql(query, engine)

    # Create tweet_text_attention table
    create_tweet_text_attention_table(engine)

    # Clear existing data if necessary in tweet_text_attention
    clear_tweet_text_attention(engine)

    # Dictionary to store word pair counts
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

# Main logic
if __name__ == '__main__':
    config = load_config()  # Assuming you have a function that loads config
    engine = connect(config)  # Assuming this function connects to the DB
    
    # Process tweets and store word pair counts
    process_tweet_text(engine)
