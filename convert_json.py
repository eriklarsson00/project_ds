
from sqlalchemy import  MetaData, Table, Column, BigInteger, Integer, Text, TIMESTAMP
from sqlalchemy.exc import SQLAlchemyError
import json
from connect import connect
from config import load_config
import pandas as pd

def create_table(engine):
    metadata = MetaData()
    
    tweets_table = Table(
        'tweets', metadata,
        Column('tweet_id', BigInteger, primary_key=True),
        Column('conversation_id', BigInteger),
        Column('author_id', Integer, nullable=False),
        Column('text', Text),
        Column('retweet_count', Integer),
        Column('reply_count', Integer),
        Column('like_count', Integer),
        Column('quote_count', Integer),
        Column('created_at', TIMESTAMP)
    )
    
    try:
        metadata.create_all(engine)  
        print("Table created successfully.")
    except SQLAlchemyError as error:
        print(f"Error creating table: {error}")


from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, DateTime
from sqlalchemy.sql import text


def unpack_one_row(row):
    tweet_id = row.get('id')
    conversation_id = row.get('conversation_id')
    author_id = row.get('author_id')
    text = row.get('text')
    public_metrics = row.get('public_metrics', {})
    retweet_count = public_metrics.get('retweet_count', 0)
    reply_count = public_metrics.get('reply_count', 0)
    like_count = public_metrics.get('like_count', 0)
    quote_count = public_metrics.get('quote_count', 0)
    created_at = row.get('created_at')
    
    return {
        'tweet_id': tweet_id,
        'conversation_id': conversation_id,
        'author_id': author_id,
        'text': text,
        'retweet_count': retweet_count,
        'reply_count': reply_count,
        'like_count': like_count,
        'quote_count': quote_count,
        'created_at': created_at
    }


def insert_into_db(engine, data):
    try:
        with engine.begin() as conn:
            insert_query = text("""
                INSERT INTO tweets (tweet_id, conversation_id, author_id, text, 
                                    retweet_count, reply_count, like_count, quote_count, created_at)
                VALUES (:tweet_id, :conversation_id, :author_id, :text, 
                        :retweet_count, :reply_count, :like_count, :quote_count, :created_at)
            """)

            records = [unpack_one_row(row) for row in data]

            conn.execute(insert_query, records)

            print(f'{len(records)} records inserted successfully')
    except Exception as error:
        print(f'Error inserting data: {error}')




  




def load_json(path):
    with open(path, 'r') as file:
        data = json.load(file)
    return data


def drop_table(engine):
    metadata = MetaData()
    
    # Reflect the database to access the 'tweets' table
    metadata.reflect(bind=engine)
    tweets_table = metadata.tables.get('tweets')
    
    if tweets_table is not None:
        try:
            tweets_table.drop(engine)
            print("Table 'tweets' dropped successfully.")
        except SQLAlchemyError as error:
            print(f"Error dropping table: {error}")
    else:
        print("Table 'tweets' does not exist.")
# 6. Main logic
if __name__ == '__main__':
    config = load_config()
    engine = connect(config)
  
    create_table(engine)


    path = 'data/aftonbladet.json'
    data = load_json(path)

    twitter_data = data['data']  
    insert_into_db(engine, twitter_data)

    
    query = text("""
    SELECT * FROM tweets;
    """)

    df = pd.read_sql(query, engine)
    print(df.head())
    
  
