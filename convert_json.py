
from sqlalchemy import  MetaData, Table, Column, BigInteger, Integer, Text, TIMESTAMP, ForeignKey
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

    hashtags_table = Table(
        'hashtags', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('tweet_id', BigInteger, ForeignKey('tweets.tweet_id'), nullable=False),
        Column('hashtag', String(255), nullable=False)
    )
    
    try:
        metadata.create_all(engine)  
        print("Tables  created successfully.")
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
 

    entities = row.get('entities', {})
    hashtags = entities.get('hashtags', [])
    hashtag_records = [
        {'tweet_id': tweet_id, 'hashtag': hashtag.get('tag')}
        for hashtag in hashtags if 'tag' in hashtag
    ]


    return {
        'tweet': {
            'tweet_id': tweet_id,
            'conversation_id': conversation_id,
            'author_id': author_id,
            'text': text,
            'retweet_count': retweet_count,
            'reply_count': reply_count,
            'like_count': like_count,
            'quote_count': quote_count,
            'created_at': created_at,
        },
        'hashtags': hashtag_records
    }


def insert_into_db(engine, data):
    try:
        with engine.begin() as conn:
            insert_query_tweet = text("""
                INSERT INTO tweets (tweet_id, conversation_id, author_id, text, 
                                    retweet_count, reply_count, like_count, quote_count, created_at)
                VALUES (:tweet_id, :conversation_id, :author_id, :text, 
                        :retweet_count, :reply_count, :like_count, :quote_count, :created_at)
            """)
            insert_hashtags_query = text("""
                INSERT INTO hashtags (tweet_id, hashtag)
                VALUES (:tweet_id, :hashtag)
            """)
            tweet_records = []
            hashtag_records = []

            # Process each row in one pass
            for row in data:
                unpacked = unpack_one_row(row)
                tweet_records.append(unpacked['tweet'])
                hashtag_records.extend(unpacked['hashtags'])


            conn.execute(insert_query_tweet, tweet_records)

            print(f'{len(tweet_records)} records inserted successfully')
            if hashtag_records:
                conn.execute(insert_hashtags_query, hashtag_records)
                print(f'{len(hashtag_records)} hashtags inserted successfully')
            else:
                print("No hashtags to insert.")
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

def drop_hashtag_table(engine):
    metadata = MetaData()
    
    # Reflect the database to access the 'tweets' table
    metadata.reflect(bind=engine)
    hashtag_table = metadata.tables.get('hashtags')
    
    if hashtag_table is not None:
        try:
            hashtag_table.drop(engine)
            print("Table 'hashtag' dropped successfully.")
        except SQLAlchemyError as error:
            print(f"Error dropping table: {error}")
    else:
        print("Table 'tweets' does not exist.")

# 6. Main logic
if __name__ == '__main__':
    config = load_config()
    engine = connect(config)
    drop_hashtag_table(engine)
    drop_table(engine)
    
    create_table(engine)


    path = 'data/Svd.json'
    data = load_json(path)

    twitter_data = data['data']  
    insert_into_db(engine, twitter_data)

    
    query = text("""
    SELECT * FROM tweets;
    """)

    query2 = text("""
    SELECT * FROM hashtags;
    """)

    df = pd.read_sql(query, engine)
    print(df.head())
    df2 = pd.read_sql(query2, engine)
    print(df2.head())
    
  
