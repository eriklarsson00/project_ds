from config import load_config
from connect import connect
import psycopg2
import json

def create_table(conn):
    try:
        cursor = conn.cursor()

        query ="""
        CREATE TABLE IF NOT EXISTS tweets (
            tweet_id BIGINT NOT NULL,
            conversation_id BIGINT,
            author_id INT NOT NULL,
            text TEXT,
            retweet_count INT,
            reply_count INT,
            like_count INT,
            quote_count INT,
            created_at TIMESTAMP,
            PRIMARY KEY (tweet_id)
            );
        """
        cursor.execute(query)
        conn.commit()
        print("Table created successfully")
    except (psycopg2.DatabaseError, Exception) as error:
        print(f' error creating table: {error}')


def load_json(path):
    with open(path, 'r') as file:
        data = json.load(file)
    return data



def unpack_one_row(row):
    tweet_id = row['id']
    conversation_id = row['conversation_id']
    author_id = row['author_id']
    text = row['text']
    retweet_count = row['public_metrics']['retweet_count']
    reply_count = row['public_metrics']['reply_count']
    like_count = row['public_metrics']['like_count']
    quote_count = row['public_metrics']['quote_count']
    created_at = row['created_at']
    return (tweet_id, conversation_id, author_id, text, retweet_count, reply_count, like_count, quote_count, created_at)


def insert_into_db(conn, data):
    try:
        with conn.cursor() as cursor:
            insert_query = """
        INSERT INTO tweets (tweet_id, conversation_id, author_id, text,
          retweet_count, reply_count, like_count, quote_count, created_at)
          Values (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
            records = [unpack_one_row(row) for row in data]
            cursor.executemany(insert_query, records)
            conn.commit()
            print('Data inserted successfully')
    except (psycopg2.DatabaseError, Exception) as error:
        print(f'Error inserting data: {error}')



if __name__ == '__main__':
    config = load_config()
    conn = connect(config)
    path = 'data/aftonbladet.json'
    data = load_json(path)

    twitter_data = data['data']
    insert_into_db(conn, twitter_data)
