import json
from sqlalchemy.sql import text
from convert_json import unpack_one_row, insert_into_db, load_json, connect, load_config

def process_tweets_file(file_path, engine):
    data = load_json(file_path)
    twitter_data = data.get('data', [])
    
    # Process in batches of 1000
    batch_size = 1000
    for i in range(0, len(twitter_data), batch_size):
        batch = twitter_data[i:i+batch_size]
        insert_into_db(engine, batch)
    return len(twitter_data)
