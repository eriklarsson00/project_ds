import os
import threading
from sqlalchemy.sql import text
from concurrent.futures import ThreadPoolExecutor
from convert_json import unpack_one_row, insert_into_db, load_json, connect, load_config

# Thread-safe unique_id manager
class UniqueIDManager:
    def __init__(self, engine):
        # Initialize with the max unique_id from the database
        self.engine = engine
        self.lock = threading.Lock()
        self.unique_id = self._get_max_unique_id() + 1  # Start from the next unique_id

    def _get_max_unique_id(self):
        with self.engine.connect() as conn:
            result = conn.execute("SELECT COALESCE(MAX(unique_id), 0) FROM tweets")
            return result.scalar()

    def get_next_unique_id(self):
        with self.lock:
            current_id = self.unique_id
            self.unique_id += 1
            return current_id

def process_batch(batch, engine, unique_id_manager):
    # Assign unique_id to each tweet in the batch
    for tweet in batch:
        tweet['unique_id'] = unique_id_manager.get_next_unique_id()
    insert_into_db(engine, batch)

def process_tweets_file(file_path, engine, unique_id_manager):
    total_processed = 0
    
    # Use os.walk to recursively find all json files
    for root, dirs, files in os.walk(file_path):
        for file in files:
            if file.endswith(".json"):
                json_file_path = os.path.join(root, file)
                data = load_json(json_file_path)
                twitter_data = data.get('data', [])
                
                # Process in batches of 1000
                batch_size = 1000
                batches = [twitter_data[i:i + batch_size] for i in range(0, len(twitter_data), batch_size)]
                
                # Use ThreadPoolExecutor to process batches in parallel
                with ThreadPoolExecutor() as executor:
                    executor.map(lambda batch: process_batch(batch, engine, unique_id_manager), batches)
                
                total_processed += len(twitter_data)
    
    return total_processed

if __name__ == '__main__':
    path = "/Users/harivs/Documents/GitHub/project_ds/tweets"
    config = load_config()
    engine = connect(config)
    
    # Initialize UniqueIDManager to manage unique_id in a thread-safe manner
    unique_id_manager = UniqueIDManager(engine)
    
    total_processed = 0
    
    # Process the tweets file path (which includes subdirectories)
    total_processed += process_tweets_file(path, engine, unique_id_manager)
    
    print(f"Total tweets processed: {total_processed}")
