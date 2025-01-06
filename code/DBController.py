
#!pip install sqlalchemy pandas
from configparser import ConfigParser
from sqlalchemy import MetaData, Index, UniqueConstraint, Table, Column, BigInteger, Integer, TIMESTAMP, ForeignKey, String, Text, text, create_engine
from sqlalchemy.exc import SQLAlchemyError
import json
import pandas as pd


def LoadJSONFile(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

def AddBatch(engine, BatchName):
    with engine.begin() as conn:
        # Insert the batch into the `batches` table with status 'NEW'
        InsertBatchQuery = text("""
            INSERT INTO batches (batch_name, batch_status)
            VALUES (:batch_name, 'NEW')
            ON CONFLICT (batch_name) DO NOTHING
        """)
        conn.execute(InsertBatchQuery, {'batch_name': BatchName})

def ReadNewBatchesFromDB(engine):
    """
    Fetch all records from the `batches` table where `batch_status` is 'NEW'.
    """
    try:
        with engine.connect() as conn:
            query = text("SELECT batch_name FROM batches WHERE batch_status = 'NEW'")
            result = conn.execute(query)
            new_batches = [{'batch_name': row['batch_name']} for row in result]
            return new_batches
    except SQLAlchemyError as error:
        print(f"Error reading new batches from database: {error}")
        return []

def UpdateBatchStatus(engine, batch_name, status):
    """
    Update the `batch_status` of a batch in the `batches` table.
    """
    try:
        with engine.begin() as conn:
            query = text("""
                UPDATE batches
                SET batch_status = :status
                WHERE batch_name = :batch_name
            """)
            conn.execute(query, {'batch_name': batch_name, 'status': status})
            print(f"Batch '{batch_name}' updated to status '{status}'.")

            if status == 'SUCCESS':
                update_tweets_query = text("""
                    UPDATE tweets
                    SET batch_name = 'NONE'
                    WHERE batch_name = :batch_name
                """)
                conn.execute(update_tweets_query, {'batch_name': batch_name})
                print(f"All tweets with batch_name '{batch_name}' have been updated to 'NONE'.")
                
    except SQLAlchemyError as error:
        print(f"Error updating batch status for '{batch_name}': {error}")

def LoadConfig(filename='config/database.ini', section='postgresql'): 
    #Copy this declaration and definition for other DB types supported by airflow, superset and sqlalchemy
    parser = ConfigParser()
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')

    ConnectionStr = f"postgresql+psycopg2://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}"
    return ConnectionStr

def ConnectDB(ConnectionStr):
    try:
        engine = create_engine(ConnectionStr)
        print("Connected to the PostgreSQL server.")
        return engine
    except SQLAlchemyError as error:
        print(f"Error connecting to the database: {error}")
        return None

def CreateAllTables(engine):
    DropAllTables(engine)  # Drop all tables before creating new ones
    metaData = MetaData()

    # Define the tweets table
    TweetsTable = Table(
        'tweets', metaData,
        Column('tweet_id', BigInteger, primary_key=True),
        Column('conversation_id', BigInteger),
        Column('author_id', Integer, nullable=False),
        Column('text', Text),
        Column('retweet_count', Integer),
        Column('reply_count', Integer),
        Column('like_count', Integer),
        Column('quote_count', Integer),
        Column('created_at', TIMESTAMP),
        Column('batch_name', Text, nullable=True)
    )

    # Define the hashtags table
    HashtagsTable = Table(
        'hashtags', metaData,
        Column('tweet_id', BigInteger, ForeignKey('tweets.tweet_id'), nullable=False),
        Column('hashtag', String(255), nullable=False)
    )

    # Define the word pairs table
    WordPairTable = Table(
        'wordpairs', metaData,
        Column('window_size', Integer, nullable=False),
        Column('word1', String, nullable=False),
        Column('word2', String, nullable=False),
        Column('word_count', Integer, nullable=False),
        Index('ix_sliding_window_size', 'window_size'),
        UniqueConstraint('window_size', 'word1', 'word2', name='uix_word_pairs')
    )

    # Define the batches table
    BatchesTable = Table(
        'batches', metaData,
        Column('batch_name', String(255), primary_key=True),
        Column('batch_status', String(255), nullable=False)
    )

    try:
        metaData.create_all(engine, checkfirst=True)
        print("Tables created successfully.")
    except SQLAlchemyError as error:
        print(f"Error creating tables: {error}")



def DropAllTables(engine):
    """
    Drop all tables, indexes, and constraints in the database schema.
    """
    metaData = MetaData()
    metaData.reflect(bind=engine)

    try:
        # Drop tables in reverse order to resolve dependencies
        for table in reversed(metaData.sorted_tables):
            print(f"Dropping table: {table.name}")
            table.drop(engine, checkfirst=True)

        # Explicitly drop all remaining indexes
        with engine.connect() as conn:
            indexes = conn.execute(text("""
                SELECT indexname 
                FROM pg_indexes 
                WHERE schemaname = 'public';
            """)).fetchall()

            for index in indexes:
                index_name = index[0]
                print(f"Dropping index: {index_name}")
                conn.execute(text(f"DROP INDEX IF EXISTS {index_name}"))

        print("All tables and indexes dropped successfully.")
    except SQLAlchemyError as error:
        print(f"Error while dropping tables or indexes: {error}")


def InsertToDBFromJSON(engine, data, BatchName, BatchSize=10000):
    try:
        with engine.begin() as conn:
           #Tweets table contains the entire tweet extracted from the json
            InsertTweetsQuery = text( """
                INSERT INTO tweets (tweet_id, conversation_id, author_id, text, 
                                    retweet_count, reply_count, like_count, quote_count, created_at, batch_name)
                VALUES (:tweet_id, :conversation_id, :author_id, :text, 
                        :retweet_count, :reply_count, :like_count, :quote_count, :created_at, :batch_name)
                                       """)
            
            #Hashtags table only contains Hashtags linked with tweet_ids
            InsertHashtagsQuery = text( """
                INSERT INTO hashtags (tweet_id, hashtag)
                VALUES (:tweet_id, :hashtag)
                                          """)
            
            Tweets = []
            Hashtags = []

            # Process each row in one pass
            for row in data:
                RowData = UnpackOneRow(row)
                RowData['tweet']['batch_name'] = BatchName
                Tweets.append(RowData['tweet'])
                Hashtags.extend(RowData['hashtags'])
            conn.execute(InsertTweetsQuery, Tweets)

            for i in range(0, len(Hashtags), BatchSize):
                batch = Hashtags[i:i + BatchSize]
                if batch:
                    conn.execute(InsertHashtagsQuery, batch)
            
    except Exception as error:
        print(f'Error inserting data: {error}')

def UnpackOneRow(row):
    
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
            'created_at': created_at
        },

        'hashtags': hashtag_records
    }

def InsertWordPairsToDB(engine, InsertData):
    
    InsertQuery = text( """
                        INSERT INTO wordpairs (window_size, word1, word2, word_count)
                        VALUES (:window_size, :word1, :word2, :word_count)
                        ON CONFLICT (window_size, word1, word2)  -- Ensure a unique constraint exists on these columns
                        DO UPDATE SET word_count = wordpairs.word_count + EXCLUDED.word_count
                        """)
    try:
        with engine.begin() as conn:
            conn.execute(InsertQuery, InsertData)
            
    except SQLAlchemyError as error:
            print(f"Error inserting/updating data: {error}")


def ReadBatchFromDB(engine, batch_name):
    if batch_name == "None":
        FilterQuery = text( """
                            SELECT text FROM tweets
                            """)
        parameters = {}

    else:
        FilterQuery = text( """
                            SELECT text FROM tweets
                            WHERE batch_name = :batch_name
                            """)
        parameters = {"batch_name": batch_name}
    
    DataFrame = pd.read_sql(FilterQuery, engine, params=parameters)
    return DataFrame

if __name__ == '__main__':
    
    config = LoadConfig()
    engine = ConnectDB(config)
    CreateAllTables(engine)
    LoadTestData = False

    if LoadTestData == True:
        Data = LoadJSONFile('tweets/aftonbladet/aftonbladet_1jzu9lk96gu5npvzel8joi184kxtmity35agj37h2ykd.json')
        TwitterData = Data['data']  
        InsertToDBFromJSON(engine, TwitterData, "aftonbladet")

        ReadQuery= text( """
                        SELECT * FROM tweets;
                     """)

        df = pd.read_sql(ReadQuery, engine)
        print(df.head())
  

