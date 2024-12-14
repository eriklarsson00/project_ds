
#!pip install sqlalchemy pandas
from configparser import ConfigParser
from sqlalchemy import MetaData, Index, UniqueConstraint, Table, Column, BigInteger, Integer, TIMESTAMP, ForeignKey, String, Text, text, create_engine
from sqlalchemy.exc import SQLAlchemyError
import json
import pandas as pd

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
 
    DropAllTables(engine) #This has to be done
    #Code below is needed before all processing can be done
    metaData = MetaData()
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

    HashtagsTable = Table(
        'hashtags', metaData,
        Column('tweet_id', BigInteger, ForeignKey('tweets.tweet_id'), nullable=False),
        Column('hashtag', String(255), nullable=False)
    )
    
    WordPairTable = Table(
        'wordpairs', metaData,
        Column('window_size', Integer, nullable=False),
        Column('word1', String, nullable=False),
        Column('word2', String, nullable=False),
        Column('word_count', Integer, nullable=False),
        Index('ix_sliding_window_size', 'window_size'),
        UniqueConstraint('window_size', 'word1', 'word2', name='uix_word_pairs')  # Ensure uniqueness
    )
    try:
        metaData.create_all(engine)  
        print("Tables created successfully.")
    except SQLAlchemyError as error:
        print(f"Error creating table: {error}")


def DropAllTables(engine):
    
    #Code below is done to wipe everything if needed. Ordered in relation to Dependency among them
    metaData = MetaData()

    # Reflect the database to access the 'tweets' table
    metaData.reflect(bind=engine)

    TweetsTable = metaData.tables.get('tweets')
    HashtagsTable = metaData.tables.get('hashtags')
    WordPairTable = metaData.tables.get('wordpairs')

    if HashtagsTable is not None:
        try:
            HashtagsTable.drop(engine)
        except SQLAlchemyError as error:
            print(f"Error dropping table: {error}")

    if WordPairTable is not None:
        try:
            WordPairTable.drop(engine)
        except SQLAlchemyError as error:
            print(f"Error dropping table: {error}")

    if TweetsTable is not None:
        try:
            TweetsTable.drop(engine)
        except SQLAlchemyError as error:
            print(f"Error dropping table: {error}")


def InsertToDBFromJSON(engine, data, BatchName, batch_size=100):
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

            for i in range(0, len(Hashtags), batch_size):
                batch = Hashtags[i:i + batch_size]
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

def LoadJSONFile(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

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
  