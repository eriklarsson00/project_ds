#!pip install sqlalchemy pandas
from configparser import ConfigParser
from sqlalchemy import MetaData, text, create_engine
from sqlalchemy.exc import SQLAlchemyError
import json
import pandas as pd
from DBController import LoadConfig, ConnectDB
import uunet.multinet as ml
from itertools import combinations
from collections import Counter
import itertools
from sqlalchemy import MetaData, text
import pandas as pd

from sqlalchemy import Table, Column, MetaData, String, Integer, PrimaryKeyConstraint



#This is code for creating a co-occurrence network from the hashtags in the tweets. Purpose is as proof of concept that network analysis can be conducted on the data.

# Create the cooccurrence_network table
def create_cooccurrence_table(engine):
    """
    Create table for cooccurance Network
    params: engine for connection to db
    """
    metadata = MetaData()

    cooccurrence_network = Table(
        'cooccurrence_network',
        metadata,
        Column('hashtag1', String, nullable=False),
        Column('hashtag2', String, nullable=False),
        Column('weight', Integer, nullable=False),
        PrimaryKeyConstraint('hashtag1', 'hashtag2')
    )

    try:
        # Create the table if it doesn't exist
        metadata.create_all(engine)
        print("cooccurrence_network table created successfully.")
    except Exception as e:
        print(f"Error creating cooccurrence_network table: {e}")



def delete_cooccurrence_table(engine):
    """
    Drops the cooccurrence_network table if it exists using SQLAlchemy MetaData.
    """
    metadata = MetaData()
    metadata.reflect(bind=engine)  # Reflect current database schema
    if 'cooccurrence_network' in metadata.tables:
        try:
            table = metadata.tables['cooccurrence_network']
            table.drop(bind=engine)  # Drop the table
            print("cooccurrence_network table deleted successfully.")
        except Exception as e:
            print(f"Error deleting cooccurrence_network table: {e}")
    else:
        print("Table cooccurrence_network does not exist.")


def fetch_hashtags(engine, batch_process=None, from_date=None, to_date=None):
    try:
        # Reflect metadata to ensure tables exist
        metadata = MetaData()
        metadata.reflect(bind=engine)

        if 'hashtags' not in metadata.tables or 'tweets' not in metadata.tables:
            print("One or both tables ('hashtags', 'tweets') do not exist in the database.")
            return None

        # Query to fetch data for network analysis
        query = """
        SELECT h.*, 
               t.author_id, 
               t.created_at, 
               t.batch_name
        FROM hashtags h
        JOIN tweets t ON h.tweet_id = t.tweet_id
        """

        filters = []
        params = {}

        if batch_process:
            filters.append("t.batch_name = :batch_process")
            params['batch_process'] = batch_process
        if from_date:
            filters.append("t.created_at >= :from_date")
            params['from_date'] = from_date
        if to_date:
            filters.append("t.created_at <= :to_date")
            params['to_date'] = to_date

        # Append filters to the query if any exist
        if filters:
            query += " WHERE " + " AND ".join(filters)

        # Fetch the data into a DataFrame using parameterized query
        df = pd.read_sql(text(query), engine, params=params)
        print("Data fetched successfully with author_id, date, and batch_name.")
        return df

    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

      
def update_or_insert_hashtag_row(engine, hashtag1, hashtag2, weight):
    """
    updates existing row with new weight if both hashtags already exists
    if unique pair add it to the table
    """
    query = text("""
        INSERT INTO cooccurrence_network (hashtag1, hashtag2, weight)
        VALUES (:hashtag1, :hashtag2, :weight)
        ON CONFLICT (hashtag1, hashtag2) 
        DO UPDATE SET weight = cooccurrence_network.weight + :weight
    """)
    with engine.begin() as conn:  # Automatically manages transactions
        conn.execute(query, {'hashtag1': hashtag1, 'hashtag2': hashtag2, 'weight': weight})

def bulk_update_or_insert_hashtag_rows(engine, pairs_with_weights):
    query = text("""
        INSERT INTO cooccurrence_network (hashtag1, hashtag2, weight)
        VALUES (:hashtag1, :hashtag2, :weight)
        ON CONFLICT (hashtag1, hashtag2) 
        DO UPDATE SET weight = cooccurrence_network.weight + EXCLUDED.weight
    """)
    try:
        with engine.begin() as conn:  
            conn.execute(
                query,
                [{'hashtag1': pair[0], 'hashtag2': pair[1], 'weight': weight}
                 for pair, weight in pairs_with_weights.items()]
            )
        print("insert/update completed successfully.")
    except Exception as e:
        print(f"Error during insert/update: {e}")

def build_cooccurence_network(engine, df):
    # List to hold hashtag pairs
    cooccurrence_edges = []
    # Step 1: Filter tweets with more than one hashtag
    group_sizes = df['tweet_id'].value_counts()
    valid_tweet_ids = group_sizes[group_sizes > 1].index
    filtered_df = df[df['tweet_id'].isin(valid_tweet_ids)]

    # Step 2: Group by tweet_id and generate hashtag pairs
    grouped = filtered_df.groupby('tweet_id')
    for _, tweet in grouped:
        hashtags = tweet['hashtag'].str.lower().unique().tolist()
        if len(hashtags) > 1:
            # Generate all pairs of hashtags in the tweet
            edges = itertools.combinations(sorted(hashtags), 2) 
            cooccurrence_edges.extend(edges)

    print(cooccurrence_edges)
    pair_counts = Counter(cooccurrence_edges)
    bulk_update_or_insert_hashtag_rows(engine, pair_counts)



def df_network(d):
    return pd.DataFrame.from_dict(d)
def create_network(dataframe):
    import matplotlib.pyplot as plt
    
    # Create an empty network
    net = ml.empty()
    ml.add_layers(net, ["HT"])  # Add one layer for hashtags
    ml.layers(net)

    # Extract edges and their weights
    from_hashtag = dataframe['hashtag1'].tolist()
    to_hashtag = dataframe['hashtag2'].tolist()
    weights = dataframe['weight'].tolist()

    # Add edges to the network
    edges = {
        "from_actor": from_hashtag,
        "from_layer": ["HT"] * len(from_hashtag),
        "to_actor": to_hashtag,
        "to_layer": ["HT"] * len(to_hashtag),
        
    }
    ml.add_edges(net, edges)

    # Debug: Print edges and network structure
    print("Edges added:", ml.edges(net))
    print("Layers:", ml.layers(net))

    # Return network edges for inspection
    return net, df_network(ml.edges(net))

def plot_network(net):
    """
    Function to plot a multiplex network.
    Args:
        net: The network object created using `uunet.multinet`.
    """
    import matplotlib.pyplot as plt

    try:
        # Compute the layout
        layout = ml.layout_multiforce(net)  # You can customize the layout parameters if needed
        # Plot the network
        ml.plot(
            net,
            layout=layout,
            vertex_labels_bbox={"boxstyle": 'round4', "fc": 'white'}
        )
        plt.show()
    except Exception as e:
        print("Plotting failed:", e)
    

from sqlalchemy import MetaData

def table_exists_reflection(engine, table_name):
    metadata = MetaData()
    metadata.reflect(bind=engine)
    return table_name in metadata.tables



def get_cooccurrence_data(engine):
    """
    Fetch all rows from the cooccurrence_network table and return as a DataFrame.
    """
    query = text("""
        SELECT hashtag1, hashtag2, weight
        FROM cooccurrence_network
    """)
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        print("Data fetched successfully from cooccurrence_network.")
        return df
    except Exception as e:
        print(f"Error fetching cooccurrence data: {e}")
        return pd.DataFrame()

# Main function to demonstrate the workflow
if __name__ == '__main__':
    config = LoadConfig(filename='../database.ini')
    engine = ConnectDB(config)
    delete_cooccurrence_table(engine=engine)
    create_cooccurrence_table(engine)
    #print(f"Engine type: {type(engine)}") 
    #create_cooccurrence_table(engine=engine)
    #if table_exists_reflection(engine, 'cooccurrence_network'):
    #    print("Table exists.")
    #else:
    #    print("Table does not exist.")
#
    #df = fetch_hashtags(engine, batch_process='svtnyheter1')
    #if df is not None:
    #    print(df.shape)  # Display the first few rows

    #small_group_df = df
    #
    #build_cooccurence_network(engine, small_group_df)
    #grouped_df.to_csv("test.csv")
    #net, test = create_network(grouped_df)
    #print(test)
    #df =  get_cooccurrence_data(engine=engine)
    #print(df)
    #plot_network(net)

