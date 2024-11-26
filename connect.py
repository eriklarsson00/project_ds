#!pip install psycopg2-binary
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

def connect(connection_str):
    try:
        engine = create_engine(connection_str)
        print("Connected to the PostgreSQL server.")
        return engine
    except SQLAlchemyError as error:
        print(f"Error connecting to the database: {error}")
        return None


from config import load_config




if __name__ == '__main__':
    config = load_config()
    connect(config)



