import os
import pymysql
from dotenv import load_dotenv

load_dotenv('config.env')

def create_db_connection(db_name):
    """
    Create a database connection using environment variables.
    
    Returns:
        pymysql.Connection: Database connection object
    """
    try:
        print(f"Connecting to {db_name}")
        connection = pymysql.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=int(os.getenv('DB_PORT', 3306)),
            database=db_name
        )
        print(f"Successfully Connected to {db_name}")
        return connection
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise
