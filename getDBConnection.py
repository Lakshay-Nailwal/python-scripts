import os
import pymysql
from dotenv import load_dotenv
import json

load_dotenv('config.env')

def create_db_connection(db_name):
    try:
        host = ""
        user = ""
        password = ""
        port = 3306  # default

        
        if db_name == 'partner':
            pass  # fill in if needed later

        elif db_name == 'vault':
            vault_config_str = os.getenv("VAULT_DB_CONFIG", "{}")
            vault_config: dict = json.loads(vault_config_str)
            host = vault_config['host']
            user = vault_config['user']
            password = vault_config['password']
            port = vault_config['port']
        else :
            host = os.getenv('DB_HOST')
            user = os.getenv('DB_USER')
            password = os.getenv('DB_PASSWORD')
            port = int(os.getenv('DB_PORT', 3306))
        
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            port=port,
            database=db_name
        )
        return connection

    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise
