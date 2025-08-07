from getDBConnection import create_db_connection
from dotenv import load_dotenv
import os

load_dotenv('config.env')

def getAllArsenal():
    connection = create_db_connection(os.getenv('MERCURY_DB'))
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM arsenal WHERE is_setup = 1 order by tenant;")
    result = cursor.fetchall()
    return result

if __name__ == "__main__":
    allTenant = getAllArsenal()
    print("Total Tenant: ", len(allTenant))