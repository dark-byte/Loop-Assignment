from mysql.connector import connect
from db_config import DB_CONFIG

# Function to connect to the database
def connect_to_database():
    conn = connect(**DB_CONFIG)
    return conn
