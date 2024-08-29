import mysql.connector
import pandas as pd
from db_config import DB_CONFIG  # Import DB configuration from db_config.py

# Function to connect to the database
def connect_to_database():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        print("Database connection successful")
        return conn
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        exit(1)

# Function to clean and format datetime values
def clean_datetime(df, column_name):
    # Remove 'UTC' and convert to the correct format
    df[column_name] = pd.to_datetime(df[column_name].str.replace(' UTC', ''), errors='coerce')
    return df

# Function to ingest CSV data into MySQL table
def ingest_csv_to_mysql(csv_file_path, table_name, columns, datetime_columns=None):
    try:
        conn = connect_to_database()
        cursor = conn.cursor()

        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(csv_file_path)

        # Clean datetime columns if specified
        if datetime_columns:
            for col in datetime_columns:
                df = clean_datetime(df, col)

        # Insert data into the table row by row
        for _, row in df.iterrows():
            values = tuple(row[column] for column in columns)
            placeholders = ', '.join(['%s'] * len(values))
            insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            
            cursor.execute(insert_query, values)

        # Commit the transaction
        conn.commit()
        print(f"Data from {csv_file_path} ingested successfully into {table_name}")

    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# Ingest store status data with datetime cleaning
ingest_csv_to_mysql('data/store_status.csv', 'store_status', ['store_id', 'timestamp_utc', 'status'], datetime_columns=['timestamp_utc'])

# Ingest business hours data
ingest_csv_to_mysql('data/store_hours.csv', 'business_hours', ['store_id', 'day_of_week', 'start_time_local', 'end_time_local'])

# Ingest store timezone data
ingest_csv_to_mysql('data/store_timezone.csv', 'store_timezone', ['store_id', 'timezone_str'])
