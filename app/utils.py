import os
from datetime import datetime, timedelta
import pandas as pd
import pytz
from app.models import connect_to_database

# Define the directory to save the CSV files
REPORTS_DIRECTORY = 'reports/'

# Ensure the directory exists
os.makedirs(REPORTS_DIRECTORY, exist_ok=True)

def create_report(report_id):
    """
    Function to create a report for uptime and downtime calculation and update the status in the database.
    """
    try:
        # Connect to the database
        conn = connect_to_database()
        cursor = conn.cursor(dictionary=True)

        # Fetch data from the database
        cursor.execute("SELECT store_id, timestamp_utc, status FROM store_status")
        status_data = cursor.fetchall()
        
        cursor.execute("SELECT store_id, day_of_week, start_time_local, end_time_local FROM business_hours")
        business_hours_data = cursor.fetchall()
        
        cursor.execute("SELECT store_id, timezone_str FROM store_timezone")
        timezone_data = cursor.fetchall()

        # Convert data to pandas DataFrames
        status_df = pd.DataFrame(status_data)
        business_hours_df = pd.DataFrame(business_hours_data)
        timezone_df = pd.DataFrame(timezone_data)

        # Log the raw data to inspect for invalid datetime values
        print(f"Raw status data: {status_df.head()}")

        # Handle datetime conversion with error coercion
        status_df['timestamp_utc'] = pd.to_datetime(status_df['timestamp_utc'], errors='coerce')

        # Identify any rows that still have out-of-bounds or missing dates
        invalid_dates = status_df[status_df['timestamp_utc'].isna()]
        if not invalid_dates.empty:
            print(f"Invalid or out-of-bounds datetime entries found: {invalid_dates}")

        # Remove rows with invalid or NaT datetime values
        status_df.dropna(subset=['timestamp_utc'], inplace=True)

        # Determine the max timestamp to use as the current time
        max_timestamp = status_df['timestamp_utc'].max()
        current_time = pd.to_datetime(max_timestamp)

        # Merge the data for further processing
        merged_df = status_df.merge(timezone_df, on='store_id', how='left')
        merged_df = merged_df.merge(business_hours_df, on='store_id', how='left')

        # Fix the FutureWarning by using the suggested approach
        merged_df['timezone_str'] = merged_df['timezone_str'].fillna('America/Chicago')

        # Convert timestamps to local time for each store
        merged_df['local_time'] = merged_df.apply(lambda row: convert_to_local_time(row['timestamp_utc'], row['timezone_str']), axis=1)

        # Calculate uptime and downtime
        report_data = []
        for store_id, group in merged_df.groupby('store_id'):
            report_entry = calculate_uptime_downtime(store_id, group, current_time)
            report_data.append(report_entry)

        # Convert the report to a DataFrame
        report_df = pd.DataFrame(report_data)

        # Convert the report DataFrame to CSV format
        report_csv = report_df.to_csv(index=False)

        # Save the CSV to a file
        csv_filename = os.path.join(REPORTS_DIRECTORY, f'report_{report_id}.csv')
        with open(csv_filename, 'w') as csv_file:
            csv_file.write(report_csv)

        # Update the report status in the database with the CSV file content
        cursor.execute("UPDATE reports SET status = %s, report_data = %s WHERE report_id = %s", ('Complete', report_csv, report_id))
        conn.commit()
    
    except Exception as e:
        print(f"Error generating report: {e}")
        # Update the status to 'Failed' in case of any error
        cursor.execute("UPDATE reports SET status = %s WHERE report_id = %s", ('Failed', report_id))
        conn.commit()
    
    finally:
        cursor.close()
        conn.close()

def convert_to_local_time(utc_time_str, timezone_str):
    """
    Convert UTC time to the local time for the given timezone.
    """
    try:
        utc_time = pd.to_datetime(utc_time_str, errors='coerce')
        if pd.isna(utc_time):
            return None
        local_tz = pytz.timezone(timezone_str)
        local_time = utc_time.tz_localize('UTC').astimezone(local_tz)
        return local_time
    except Exception as e:
        print(f"Error converting time: {e}")
        return None

def calculate_uptime_downtime(store_id, store_data, current_time):
    """
    Calculate the uptime and downtime for a given store within business hours.
    """
    # Filter business hours for the store
    business_hours = store_data.drop_duplicates(subset=['store_id', 'day_of_week', 'start_time_local', 'end_time_local'])

    # Prepare to calculate uptime and downtime
    total_minutes = timedelta(hours=0)
    uptime_minutes_last_hour = 0
    downtime_minutes_last_hour = 0
    uptime_hours_last_day = 0
    downtime_hours_last_day = 0
    uptime_hours_last_week = 0
    downtime_hours_last_week = 0

    # Iterate over each day of business hours
    for _, hours_row in business_hours.iterrows():
        day_of_week = hours_row['day_of_week']
        start_time_local = pd.to_datetime(hours_row['start_time_local']).time()
        end_time_local = pd.to_datetime(hours_row['end_time_local']).time()

        # Filter the data for observations within this day
        observations = store_data[(store_data['day_of_week'] == day_of_week)]

        # Iterate over the observations to calculate uptime and downtime
        for _, obs in observations.iterrows():
            obs_time = obs['local_time'].time()
            status = obs['status']

            # Check if observation falls within business hours
            if start_time_local <= obs_time <= end_time_local:
                # Logic to calculate uptime and downtime within business hours
                if status == 'active':
                    uptime_minutes_last_hour += calculate_minutes(obs_time, start_time_local, end_time_local)
                else:
                    downtime_minutes_last_hour += calculate_minutes(obs_time, start_time_local, end_time_local)

    # Summarize all calculations to create the report entry
    report_entry = {
        'store_id': store_id,
        'uptime_last_hour': uptime_minutes_last_hour,
        'uptime_last_day': uptime_hours_last_day,
        'uptime_last_week': uptime_hours_last_week,
        'downtime_last_hour': downtime_minutes_last_hour,
        'downtime_last_day': downtime_hours_last_day,
        'downtime_last_week': downtime_hours_last_week
    }

    return report_entry

def calculate_minutes(obs_time, start_time, end_time):
    """
    Helper function to calculate the minutes of uptime or downtime.
    """
    if obs_time < start_time:
        return 0
    elif obs_time > end_time:
        return 0
    else:
        return (datetime.combine(datetime.min, end_time) - datetime.combine(datetime.min, obs_time)).seconds / 60
