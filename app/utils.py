import os
from datetime import datetime, timedelta, time
import pandas as pd
import pytz
import mysql.connector
from app.models import connect_to_database

# Define the directory to save the CSV files
REPORTS_DIRECTORY = 'reports/'

# Ensure the directory exists
os.makedirs(REPORTS_DIRECTORY, exist_ok=True)

def create_report(report_id):
    # Connect to the database using mysql.connector
    connection = connect_to_database()
    cursor = connection.cursor(dictionary=True)  # Use dictionary cursor to fetch rows as dictionaries

    try:
        # Fetch data from the database
        cursor.execute("SELECT store_id, day_of_week, start_time_local, end_time_local FROM business_hours")
        business_hours_data = cursor.fetchall()
        business_hours_df = pd.DataFrame(business_hours_data)

        cursor.execute("SELECT store_id, timestamp_utc, status FROM store_status")
        store_status_data = cursor.fetchall()
        store_status_df = pd.DataFrame(store_status_data)

        cursor.execute("SELECT store_id, timezone_str FROM store_timezone")
        store_timezone_data = cursor.fetchall()
        store_timezone_df = pd.DataFrame(store_timezone_data)

        # Convert 'start_time_local' and 'end_time_local' to proper datetime objects using 'day_of_week'
        def convert_to_datetime(row):
            # Get the current week start (Monday)
            current_date = datetime.now()
            week_start = current_date - timedelta(days=current_date.weekday())  # Start of the week (Monday)
            # Compute the specific date for the given 'day_of_week'
            target_date = week_start + timedelta(days=row['day_of_week'])

            # Convert 'start_time_local' and 'end_time_local' from Timedelta to time
            start_time = (pd.to_timedelta(row['start_time_local']).components.hours, 
                          pd.to_timedelta(row['start_time_local']).components.minutes,
                          pd.to_timedelta(row['start_time_local']).components.seconds)
            
            end_time = (pd.to_timedelta(row['end_time_local']).components.hours, 
                        pd.to_timedelta(row['end_time_local']).components.minutes,
                        pd.to_timedelta(row['end_time_local']).components.seconds)

            # Combine the date with the start and end times
            start_datetime = datetime.combine(target_date, time(start_time[0], start_time[1], start_time[2]))
            end_datetime = datetime.combine(target_date, time(end_time[0], end_time[1], end_time[2]))
            
            return pd.Series({'start_time_local': start_datetime, 'end_time_local': end_datetime})

        business_hours_df[['start_time_local', 'end_time_local']] = business_hours_df.apply(convert_to_datetime, axis=1)

        # Convert timestamp_utc to local timezone for each store
        store_status_df = store_status_df.merge(store_timezone_df, on='store_id')
        store_status_df['timestamp_local'] = store_status_df.apply(
            lambda row: pd.to_datetime(row['timestamp_utc']).tz_localize('UTC').astimezone(pytz.timezone(row['timezone_str'])),
            axis=1
        )

        # Initialize an empty list to hold report data
        report_data = []

        # Process each store
        for store_id, group in store_status_df.groupby('store_id'):
            # Get store business hours
            store_hours = business_hours_df[business_hours_df['store_id'] == store_id]
            if store_hours.empty:
                continue

            # Calculate uptime and downtime for each period
            uptime_last_hour = uptime_last_day = uptime_last_week = 0
            downtime_last_hour = downtime_last_day = downtime_last_week = 0

            now = datetime.now(pytz.utc)

            # Convert business hours to local timezone
            for _, hours in store_hours.iterrows():
                local_timezone = pytz.timezone(store_timezone_df.loc[store_timezone_df['store_id'] == store_id, 'timezone_str'].values[0])
                start_time_local = local_timezone.localize(hours['start_time_local'])
                end_time_local = local_timezone.localize(hours['end_time_local'])

                # Get observations within business hours
                within_business_hours = group[(group['timestamp_local'] >= start_time_local) & (group['timestamp_local'] <= end_time_local)]

                # Calculate uptime and downtime based on observations
                for period, duration in [('hour', 60), ('day', 1440), ('week', 10080)]:
                    end_time = now
                    start_time = now - timedelta(minutes=duration)

                    period_data = within_business_hours[(within_business_hours['timestamp_local'] >= start_time) & (within_business_hours['timestamp_local'] <= end_time)]
                    
                    if not period_data.empty:
                        active_periods = period_data[period_data['status'] == 'active']
                        inactive_periods = period_data[period_data['status'] == 'inactive']

                        # Interpolation logic: assume uptime or downtime between observations
                        total_minutes = (end_time - start_time).total_seconds() / 60
                        observed_minutes = len(active_periods) * 1  # Assuming each observation represents 1 minute
                        uptime = observed_minutes / total_minutes * duration
                        downtime = duration - uptime

                        # Assign values to respective periods
                        if period == 'hour':
                            uptime_last_hour = uptime
                            downtime_last_hour = downtime
                        elif period == 'day':
                            uptime_last_day = uptime / 60
                            downtime_last_day = downtime / 60
                        elif period == 'week':
                            uptime_last_week = uptime / 60
                            downtime_last_week = downtime / 60

            # Append the store report data
            report_data.append({
                'store_id': store_id,
                'uptime_last_hour': uptime_last_hour,
                'uptime_last_day': uptime_last_day,
                'uptime_last_week': uptime_last_week,
                'downtime_last_hour': downtime_last_hour,
                'downtime_last_day': downtime_last_day,
                'downtime_last_week': downtime_last_week
            })

        # Create a DataFrame for the report
        report_df = pd.DataFrame(report_data)

        # Save the report to a CSV file
        report_filename = os.path.join(REPORTS_DIRECTORY, f'report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
        report_df.to_csv(report_filename, index=False)

        # Read CSV file content for update
        with open(report_filename, 'r') as file:
            report_csv = file.read()

        # Update the report status in the database with the CSV file content
        cursor.execute("UPDATE reports SET status = %s, report_data = %s WHERE report_id = %s", ('Complete', report_csv, report_id))
        connection.commit()

        print(f"Report generated and saved to {report_filename}")

    except Exception as e:
        # Update the report status in the database to 'Failed'
        cursor.execute("UPDATE reports SET status = %s WHERE report_id = %s", ('Failed', report_id))
        connection.commit()
        print(f"Failed to generate report: {e}")

    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()
