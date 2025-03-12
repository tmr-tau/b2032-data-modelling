import psycopg2
import pandas as pd
import os
import json
from datetime import datetime

# Database connection parameters
DB_PARAMS = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

def get_db_connection():
    return psycopg2.connect(**DB_PARAMS)

def load_data(query):
    with get_db_connection() as conn:
        return pd.read_sql(query, conn)

def execute_query(query, params=None):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            conn.commit()

def log_event_change(event_session_id, schedule_version_id, change_type, changed_fields, previous_values):
    query = """
    INSERT INTO event_audit (
        event_session_id, schedule_version_id, change_type, changed_fields, previous_values, change_timestamp
    ) VALUES (%s, %s, %s, %s, %s, %s)
    """
    execute_query(query, (
        event_session_id, schedule_version_id, change_type, 
        json.dumps(changed_fields), json.dumps(previous_values), datetime.now()
    ))

def get_changed_columns(old_row, new_row):
    changed_fields = {}
    previous_values = {}
    for column in old_row.index:
        if old_row[column] != new_row[column]:
            changed_fields[column] = new_row[column]
            previous_values[column] = old_row[column]
    return changed_fields, previous_values

def process_staging_data(new_version_number):
    # Step 1: Create new schedule version
    query = """
    INSERT INTO dim_schedule_version (version_number, valid_from, valid_to)
    VALUES (%s, %s, %s) RETURNING schedule_version_id
    """
    schedule_version_id = None
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (new_version_number, datetime.now(), '9999-12-31 23:59:59'))
            schedule_version_id = cursor.fetchone()[0]
    
    # Step 2: Load staging data
    staging_data_query = "SELECT * FROM staging_event_session"
    staging_df = load_data(staging_data_query)
    
    # Step 3: Load current data
    current_data_query = "SELECT * FROM fct_event_session WHERE is_current = TRUE"
    current_df = load_data(current_data_query)
    
    # Step 4: Identify inserts, updates, and deletes
    staging_df['hash_key'] = staging_df.apply(
        lambda row: pd.util.hash_pandas_object(row[['venue_id', 'sport_id', 'day_id', 'event_date', 'event_type', 'start_time', 'date_start']]).sum(), axis=1
    )
    current_df['hash_key'] = current_df.apply(
        lambda row: pd.util.hash_pandas_object(row[['venue_id', 'sport_id', 'day_id', 'event_date', 'event_type', 'start_time', 'date_start']]).sum(), axis=1
    )
    
    # Identify inserts and updates
    merged_df = staging_df.merge(current_df, on='hash_key', how='left', suffixes=('_new', '_old'))
    inserts_df = merged_df[merged_df['event_session_id_old'].isna()]
    updates_df = merged_df[~merged_df['event_session_id_old'].isna()]
    
    # Identify deletes
    deletes_df = current_df[~current_df['hash_key'].isin(staging_df['hash_key'])]
    
    # Step 5: Process inserts
    for index, row in inserts_df.iterrows():
        insert_query = """
        INSERT INTO fct_event_session (
            hash_key, schedule_version_id, current_version_id, version_array, sport_id, venue_id, day_id, 
            event_date, start_time, end_time, date_start, date_end, event_type, 
            gross_seats, seat_kill, est_ticket_sold, net_seats, est_sold_seats, workforce_count, 
            valid_from, valid_to, is_current, additional_attributes
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            row['hash_key'], schedule_version_id, schedule_version_id, [schedule_version_id], row['sport_id_new'], row['venue_id_new'], row['day_id_new'],
            row['event_date_new'], row['start_time_new'], row['end_time_new'], row['date_start_new'], row['date_end_new'], row['event_type_new'],
            row['gross_seats_new'], row['seat_kill_new'], row['est_ticket_sold_new'], row['net_seats_new'], row['est_sold_seats_new'], row['workforce_count_new'],
            datetime.now(), '9999-12-31 23:59:59', True, row['additional_attributes_new']
        )
        execute_query(insert_query, params)
        log_event_change(None, schedule_version_id, 'INSERT', row.to_dict(), None)
    
    # Step 6: Process updates
    for index, row in updates_df.iterrows():
        old_row = row.filter(like='_old')
        new_row = row.filter(like='_new')
        changed_fields, previous_values = get_changed_columns(old_row, new_row)
        
        if changed_fields:
            update_query = """
            UPDATE fct_event_session
            SET is_current = FALSE, valid_to = %s
            WHERE event_session_id = %s
            """
            execute_query(update_query, (datetime.now(), row['event_session_id_old']))
            log_event_change(row['event_session_id_old'], row['schedule_version_id_old'], 'UPDATE', changed_fields, previous_values)
            
            insert_query = """
            INSERT INTO fct_event_session (
                hash_key, schedule_version_id, current_version_id, version_array, sport_id, venue_id, day_id, 
                event_date, start_time, end_time, date_start, date_end, event_type, 
                gross_seats, seat_kill, est_ticket_sold, net_seats, est_sold_seats, workforce_count, 
                valid_from, valid_to, is_current, additional_attributes
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = (
                row['hash_key'], schedule_version_id, schedule_version_id, row['version_array_old'] + [schedule_version_id], row['sport_id_new'], row['venue_id_new'], row['day_id_new'],
                row['event_date_new'], row['start_time_new'], row['end_time_new'], row['date_start_new'], row['date_end_new'], row['event_type_new'],
                row['gross_seats_new'], row['seat_kill_new'], row['est_ticket_sold_new'], row['net_seats_new'], row['est_sold_seats_new'], row['workforce_count_new'],
                datetime.now(), '9999-12-31 23:59:59', True, row['additional_attributes_new']
            )
            execute_query(insert_query, params)
    
    # Step 7: Process deletes
    for index, row in deletes_df.iterrows():
        old_values = {
            'sport_id': row['sport_id'], 'venue_id': row['venue_id'], 'day_id': row['day_id'],
            'event_date': row['event_date'], 'start_time': row['start_time'], 'end_time': row['end_time'],
            'date_start': row['date_start'], 'date_end': row['date_end'], 'event_type': row['event_type'],
            'gross_seats': row['gross_seats'], 'seat_kill': row['seat_kill'], 'est_ticket_sold': row['est_ticket_sold'],
            'net_seats': row['net_seats'], 'est_sold_seats': row['est_sold_seats'], 'workforce_count': row['workforce_count'],
            'additional_attributes': row['additional_attributes']
        }
        
        update_query = """
        UPDATE fct_event_session
        SET is_current = FALSE, valid_to = %s, current_version_id = %s, version_array = array_append(version_array, %s)
        WHERE event_session_id = %s
        """
        execute_query(update_query, (datetime.now(), schedule_version_id, schedule_version_id, row['event_session_id']))
        log_event_change(row['event_session_id'], row['schedule_version_id'], 'DELETE', None, old_values)

if __name__ == "__main__":
    process_staging_data('v1')
