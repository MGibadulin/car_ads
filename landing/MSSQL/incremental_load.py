"""Incremental load from source DB to destination DB."""

import json
import math
from pathlib import Path
import sys
import time
import pymssql

PROCESS_DESC =      "mssql_incremental_load.py"
SOURCE_DB =         "[car_ads_training_db].[dbo].[ads_archive]"
DEST_DB =           "[Landing].[dbo].[lnd_ads_archive]"
PROCESS_LOG_DB =    "[Landing].[dbo].[process_log]"
EVENT_LOG_DB =      "[Landing].[dbo].[event_log]"

def get_config():
    """Load config data."""
    
    file_path = Path(__file__).resolve().parents[0]

    try:
        with open(file_path.joinpath("config.json"), encoding='utf8') as config_file:
            configs = json.load(config_file)
    except OSError as err:
        print("File config.json not found", err)
        print("Script terminated")
        sys.exit()
    return configs

def extract_data_from_source(cursor, previous_load_time):
    """Extract data from source DB."""
    stmt = f"""
        select
            select
            ads_id,
            source_id,
            card_url,
            card_compressed,
            modify_date
        from {SOURCE_DB}
        where modify_date > '{previous_load_time}';"""
        
    try:
        cursor.execute(stmt)
    except  pymssql.Error as err:
        print("Caught a pymssql.Error exception:", err)
        print("Script terminated")
        sys.exit()

def prepare_data_to_load(process_log_id, data):
    """Prepare data to load."""
    
    sql_stmt = fr"""insert into {DEST_DB} (
                     ads_id
                    ,source_id
                    ,card_url
                    ,card_compressed
                    ,source_date
                    ,process_log_id
                    ) values """
    data_compressed = [item for item in data if item['card_compressed'] is not None]
    sql_stmt += ", ".join(f"""( {item['ads_id']}, 
                      '{item['source_id']}', 
                      '{item['card_url']}',  
                      CONVERT(VARBINARY(MAX), '0x'+'{item['card_compressed'].hex()}', 1),
                      '{str(item['modify_date'])[:-3]}',
                      {process_log_id})""" for item in data_compressed)
    if data_compressed:
        sql_stmt += ", "
        
    data_null = [item for item in data if item['card_compressed'] is None]
    sql_stmt += ", ".join(f"""( {item['ads_id']}, 
                      '{item['source_id']}', 
                      '{item['card_url']}',  
                      NULL,
                      '{str(item['modify_date'])[:-3]}', 
                      {process_log_id})""" for item in data_null)
    return sql_stmt

def load_data_to_destination(cursor, sql_stmt):
    """Load data to destination DB."""
    
    try:
        cursor.execute(sql_stmt)
    except  pymssql.Error as err:
        print("Caught a pymssql.Error exception:", err)
        print("Script terminated")
        sys.exit()

def write_process_log_start(cursor):
    """Write process log."""
    
    process_log_id = None
    cursor.execute(
        f"""
            insert into {PROCESS_LOG_DB} (process_desc, [user], host, connection_id)         
            select '{PROCESS_DESC}', 
                    SYSTEM_USER, 
                    HOST_NAME(),
                    @@spid;
        """
    )
    cursor.execute("select scope_identity() as process_log_id;")
    process_log_id = cursor.fetchone()[0]
    return process_log_id

def write_process_log_end(cursor, process_log_id):
    cursor.execute(
        f"""
            update {PROCESS_LOG_DB} 
                set end_date = getdate() 
            where process_log_id = {process_log_id};
        """
    )
    return process_log_id

def write_event_log(cursor, process_log_id, event_desc, status):
    """Write event log."""
    
    cursor.execute(
        f"""
            insert into {EVENT_LOG_DB} (event_desc, status, process_log_id)         
            select  '{event_desc}', 
                    '{status}', 
                    '{process_log_id}';
        """
    )

def get_process_start_time(cursor, process_log_id):
    """Get process start time."""
    
    cursor.execute(f"select start_date from {PROCESS_LOG_DB} where process_log_id = {process_log_id};")
    process_start_time = str(cursor.fetchone()[0])[:-3]
    return process_start_time

def get_previous_load_time(cursor):
    """Get previous load time."""
    try:
        cursor.execute(f"""
                    select max(source_date)
                    from {DEST_DB};""")
        previous_load_time = cursor.fetchone()
    except  pymssql.Error as err:
        print("Caught a pymssql.Error exception:", err)
        print("Script terminated")
        sys.exit()

    if previous_load_time[0]:
        previous_load_time = str(previous_load_time[0])[:-3]
        return previous_load_time
    else:
        print("Information about previous downloads was not found.")
        print("Script terminated")
        sys.exit()

def get_cnt_extract_rows(cursor, previous_load_time):
    """Get max ads_id."""
    
    stmt =  f"""select count(*) as cnt from {SOURCE_DB} 
                    where modify_date > '{previous_load_time}';"""
    try:
        cursor.execute(stmt)
        cnt_extract_rows = cursor.fetchone()
    except  pymssql.Error as err:
        print("Caught a pymssql.Error exception:", err)
        print("Script terminated")
        sys.exit()
        
    if cnt_extract_rows['cnt']:
        return cnt_extract_rows['cnt']
    else:
        print("The number of new rows in the table is not received. No data to incremental load")
        print("Script terminated")
        sys.exit()

def main():
    """Main fuction."""

    configs = get_config()

    source_db_conx = pymssql.connect(**configs["source_db"], autocommit=True)
    print(f"{time.strftime('%X', time.gmtime())}, Connected to source database")

    dest_db_conx = pymssql.connect(**configs["dest_db"], autocommit=True)
    print(f"{time.strftime('%X', time.gmtime())}, Connected to destination database")

    with source_db_conx, dest_db_conx:
        
        source_cur = source_db_conx.cursor(as_dict=True)
        dest_cur = dest_db_conx.cursor()
        process_log_id = write_process_log_start(dest_cur)
        previous_load_time = get_previous_load_time(dest_cur)
        process_start_time = get_process_start_time(dest_cur, process_log_id)
        
        write_event_log(dest_cur, process_log_id, f"Timestamp start of incremental upload:{process_start_time}", "START")
        
        print(f"{time.strftime('%X', time.gmtime())}, Getting information to calculate the number of batches")
        write_event_log(dest_cur, process_log_id, "Getting information to calculate the number of batches", "INFO")
        cnt_extract_rows = get_cnt_extract_rows(source_cur, previous_load_time)
        
        batch_size = configs["batch_size"]
        number_batches = math.ceil(cnt_extract_rows / batch_size)
        print(f"{time.strftime('%X', time.gmtime())}, For a incremental load, it will be necessary to process {number_batches} batches")
        write_event_log(dest_cur, process_log_id, f"For a incremental load, it will be necessary to process {number_batches} batches", "INFO")

        for batch_num in range(1, number_batches + 1):

            print(f"{time.strftime('%X', time.gmtime())}, Extracting from source batch #{batch_num}/{number_batches}")
            write_event_log(dest_cur, process_log_id, f"Extracting from source batch #{batch_num}/{number_batches}", "INFO")
            cards = source_cur.fetchmany(batch_size)
           
            print(f"{time.strftime('%X', time.gmtime())}, Prepare data for loading to destination batch #{batch_num}/{number_batches}")
            write_event_log(dest_cur, process_log_id, f"Prepare data for loading to destination batch #{batch_num}/{number_batches}", "INFO")
            stmt = prepare_data_to_load(process_log_id, cards)
            
            print(f"{time.strftime('%X', time.gmtime())}, Loading to destination batch #{batch_num}/{number_batches}")
            write_event_log(dest_cur, process_log_id, f"Loading to destination batch #{batch_num}/{number_batches}", "INFO")
            load_data_to_destination(dest_cur, stmt)
            

        write_process_log_end(dest_cur, process_log_id)

if __name__ == "__main__":
    main()
