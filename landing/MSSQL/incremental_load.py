"""Incremental load from source DB to destination DB."""

import json
import math
import sys
import time
import pymssql

PROCESS_DESC = "incremental_load.py"

def get_config():
    """Load config data."""
    try:
        with open("./config.json", encoding='utf8') as config_file:
            configs = json.load(config_file)
    except OSError as err:
        print("File config.json not found", err)
        print("Script terminated")
        sys.exit()
    return configs

def clean_destanation_db(cursor, previous_load_time):
    """Clean destination Db from bad batches."""
    stmt = f"""
        delete from [Landing].[dbo].[ads_archive]
        where insert_date >= {previous_load_time};"""
    row_deleted = 0
    try:
        cursor.execute(stmt)
        cursor.execute("select @@ROWCOUNT;")
        row_deleted = cursor.fetchone()[0]
    except  pymssql.Error as err:
        print("Caught a pymssql.Error exception:", err)
    return row_deleted

def extract_data_from_source(connection, batch_size: int, start_ads_id: int, process_start_time, previous_load_time):
    """Extract data from source DB."""
    stmt = f"""
        select
            ads_id,
            source_id,
            card_url,
            card_compressed,
            source_num
        from [Landing].[dbo].[ads_archive]
        where ads_id between {start_ads_id} and {start_ads_id+batch_size-1}
            and modify_date < '{process_start_time}'
            and modify_date >= '{previous_load_time}';"""
    cursor = connection.cursor(as_dict=True)
    data = []
    try:
        cursor.execute(stmt)
        data = cursor.fetchall()
    except  pymssql.Error as err:
        print("Caught a pymssql.Error exception:", err)
    return data

def prepare_data_to_upload(process_log_id, data):
    """Prepare data to load."""
    sql_stmt = r"""insert into [Landing].[dbo].[lnd_ads_archive] (
                     ads_id
                    ,source_id
                    ,card_url
                    ,card_compressed
                    ,process_log_id
                    ) values """
    sql_stmt += ", ".join(f"""( {item['ads_id']}, 
                      '{item['source_id']}', 
                      '{item['card_url']}',  
                      CONVERT(VARBINARY(MAX), '0x'+'{item['card_compressed'].hex()}', 1), 
                      {process_log_id})""" for item in data)
    return sql_stmt

def load_data_to_destination(connection, sql_stmt):
    """Load data to destination DB."""
    
    cursor = connection.cursor()  
    try:
        cursor.execute(sql_stmt)
    except  pymssql.Error as err:
        print("Caught a pymssql.Error exception:", err)

def write_process_log_start(cursor):
    """Write process log."""
    
    process_log_id = None
    cursor.execute(
        f"""
            insert into [Landing].[dbo].[process_log] (process_desc, [user], host, connection_id)         
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
            update [Landing].[dbo].[process_log] 
                set end_date = getdate() 
            where process_log_id = {process_log_id};
        """
    )
    return process_log_id

def write_event_log(cursor, process_log_id, event_desc, status):
    """Write event log."""
    
    cursor.execute(
        f"""
            insert into [Landing].[dbo].[event_log] (event_desc, status, process_log_id)         
            select  '{event_desc}', 
                    '{status}', 
                    '{process_log_id}';
        """
    )

def get_process_start_time(cursor, process_log_id):
    """Get process start time."""
    
    cursor.execute(f"select start_date from [Landing].[dbo].[process_log] where process_log_id = {process_log_id};")
    process_start_time = str(cursor.fetchone()[0])[:-3]
    return process_start_time

def get_previous_load_time(cursor, process_log_id):
    """Get previous load time."""
    try:
        cursor.execute(f"""
                    select max(start_date) 
                    from [Landing].[dbo].[process_log] 
                    where process_log_id < {process_log_id}
                    and (process_desc = 'full_load.py' or process_desc = 'incremental_load.py')
                    and end_date IS NOT NULL;""")
        previous_load_time = cursor.fetchone()
    except  pymssql.Error as err:
        print("Caught a pymssql.Error exception:", err)
        sys.exit()

    if previous_load_time[0]:
        previous_load_time = str(previous_load_time[0])[:-3]
        return previous_load_time
    else:
        print("Information about previous downloads was not found.")
        sys.exit()

def get_max_ads_id(cursor, process_start_time, previous_load_time):
    """Get max ads_id."""
    
    stmt =  f"""select
                    max(ads_id) from [Landing].[dbo].[ads_archive] 
                    where modify_date < '{process_start_time}'
                    and modify_date >= '{previous_load_time}';"""
    try:
        cursor.execute(stmt)
        max_ads_id = cursor.fetchone()
    except  pymssql.Error as err:
        print("Caught a pymssql.Error exception:", err)
        sys.exit()
        
    if max_ads_id[0]:
        return max_ads_id[0]
    else:
        print("Maximum ads_id not get. No data to incremental load")
        sys.exit()

def main():
    """Main fuction."""

    configs = get_config()

    source_db_conx = pymssql.connect(**configs["source_db"], autocommit=True)
    print(f"{time.strftime('%X', time.gmtime())}, Connected to source database")

    dest_db_conx = pymssql.connect(**configs["target_db"], autocommit=True)
    print(f"{time.strftime('%X', time.gmtime())}, Connected to destination database")

    with source_db_conx, dest_db_conx:
        
        source_cur = source_db_conx.cursor()
        dest_cur = dest_db_conx.cursor()
        process_log_id = write_process_log_start(dest_cur)
        previous_load_time = get_previous_load_time(dest_cur, process_log_id)
        process_start_time = get_process_start_time(dest_cur, process_log_id)
        
        print(f"{time.strftime('%X', time.gmtime())}, Cleaning up the database of incorrectly processed batches")
        write_event_log(dest_cur, process_log_id, "Cleaning up the database of incorrectly processed batches", "INFO")
        rows_deleted = clean_destanation_db(dest_cur, previous_load_time)
        print(f"{time.strftime('%X', time.gmtime())}, Rows removed during database cleanup: {rows_deleted}")
        write_event_log(dest_cur, process_log_id, f"Rows removed during database cleanup: {rows_deleted}", "INFO")
        
        write_event_log(dest_cur, process_log_id, f"Timestamp start of incremental upload:{process_start_time}", "START")
        
        print(f"{time.strftime('%X', time.gmtime())}, Getting information to calculate the number of batches")
        write_event_log(dest_cur, process_log_id, "Getting information to calculate the number of batches", "INFO")
        max_ads_id = get_max_ads_id(source_cur, process_start_time, previous_load_time)
        
        batch_size = configs["batch_size"]
        number_batches = math.ceil(max_ads_id / batch_size)
        print(f"{time.strftime('%X', time.gmtime())}, For a incremental load, it will be necessary to process {number_batches} batches")
        write_event_log(dest_cur, process_log_id, f"For a incremental load, it will be necessary to process {number_batches} batches", "INFO")

        for batch_num, start_ads_id in enumerate(range(1, max_ads_id + 1, batch_size), start=1):

            print(f"{time.strftime('%X', time.gmtime())}, Extracting from source batch #{batch_num}/{number_batches}")
            write_event_log(dest_cur, process_log_id, f"Extracting from source batch #{batch_num}/{number_batches}", "INFO")
            cards = extract_data_from_source(source_db_conx,
                                               batch_size,
                                               start_ads_id,
                                               process_start_time,
                                               previous_load_time)
           
            if cards:
                print(f"{time.strftime('%X', time.gmtime())}, Prepare data for loading to destination batch #{batch_num}/{number_batches}")
                write_event_log(dest_cur, process_log_id, f"Prepare data for loading to destination batch #{batch_num}/{number_batches}", "INFO")
                stmt = prepare_data_to_upload(process_log_id, cards)
                
                print(f"{time.strftime('%X', time.gmtime())}, Loading to destination batch #{batch_num}/{number_batches}")
                write_event_log(dest_cur, process_log_id, f"Loading to destination batch #{batch_num}/{number_batches}", "INFO")
                load_data_to_destination(dest_db_conx, stmt)
            else:
                print(f"{time.strftime('%X', time.gmtime())}, Batch #{batch_num}/{number_batches} is empty, go on to the next one")
                write_event_log(dest_cur, process_log_id, f"Batch #{batch_num}/{number_batches} is empty, go on to the next one", "INFO")


        write_process_log_end(dest_cur, process_log_id)

if __name__ == "__main__":
    main()
