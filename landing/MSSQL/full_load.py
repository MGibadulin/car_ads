"""Full load from source DB to Target DB."""

import json
import math
import time
import pymssql


PROCESS_DESC = "full_load.py"

def get_config():
    """Load config data."""
    try:
        with open("./config.json", encoding='utf8') as config_file:
            configs = json.load(config_file)
    except OSError as err:
        print("File config.json not found", err)
        print("Script terminated")
        exit()
    return configs

def download_cards_from_source(connection, batch_size: int, ads_id: int, process_start_time):
    """Get cards from source DB."""
    stmt = f"""
        select
            ads_id,
            source_id,
            card_url,
            card_compressed,
            source_num
        from car_ads_training_db.dbo.ads_archive
        where ad_status = 2
            and ads_id between {ads_id} and {ads_id+batch_size-1}
            and modify_date < '{process_start_time}';"""
    cursor = connection.cursor(as_dict=True)
    cards = []
    try:
        cursor.execute(stmt)
        cards = cursor.fetchall()
    except  pymssql.Error as err:
        print("Caught a pymssql.Error exception:", err)
    return cards

def prepare_data_to_upload(process_log_id, cards):
    """Prepare data to upload."""
    sql_stmt = r"""insert into [Landing].[dbo].[lnd_ads_archive] (
                     ads_id
                    ,source_id
                    ,card_url
                    ,card_compressed
                    ,process_log_id
                    ) values """
    sql_stmt += ", ".join(f"""( {card['ads_id']}, 
                      '{card['source_id']}', 
                      '{card['card_url']}',  
                      CONVERT(VARBINARY(MAX), '0x'+'{card['card_compressed'].hex()}', 1), 
                      {process_log_id})""" for card in cards)
    return sql_stmt

def upload_cards_to_destanation(connection, sql_stmt):
    """Upload cards to target DB."""
    
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
    
    cursor.execute(f"select start_date from [Landing].[dbo].[process_log] where process_log_id = {process_log_id}")
    process_start_time = str(cursor.fetchone()[0])[:-3]
    return process_start_time

def get_max_ads_id(cursor, process_start_time):
    """Get max ads_id."""
    
    stmt =  f"""select
                    max(ads_id) from [car_ads_training_db].[dbo].[ads_archive] 
                    where ad_status=2
                    and modify_date < '{process_start_time}';"""
    cursor.execute(stmt)
    max_ads_id = cursor.fetchone()[0]
    return max_ads_id

def main():
    """Main fuction."""

    configs = get_config()

    source_db_conx = pymssql.connect(**configs["source_db"], autocommit=True)
    print(f"{time.strftime('%X', time.gmtime())}, Connected to source database")

    dest_db_conx = pymssql.connect(**configs["target_db"], autocommit=True)
    print(f"{time.strftime('%X', time.gmtime())}, Connected to destanation database")

    with source_db_conx, dest_db_conx:
        
        source_cur = source_db_conx.cursor()
        dest_cur = dest_db_conx.cursor()

        process_log_id = write_process_log_start(dest_cur)
        
        process_start_time = get_process_start_time(dest_cur, process_log_id)
        write_event_log(dest_cur, process_log_id, f"Timestamp start of full upload:{process_start_time}", "START")
        
        print(f"{time.strftime('%X', time.gmtime())}, Getting information to calculate the number of batches")
        write_event_log(dest_cur, process_log_id, "Getting information to calculate the number of batches", "INFO")
        max_ads_id = get_max_ads_id(source_cur, process_start_time)
        
        batch_size = configs["batch_size"]
        number_batches = math.ceil(max_ads_id / batch_size)
        print(f"{time.strftime('%X', time.gmtime())}, For a full load, it will be necessary to process {number_batches} batches")
        write_event_log(dest_cur, process_log_id, f"For a full load, it will be necessary to process {number_batches} batches", "INFO")

        for batch_num, ads_id in enumerate(range(1, max_ads_id + 1, batch_size), start=1):

            print(f"{time.strftime('%X', time.gmtime())}, Downloading from source batch #{batch_num}/{number_batches}")
            write_event_log(dest_cur, process_log_id, f"Downloading from source batch #{batch_num}/{number_batches}", "INFO")
            cards = download_cards_from_source(source_db_conx,
                                               batch_size,
                                               ads_id,
                                               process_start_time)
           
            print(f"{time.strftime('%X', time.gmtime())}, Prepare data for uploading to destanation batch #{batch_num}/{number_batches}")
            write_event_log(dest_cur, process_log_id, f"Prepare data for uploading to destanation batch #{batch_num}/{number_batches}", "INFO")
            stmt = prepare_data_to_upload(process_log_id, cards)
            
            print(f"{time.strftime('%X', time.gmtime())}, Uploading to destanation batch #{batch_num}/{number_batches}")
            write_event_log(dest_cur, process_log_id, f"Uploading to destanation batch #{batch_num}/{number_batches}", "INFO")
            upload_cards_to_destanation(dest_db_conx, stmt)

        write_process_log_end(dest_cur, process_log_id)

if __name__ == "__main__":
    main()
