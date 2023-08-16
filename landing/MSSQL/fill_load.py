"""Fil load from source DB to Target DB."""

import json
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
    return configs


def download_cards_from_source(connection, batch_size: int, ads_id: int, process_start_time):
    """Get cards from source DB."""
    stmt = f"""select
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

def upload_cards_to_destanation(connection, process_log_id, cards):
    """Upload cards to target DB."""
    stmt = r"""insert into [Landing].[dbo].[lnd_ads_archive] (
                     ads_id
                    ,source_id
                    ,card_url
                    ,card_compressed
                    ,process_log_id
                    ) values """
    stmt += ", ".join(f"""( {card['ads_id']}, 
                      '{card['source_id']}', 
                      '{card['card_url']}',  
                      CONVERT(VARBINARY(MAX), '0x'+'{card['card_compressed'].hex()}', 1), 
                      {process_log_id})""" for card in cards)
    cursor = connection.cursor()
    
    try:
        cursor.execute(stmt)
    except  pymssql.Error as err:
        print("Caught a pymssql.Error exception:", err)
    
def write_process_log(cursor, action):
    """Write process log."""
    
    process_log_id = None
    if action == "START":
        cursor.execute(f"""
                    insert into [Landing].[dbo].[process_log] (process_desc, [user], host, connection_id)         
                    select '{PROCESS_DESC}', 
                            SYSTEM_USER, 
                            HOST_NAME(),
                            @@spid;
                """)
        cursor.execute("select scope_identity() as process_log_id;")
        process_log_id = cursor.fetchone()[0]
    elif action == "END":
        cursor.execute(
            f"""
                update process_log 
                    set end_date = getdate() 
                where process_log_id = {process_log_id};
            """
        )
    return process_log_id

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

    target_db_conx = pymssql.connect(**configs["target_db"], autocommit=True)
    print(f"{time.strftime('%X', time.gmtime())}, Connected to target database")

    with source_db_conx, target_db_conx:
        
        source_cur = source_db_conx.cursor()
        target_cur = target_db_conx.cursor()

        process_log_id = write_process_log(target_cur, "START")
        
        # ! time from source may be difference from target
        process_start_time = get_process_start_time(target_cur, process_log_id)
        
        max_ads_id = get_max_ads_id(source_cur, process_start_time)
        
        print(f"{time.strftime('%X', time.gmtime())}, Get max ads_id from source database")

        for batch_num, ads_id in enumerate(range(1, max_ads_id + 1, configs["batch_size"])):

            cards = download_cards_from_source(source_db_conx,
                                               configs["batch_size"],
                                               ads_id,
                                               process_start_time)

            print(f"{time.strftime('%X', time.gmtime())}. Download cards. Batch #{batch_num}.")

            upload_cards_to_destanation(target_db_conx, process_log_id, cards)
            print(f"{time.strftime('%X', time.gmtime())}. Upload cards to destanation. Batch #{batch_num}.")

        write_process_log(target_cur, "END")

if __name__ == "__main__":
    main()
