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
    cursor = connection.cursor() # as_dict=True
    cards = []
    try:
        cursor.execute(stmt)
        cards = cursor.fetchall()
    except  pymssql.Error as err:
        print("Caught a pymssql.Error exception:", err)
    return cards

def upload_cards_to_destanation(connection, cards):
    """Upload cards to target DB."""
    stmt = r"""insert into [Landing].[dbo].[lnd_ads_archive] (
                     ads_id
                    ,source_id
                    ,card_url
                    ,card_compressed
                    ) values """
    stmt += ", ".join(fr"{card}" for card in cards)
    cursor = connection.cursor()
    
    try:
        cursor.execute(stmt)
    except  pymssql.Error as err:
        print("Caught a pymssql.Error exception:", err)
    

def main():
    """Main fiction."""

    configs = get_config()

    source_db_conx = pymssql.connect(**configs["source_db"], autocommit=True)
    print(f"{time.strftime('%X', time.gmtime())}, Connected to source database")

    target_db_conx = pymssql.connect(**configs["target_db"], autocommit=True)
    print(f"{time.strftime('%X', time.gmtime())}, Connected to target database")

    with source_db_conx, target_db_conx:
        source_cur = source_db_conx.cursor()
        target_cur = target_db_conx.cursor()

        target_cur.execute(f"""
                insert into [Landing].[dbo].[process_log] (process_desc, [user], host, connection_id)         
                select '{PROCESS_DESC}', 
                       SYSTEM_USER, 
                       HOST_NAME(),
                       @@spid;
            """)

        target_cur.execute("select scope_identity() as process_log_id;")
        process_log_id = target_cur.fetchone()[0]

        source_cur.execute("select getdate()")
        process_start_time = str(source_cur.fetchone()[0])[:-3]

        stmt =  f"""select
                    max(ads_id) from [car_ads_training_db].[dbo].[ads_archive] 
                    where ad_status=2
                    and modify_date < '{process_start_time}';"""
        source_cur.execute(stmt)
        print(f"{time.strftime('%X', time.gmtime())}, Get max ads_id from source database")

        max_ads_id = source_cur.fetchone()[0]

        for batch_num, ads_id in enumerate(range(1, max_ads_id + 1, configs["batch_size"])):

            cards = download_cards_from_source(source_db_conx,
                                               configs["batch_size"],
                                               ads_id,
                                               process_start_time)

            print(f"{time.strftime('%X', time.gmtime())}. Download cards. Batch #{batch_num}.")

            upload_cards_to_destanation(target_db_conx, cards)
            print(f"{time.strftime('%X', time.gmtime())}. Upload cards to destanation. Batch #{batch_num}.")

        target_cur.execute(
            f"""
                update process_log 
                    set end_date = getdate() 
                where process_log_id = {process_log_id};
            """
        )

if __name__ == "__main__":
    main()
