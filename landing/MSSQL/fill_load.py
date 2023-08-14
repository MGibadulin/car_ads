import pymssql
import json
import time
import zlib


def get_config():
    """Load config data."""
    try:
        with open("./config.json", encoding='utf8') as config_file:
            configs = json.load(config_file)
    except OSError as err:
        print("File config.json not found", err)
        print("Script terminated")
    return configs


def download_cards_from_source(connection, batch_size: int):
    """Get cards from source."""
    stmt = f"""select top {batch_size} a.ads_id
                from dbo.ads as a
                left join dbo.tokenized_card as t
                on a.ads_id = t.ads_id
                where t.ads_id is null
                and a.ad_status = 2
                and source_id = 'https://www.cars.com'"""
    cursor = connection.cursor()
    rows = []
    try:
        cursor.execute(stmt)
        rows = cursor.fetchall()
    except  pymssql.Error as err:
        print("Caught a pymssql.Error exception:", err)
    return rows

def upload_cards_to_destanation():
    pass

def main():

    configs = get_config()
    connection = pymssql.connect(**configs["mssql_db"], autocommit=True)
    print(f"{time.strftime('%X', time.gmtime())}. Connected to database")

    with connection:
        num_butch = 1
        cards = download_cards_from_source(connection, configs["batch_size"])
        print(f"{time.strftime('%X', time.gmtime())}. Download cards. Batch #{num_butch}.")

        while cards:
            cards = []
            print(f"{time.strftime('%X', time.gmtime())}. Transform cards. Batch #{num_butch}.")
            for card in cards:
                pass

            upload_cards_to_destanation(cards)
            print(f"{time.strftime('%X', time.gmtime())}. Upload cards to destanation. Batch #{num_butch}.")
            
            num_butch += 1
            cards = download_cards_from_source(connection, configs["batch_size"])
            print(f"{time.strftime('%X', time.gmtime())}. Download cards. Batch #{num_butch}.")

if __name__ == "__main__":
    main()