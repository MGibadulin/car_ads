import pymssql
import json
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

   
def get_not_tokenized_cards(connection, batch_size: int):
    """Get cards not yet tokenized."""
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

def tokenize_card():
    return ""
    
def upload_tokenized_cards_to_db():
    pass    
    
def main():

    configs = get_config()
    connection = pymssql.connect(**configs["mssql_db"], autocommit=True)

    with connection:

        cards = get_not_tokenized_cards(connection, configs["batch_size"])
        
        while cards:
            tokenized_cards = []
            for card in cards:
                tokenized_card = tokenize_card(card)
                tokenized_cards.append(tokenized_card)
            
            upload_tokenized_cards_to_db(tokenized_cards)
            
            cards = get_not_tokenized_cards(connection, configs["batch_size"])
        
        # cursor = connection.cursor()
        # stmt = 'SELECT top 10 ads_id, card_compressed from dbo.ads;'
        # cursor.execute(stmt)  
        # row = cursor.fetchone()  
        # while row:
        #     compressed_data = row[1]
        #     if compressed_data:
        #         plain_string = zlib.decompress(compressed_data, zlib.MAX_WBITS+16).decode(encoding='utf-8')
        #         print(f"{str(row[0])} {plain_string[:300]}")     
        #     row = cursor.fetchone()

    
if __name__ == "__main__":
    main()
