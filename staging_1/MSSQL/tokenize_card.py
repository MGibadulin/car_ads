import pymssql
import json
import zlib


def get_config():
    with open("./config.json", encoding='utf8') as config_file:
        configs = json.load(config_file)
    return configs

def main():

    configs = get_config()
    connection = pymssql.connect(**configs["mssql_db"], autocommit=True)

    with connection:
        
        cursor = connection.cursor()
        stmt = 'SELECT top 10 ads_id, card_compressed from dbo.ads;'
        cursor.execute(stmt)  
        row = cursor.fetchone()  
        while row:
            compressed_data = row[1]
            if compressed_data:
                plain_string = zlib.decompress(compressed_data, zlib.MAX_WBITS+16).decode(encoding='utf-8')
                print(f"{str(row[0])} {plain_string[:300]}")     
            row = cursor.fetchone()

    
if __name__ == "__main__":
    main()
