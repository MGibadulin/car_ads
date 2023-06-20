import json
import re
import time
import pymysql
import os

start_time = time.time()
start_time_str = time.strftime("%Y-%m-%d-%H-%M-%S", time.gmtime(start_time))

def make_folder(start_folder, subfolders_chain):
    folder = start_folder
    for subfolder in subfolders_chain:
        folder += "/" + subfolder
        if not os.path.isdir(folder):
            os.mkdir(folder)

    return folder


def execute_sql(con, sql_statements, fetch_mode="fetchone"):
    cur = con.cursor()
    for sql in sql_statements:
        cur.execute(sql)

    res = None
    if cur.rowcount > 0:
        if fetch_mode == "fetchone":
            res = cur.fetchone()
        else:
            res = cur.fetchall()

    return res


def audit_start(con, context):
    process_desc = context["process_desc"]
    sql_statements = [
        f"""
            insert into process_log(process_desc, user, host) 
            select '{process_desc}', user, host 
            from information_schema.processlist 
            where ID = connection_id();
        """,
        "select last_insert_id() as process_log_id;"
    ]

    return execute_sql(con, sql_statements)


def audit_end(con, context):
    process_log_id = context["process_log_id"]

    sql_statements = [f"update process_log set end_date = current_timestamp where process_log_id = {process_log_id};"]

    return execute_sql(con, sql_statements)

def main():
    with open("config.json") as config_file:
        configs = json.load(config_file)
        
    conn_training_db = pymysql.connect(**configs["car_ads_training"])
    conn_ads_db= pymysql.connect(**configs["car_ads_db"])
    
    with conn_training_db, conn_ads_db:
        
        process_log_id = audit_start(conn_ads_db, {"process_desc": "migrate_car_ads.py"})[0]
        
        cur_training_db = conn_training_db.cursor()
        sql_cmd = """
                select 
                    ads.ads_id,
                    ads.source_id,
                    ads.card_url,
                    ads.ad_group_id,
                    ads.insert_process_log_id,
                    ads.insert_date,
                    ads.change_status_process_log_id,
                    ads.ad_status,
                    ads.change_status_date,
                    ads.card,
                    ad_groups.group_url,
                    ad_groups.process_log_id as process_log_id_g,
                    ad_groups.insert_date as insert_date_g
                    from  ads
                    inner join ad_groups on ads.ad_group_id = ad_groups.ad_group_id
                    where ad_status = 1 or ad_status = 2;
                """
                
        cur_training_db.execute(sql_cmd)
        
        ads_table = cur_training_db.fetchall()
        
        for row in ads_table:
            file_name = row[1] + row[2] #source_id + card_url
            file_name= file_name.split("?")
            file_name = file_name[0].replace("/", "-").replace(".", "-").replace(":", "-")
            card = row[9]
            
            url = row[10]
            year = re.search(r"&year_min=(\d+)&", url).group(1)
            price_usd = re.search(r"&list_price_min=(\d+)&", url).group(1)
            
            try:
                folder = make_folder(configs["folders"]["base_folder"],
                                                [
                                                    configs["folders"]["scrapped_data"],
                                                    "cars_com", "json",
                                                    f"{start_time_str}",
                                                    f"{year}",
                                                    f"price_{price_usd}-{price_usd + 9999}"
                                                ])
                with open(f"{folder}/{file_name}.json", "w", encoding="utf-8") as f:
                    f.write(card)
            except OSError as e:
                print("Caught a OSError exception:", e)
                continue
            
            page_size = re.search(r"&page_size=(\d+)&", url).group(1)
            page_num = re.search(r"&page=(\d+)&", url).group(1)
            
            cur_ads_db = conn_ads_db.cursor()
            sql_cmd = f"""
                    select
                        ad_group_id
                    from ad_groups
                    where price_min = {price_usd}
                        and page_size = {page_size}
                        and year = {year}
                        and page_num = {page_num};
                    """
            cur_ads_db.execute(sql_cmd)
            
            if cur_ads_db.rowcount > 0:
                # ad_group with particular fields exist
                ad_group_id = cur_ads_db.fetchone()[0]
            else:
                # ad_group with particular fields des not exist
                # create ad_group record with (price_min, page_size, year, page_num)
                sql_statements  =  [
                    f"""
                    insert ad_groups(
                        price_min,
                        page_size,
                        year,
                        page_num,    
                        process_log_id,
                        insert_date
                    )
                    values(
                        {price_usd},
                        {page_size},
                        {year},
                        {page_num},
                        {process_log_id},
                        current_timestamp
                    );
                """,
                "select last_insert_id() as process_log_id;"
                ]
                ad_group_id = execute_sql(conn_ads_db, sql_statements)[0]
                
                
            sql_cmd = f"""
                    insert car_ads_db (
                        source_id,
                        card_url,
                        ad_group_id,
                        insert_process_log_id,
                        insert_date,
                        change_status_process_log_id,
                        ad_status,
                        change_status_date
                )
                values(
                    
                )
                    """
            # try save json on disk
            # check is there (year, page_size, page_num, price_min) in car_ads_db.ad_groups
            # if is not, then insert
            # insert row in  car_ads_db
            # save json       
        
        
if __name__ == "__main__":
    main()