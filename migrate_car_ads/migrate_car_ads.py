"""Migration data between schemas."""
import json
import re
import time
import os
import sys
import pymysql


start_time = time.time()
start_time_str = time.strftime("%Y-%m-%d-%H-%M-%S", time.gmtime(start_time))

def make_folder(start_folder, subfolders_chain):
    """Make folder chain."""
    folder = start_folder
    for subfolder in subfolders_chain:
        folder += "/" + subfolder
        if not os.path.isdir(folder):
            os.mkdir(folder)

    return folder


def execute_sql(con, sql_statements, fetch_mode="fetchone"):
    """Execute list of sql statments."""
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
    """Log audit start."""
    process_desc = context["process_desc"]
    sql_statements = [
        f"""
            insert into car_ads_db.process_log(process_desc, user, host) 
            select '{process_desc}', user, host 
            from information_schema.processlist 
            where ID = connection_id();
        """,
        "select last_insert_id() as process_log_id;"
    ]

    return execute_sql(con, sql_statements)


def audit_end(con, context):
    """Log audit end."""
    process_log_id = context["process_log_id"]

    sql_statements = [f"update car_ads_db.process_log set end_date = current_timestamp where process_log_id = {process_log_id};"]

    return execute_sql(con, sql_statements)

def main():
    """Main function."""
    with open("config.json", encoding="utf8") as config_file:
        configs = json.load(config_file)

    connection = pymysql.connect(**configs["car_ads_training"])

    with connection:
        print("Connection established")
        process_log_id = audit_start(connection, {"process_desc": "migrate_car_ads.py"})[0]

        cursor = connection.cursor()
        sql_cmd = """
                select 
                    car_ads_training_db.ads.ads_id,
                    car_ads_training_db.ads.source_id,
                    car_ads_training_db.ads.card_url,
                    car_ads_training_db.ads.ad_group_id,
                    car_ads_training_db.ads.insert_process_log_id,
                    car_ads_training_db.ads.insert_date,
                    car_ads_training_db.ads.change_status_process_log_id,
                    car_ads_training_db.ads.ad_status,
                    car_ads_training_db.ads.change_status_date,
                    car_ads_training_db.ads.card,
                    car_ads_training_db.ad_groups.group_url,
                    car_ads_training_db.ad_groups.process_log_id as process_log_id_g,
                    car_ads_training_db.ad_groups.insert_date as insert_date_g
                    from  car_ads_training_db.ads
                    inner join car_ads_training_db.ad_groups on car_ads_training_db.ads.ad_group_id = car_ads_training_db.ad_groups.ad_group_id
                    left join car_ads_db.ads on car_ads_training_db.ads.card_url = car_ads_db.ads.card_url
                    where (car_ads_training_db.ads.ad_status = 1 or car_ads_training_db.ads.ad_status = 2) and car_ads_db.ads.card_url is null;
                """

        cursor.execute(sql_cmd)

        ads_table = cursor.fetchall()
        print(f"Data from car_ads_training_db gets. Size of data is {sys.getsizeof(ads_table)} bytes")
        cnt_g = 0
        cnt_r = 0
        for row in ads_table:
            file_name = row[1] + row[2] #source_id + card_url
            file_name= file_name.split("?")
            file_name = file_name[0].replace("/", "-").replace(".", "-").replace(":", "-")
            card = row[9]

            url = row[10]
            year = re.search(r"&year_min=(\d+)&", url).group(1)
            price_usd = int(re.search(r"&list_price_min=(\d+)&", url).group(1))

            # save card data in JSON
            try:
                folder = make_folder(configs["folders"]["base_folder"],
                                                [
                                                    configs["folders"]["scrapped_data"],
                                                    "cars_com", "json",
                                                    f"{start_time_str}",
                                                    f"{year}",
                                                    f"price_{price_usd}-{price_usd + 9999}"
                                                ])
                with open(f"{folder}/{file_name}.json", "w", encoding="utf-8") as fp:
                    fp.write(card)
            except OSError as err:
                print("Caught a OSError exception:", err)
                continue

            page_size = re.search(r"&page_size=(\d+)&", url).group(1)
            page_num = re.search(r"&page=(\d+)&", url).group(1)

            # check fields in ad_groups table  
            sql_cmd = f"""
                    select
                        ad_group_id
                    from car_ads_db.ad_groups
                    where price_min = {price_usd}
                        and page_size = {page_size}
                        and year = {year}
                        and page_num = {page_num};
                    """
            cursor.execute(sql_cmd)

            if cursor.rowcount > 0:
                # row with particular fields exist in table ad_groups
                ad_group_id = cursor.fetchone()[0]
            else:
                # ad_group with particular fields des not exist
                # create ad_group record with (price_min, page_size, year, page_num)
                sql_statements  =  [
                    f"""
                    insert car_ads_db.ad_groups(
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
                ad_group_id = execute_sql(connection, sql_statements)[0]
                cnt_g += 1

            sql_cmd = f"""
                    insert car_ads_db.ads (
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
                    "{row[1]}",
                    "{row[2]}",
                    {ad_group_id},
                    {process_log_id},
                    current_timestamp,
                    {row[6]},
                    {row[7]},
                    "{row[8]}"
                )
            """
            cursor.execute(sql_cmd)
            cnt_r += 1

        audit_end(connection, {"process_log_id": process_log_id})
        print("Done")
        print(f"JSON saved and ads inserted {cnt_r}. Groups inserted {cnt_g}. Time elapsed {time.time()-start_time:2.1f} sec")

if __name__ == "__main__":
    main()
