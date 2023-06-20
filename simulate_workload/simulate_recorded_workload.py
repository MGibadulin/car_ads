import argparse
from datetime import datetime
import json
import pymysql


DEFAULT_THREAD_ID = 16

STORAGE_CMD_DB = 0
STORAGE_CMD_LOCAL = 1

EXEC_INIT_SCRIPT = 1
NOT_EXEC_INIT_SCRIPT = 0

LEVEL_MSG_SHOW_CONSOLE_0 = 0  # Do not show message in console
LEVEL_MSG_SHOW_CONSOLE_1 = 1  # reserved
LEVEL_MSG_SHOW_CONSOLE_2 = 2  # Show message with time and counter
LEVEL_MSG_SHOW_CONSOLE_3 = 3  # Show message with time, counter and SQL command

def get_args() -> dict:
    """Get args command line"""

    parser = argparse.ArgumentParser(
        description="The script simulate workload on MySQL by reading slow_log"
    )

    parser.add_argument(
        "-thread_id", type=int, default=DEFAULT_THREAD_ID, help="Thread_id number"
    )

    parser.add_argument(
        "-storage_cmd",
        type=int,
        default=STORAGE_CMD_DB,
        help="Get commands from local storage or from database",
    )
    parser.add_argument(
        "-exec_init_script",
        type=int,
        default=NOT_EXEC_INIT_SCRIPT,
        help="Execute initialaze script database before simulate recorded workload",
    )
    parser.add_argument(
        "-level_show_console",
        type=int,
        default=LEVEL_MSG_SHOW_CONSOLE_2,
        help="Level of message for show in console",
    )

    args = parser.parse_args()
    return vars(args)


def init_db_simulate(con, sql_script_path):
    """Execute initialaze script database before simulate recorded workload"""

    result_code = 0

    if sql_script_path is not None:
        cur = con.cursor()
        with open(sql_script_path) as init_db_file:
            for sql_stmt in init_db_file.read().split(";"):
                try:
                    cur.execute(sql_stmt)
                except:
                    result_code = -1

    return result_code


def show_log_message(level_show_console, sql_cmd, len_workload, cnt):
    """Print log message in console"""

    msg = "\r"

    if len_workload is None:
        len_workload = "??"
        percent_complete = "??%"
    else:
        percent_complete = f"{(cnt / len_workload):2.1%}"

    if level_show_console == LEVEL_MSG_SHOW_CONSOLE_3:
        msg = f"{datetime.now()} | Total commands executed: {cnt} from {len_workload}. Completed {percent_complete}"
        msg += sql_cmd[:100] + ("..." if len(sql_cmd) > 100 else "")
    elif level_show_console == LEVEL_MSG_SHOW_CONSOLE_2:
        msg = f"\r{datetime.now()} | Total commands executed: {cnt} from {len_workload}. Completed {percent_complete}"

    if level_show_console in (
        LEVEL_MSG_SHOW_CONSOLE_2,
        LEVEL_MSG_SHOW_CONSOLE_3,
    ):
        print(msg)


def exec_commands_local(level_show_console, cursor) -> int:  
    """Get list all commands recorded workload from database, save locally and execute"""

    idx = 1
    sql_cmd = f"""
                SELECT `sql_cmd`
                FROM `cmd_for_exec`;"""
    cursor.execute(sql_cmd)

    stmts_sql_for_exec = cursor.fetchall()

    len_workload = len(stmts_sql_for_exec)

    for idx, stmt_sql in enumerate(stmts_sql_for_exec, start=1):
        sql_cmd = stmt_sql[0]
        execute_cmd(level_show_console, cursor, sql_cmd, len_workload, idx)
    return idx


def exec_commands_from_db(level_show_console, cursor) -> int:
    """Get command recorded workload from database by one and execute"""

    idx = 1
    is_cmd_to_execute = True
    len_workload = None
    while is_cmd_to_execute:
        sql_cmd = f"""
                    SELECT `sql_cmd`
                    FROM `cmd_for_exec`
                    WHERE `cmd_id` = {idx};"""
        cursor.execute(sql_cmd)

        if cursor.rowcount > 0:
            row = cursor.fetchone()
            sql_cmd = row[0]
            execute_cmd(level_show_console, cursor, sql_cmd, len_workload, idx)
        else:
            is_cmd_to_execute = False

        idx += 1
    return idx - 1


def execute_cmd(level_show_console, cursor, sql_cmd, len_workload, idx):
    """Execute SQL statment and show log message"""

    try:
        cursor.execute(sql_cmd)
        show_log_message(level_show_console, sql_cmd, len_workload, idx)
    except pymysql.err.DataError as e:
        print("Caught a pymysql.err.DataError exception:", e)


def create_fill_commands_table(thread_id, cursor):
    """Create temporary `cmd_for_exec` table with command for a particular thread"""

    sql_cmd = """DROP TABLE IF EXISTS `cmd_for_exec`;"""
    cursor.execute(sql_cmd)

    sql_cmd = """
                CREATE TEMPORARY TABLE IF NOT EXISTS `cmd_for_exec` (
                    `cmd_id` int NOT NULL AUTO_INCREMENT,
                    `start_time` timestamp(6) NOT NULL,
                    `sql_cmd`  mediumtext NOT NULL,
                    `thread_id` int unsigned NOT NULL,
                    PRIMARY KEY (cmd_id)
                );"""
    cursor.execute(sql_cmd)

    sql_cmd = f"""
                INSERT INTO `cmd_for_exec` (
                    `start_time`,
                    `sql_cmd`,
                    `thread_id`
                )
                SELECT
                    `start_time`,
                    CONVERT(`sql_text` USING utf8),
                    `thread_id`
                FROM car_ads_training_db.workload
                WHERE db = 'car_ads_training_db'
                    AND `thread_id` = {thread_id}
                    AND CONVERT(`sql_text` USING utf8) <> ''
                    AND CONVERT(`sql_text` USING utf8) NOT LIKE "--%"
                    ORDER BY `start_time` ASC;"""
    cursor.execute(sql_cmd)


def main():
    """Main function"""

    # Get args command line
    args_app = get_args()
    thread_id = args_app["thread_id"]
    exec_init_script = args_app["exec_init_script"]
    level_show_console = args_app["level_show_console"]
    storage_cmd = args_app["storage_cmd"]

    # Load app config
    with open("config.json") as config_file:
        configs = json.load(config_file)

    # Connect to database
    print("Connect to database")
    connection = pymysql.connect(**configs["audit_db"])

    # Execute initialaze script database
    if exec_init_script:
        init_db_simulate(connection, configs.get("simulator_init_db"))

    with connection:
        cursor = connection.cursor()

        # Cretae and fill table with commands for following execute
        print("Cretae and fill table with commands for following execute")
        create_fill_commands_table(thread_id, cursor)
        
        start_ts = datetime.now()
        # Execute commands from recorded workload from local stoarge or from DB
        if storage_cmd == STORAGE_CMD_LOCAL:
            totl_cmd = exec_commands_local(level_show_console, cursor)
        elif storage_cmd == STORAGE_CMD_DB:
            totl_cmd = exec_commands_from_db(level_show_console, cursor)
        else:
            print("Workload did not simulate")
            totl_cmd = 0
        print(f"Elapsed time: {datetime.now() - start_ts}. Total executed commands: {totl_cmd}")

    print("Done")


if __name__ == "__main__":
    main()
