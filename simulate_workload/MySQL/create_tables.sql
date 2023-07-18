create database if not exists car_ads_training_db;

use car_ads_training_db;

set global general_log = 0;
set global slow_query_log = 0;
set global max_heap_table_size = 1024*1024*1024*4;


create table if not exists ads
(
    ads_id                       int not null auto_increment,
	INDEX USING BTREE (ads_id),
    source_id                    varchar(100) not null,
    card_url                     varchar(255) not null,
    ad_group_id                  int not null,
    insert_process_log_id        int not null,
    insert_date                  datetime not null default current_timestamp,
    change_status_process_log_id int,
    ad_status                    tinyint not null default 0,
    change_status_date           datetime,
    card                         varchar(21300)
    
) character set latin1  ENGINE = MEMORY;

create table if not exists ad_groups
(
    ad_group_id         int not null auto_increment,
	INDEX USING BTREE (ad_group_id),
    group_url           varchar(255) not null,
    process_log_id      int not null,
    insert_date         datetime not null default current_timestamp
) character set latin1  ENGINE = MEMORY;

create table if not exists process_log
(
    process_log_id      int not null auto_increment,
	INDEX USING BTREE (process_log_id),
    process_desc        varchar(255) not null,
    `user`              varchar(255) not null,
    host                varchar(255) not null,
    connection_id       int,
    start_date          datetime not null default current_timestamp,
    end_date            datetime
) character set latin1  ENGINE = MEMORY;