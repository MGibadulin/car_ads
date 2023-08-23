#!/bin/bash

echo "------------------------------------------------------------"
echo Create tables
echo "------------------------------------------------------------"
echo Create *process_log* table 
sqlcmd -S localhost -U sa -P CARadspass07 -i ./DDL/tables/process_log.sql
wait
echo Done

echo "------------------------------------------------------------"
echo Create *event_log* table 
sqlcmd -S localhost -U sa -P CARadspass07 -i ./DDL/tables/event_log.sql
wait
echo Done

echo "------------------------------------------------------------"
echo Create *ads_archive* table 
sqlcmd -S localhost -U sa -P CARadspass07 -i ./DDL/tables/ads_archive.sql
wait
echo Done

echo "------------------------------------------------------------"
echo Starting finder.py
python3 /path_to_script/finder.py &
echo Done

echo "------------------------------------------------------------"
echo Starting scraper.py
python3 /path_to_script/scrapper.py &
echo Done

echo "------------------------------------------------------------"
echo Waiting 5 minutes, while scrapping data
sleep 300 &
echo Scrapping running in background...
wait

pkill -f finder.py
pkill -f scraper.py

echo "------------------------------------------------------------"
echo Starting full_load.py
python3 /path_to_script/full_load.py &
echo Full load running in background...
wait

echo "------------------------------------------------------------"
echo Testing full load
echo Run SQL script get number rows from source_db.ads_archive
echo Run SQL script get number rows from landing.ads_archive
echo Compare number of rows
echo Test Fail or Pass

echo "------------------------------------------------------------"
echo Starting scraper.py
python3 /path_to_script/scraper.py &
echo Done

echo "------------------------------------------------------------"
echo Waiting 5 minutes, while scrapping new data
sleep 300 &
echo Scrapping running in background...
wait

pkill -f scrapper.py

echo "------------------------------------------------------------"
echo Starting incremental_load.py
python3 /path_to_script/incremental_load.py &
echo Incremental load running in background...
wait

echo "------------------------------------------------------------"
echo Testing incremental load
echo Run SQL script get number rows from source_db.ads_archive
echo Run SQL script get number rows from landing.ads_archive
echo Compare number of rows
echo Test Fail or Pass