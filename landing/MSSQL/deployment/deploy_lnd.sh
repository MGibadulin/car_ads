#!/bin/bash

echo "------------------------------------------------------------"
echo "Create tables"
echo "------------------------------------------------------------"

LIST=("../DDL/tables/process_log.sql" "../DDL/tables/event_log.sql" "../DDL/tables/lnd_ads_archive.sql")

for ITEM in ${LIST[@]}
do
    echo "Create table $ITEM"
    sqlcmd -S "server_ip,1434" -U sa -P pass -i $ITEM
    wait
    echo "Done"
done

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
python3 ../full_load.py &
echo Full load running in background...
wait

echo "------------------------------------------------------------"
echo Testing full load
echo Run SQL script get number rows from car_ads_training_db.dbo.ads_archive
RESULT_1 = $(sqlcmd -S "server_ip,1434" -U sa -P pass -h -1 -Q "select count(*) from car_ads_training_db.dbo.ads_archive;")
wait

echo Run SQL script get number rows from Landing.dbo.lnd_ads_archive
RESULT_2 = $(sqlcmd -S "server_ip,1434" -U sa -P pass -h -1 -Q "select count(*) from Landing.dbo.lnd_ads_archive;")
wait
echo Compare number of rows
if [ $RESULT_1 -eq $RESULT_2 ]
then
    echo "Test PASS"
else
    echo "Test FAIL"
    exit 1
fi


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
python3 ../incremental_load.py &
echo Incremental load running in background...
wait

echo "------------------------------------------------------------"
echo Testing incremental load
echo Run SQL script get number rows from source_db.ads_archive
echo Run SQL script get number rows from landing.ads_archive
echo Compare number of rows
echo Test Fail or Pass