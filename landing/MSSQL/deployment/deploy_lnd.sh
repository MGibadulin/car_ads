#!/bin/bash

SOURCE_DB="car_ads_training_db.dbo.ads_archive"
DEST_DB="Landing.dbo.lnd_ads_archive"

echo "------------------------------------------------------------"
echo "Create tables"
echo "------------------------------------------------------------"

#LIST=("../DDL/tables/process_log.sql" "../DDL/tables/event_log.sql" "../DDL/tables/lnd_ads_archive.sql")
LIST=("../DDL/tables/lnd_ads_archive.sql")
for ITEM in ${LIST[@]}
do
    echo "Create table $ITEM"
    sqlcmd -S "srv,1434" -U sa -P pass -i $ITEM
    wait
    echo "Done"
done

# ! how to check that the sqlcmd was executed without errors
# ! how to check that the python script was executed without errors
# ! May use test table?


echo "------------------------------------------------------------"
echo "Starting full_load.py"
python3 ../full_load.py &
echo "Full load running in background..."
wait

echo "------------------------------------------------------------"
echo "Testing full load"

echo "Run SQL script get number rows from $SOURCE_DB"
RESULT_1=$(sqlcmd -S "srv,1434" -U sa -P pass -h -1 -W -i get_count_source_db.sql)
wait
CNT_ROW_SOURCE="${RESULT_1//[^0-9]/}"
echo "Rows in $SOURCE_DB: $CNT_ROW_SOURCE"


echo "Run SQL script get number rows from $DEST_DB"
RESULT_2=$(sqlcmd --S "srv,1434" -U sa -P pass -h -1 -W -i get_count_dest_db.sql)
wait
CNT_ROW_DEST="${RESULT_2//[^0-9]/}"
echo "Rows in $DEST_DB: $CNT_ROW_DEST"


echo "Compare number of rows"
if [ $CNT_ROW_SOURCE -eq $CNT_ROW_DEST ]
then
    echo "Test PASS"
else
    echo "Test FAIL"
    exit 1
fi

echo "------------------------------------------------------------"
echo "Add testing data to source table"
sqlcmd -S "srv,1434" -U sa -P pass -i add_testing_data.sql


echo "------------------------------------------------------------"
echo "Starting incremental_load.py"
python3 ../incremental_load.py &
echo "Incremental load running in background..."
wait

echo "------------------------------------------------------------"
echo "Testing incremental load"

echo "Run SQL script get number new rows from $SOURCE_DB"
RESULT_1=$(sqlcmd -S "srv,1434" -U sa -P pass -h -1 -W -i get_count_source_db.sql)
wait
CNT_ROW_SOURCE="${RESULT_1//[^0-9]/}"
echo "Rows in $SOURCE_DB: $CNT_ROW_SOURCE"


echo "Run SQL script get number rows from $DEST_DB"
RESULT_2=$(sqlcmd -S "srv,1434" -U sa -P pass -h -1 -W -i get_count_dest_db.sql)
wait
CNT_ROW_DEST="${RESULT_2//[^0-9]/}"
echo "Rows in $DEST_DB: $CNT_ROW_DEST"

echo "Compare number of rows"
if [ $CNT_ROW_SOURCE -eq $CNT_ROW_DEST ]
then
    echo "Test PASS"
else
    echo "Test FAIL"
    exit 1
fi

echo "------------------------------------------------------------"
echo "End deployment script"
