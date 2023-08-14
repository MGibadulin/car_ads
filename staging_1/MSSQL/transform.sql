BEGIN

    --start audit
    DECLARE process_id STRING;
    DECLARE truncated_row_cound INT64;
    DECLARE inserted_row_count INT64;
    DECLARE processed_row_count INT64;
    DECLARE message STRING;
    DECLARE metrics STRUCT < truncated INT64, inserted INT64, updated INT64, mark_as_deleted INT64, message STRING>;

    CALL `paid-project-346208`.meta_ds.usp_write_process_log('START', process_id,
                                                             'usp_stg1_cars_com_card_direct_tokenized_300_full_reload',
                                                             NULL);

    CALL `paid-project-346208`.meta_ds.usp_write_event_log(process_id, 'load data into stg1_cars_com_card_300',
                                                           'start');
    --end audit

    --start transforming data
    TRUNCATE TABLE `paid-project-346208`.car_ads_ds_staging_test.`cars_com_card_tokenized_300_Dima`;

    SET truncated_row_cound = @@row_count; --audit truncated rows

    SET processed_row_count =
            (SELECT COUNT(*) FROM `paid-project-346208`.car_ads_ds_landing.`cars_com_card_direct_300_Dima`);

    INSERT INTO `paid-project-346208`.car_ads_ds_staging_test.`cars_com_card_tokenized_300_Dima`
    (row_id,
     card_id,
     brand,
     model,
     `year`,
     price_history,
     price_usd,
     adress,
     state,
     zip_code,
     home_delivery,
     virtual_appointments,
     included_warranty,
     VIN,
     transmission,
     transmission_type,
     engine,
     engine_vol,
     fuel,
     mpg,
     mileage,
     mileage_unit,
     body,
     drive,
     color,
     one_owner,
     accidents_or_damage,
     clean_title,
     personal_use_only,
     comment,
     scrap_date,
     source_id,
     modified_date,
     row_hash,
     operation)
    WITH tokenized_data AS (SELECT SAFE_CAST(GENERATE_UUID() AS STRING)                                               AS row_id,
                                   SAFE_CAST(card_id AS STRING)                                                       AS card_id,
                                   CASE
                                       WHEN SAFE_CAST(REGEXP_EXTRACT(title, r'(\S+)', 1, 2) AS STRING) = 'Land'
                                           THEN 'Land Rover'
                                       WHEN SAFE_CAST(REGEXP_EXTRACT(title, r'(\S+)', 1, 2) AS STRING) = 'Alfa'
                                           THEN 'Alfa Romeo'
                                       WHEN SAFE_CAST(REGEXP_EXTRACT(title, r'(\S+)', 1, 2) AS STRING) = 'Am'
                                           THEN 'Am General'
                                       WHEN SAFE_CAST(REGEXP_EXTRACT(title, r'(\S+)', 1, 2) AS STRING) = 'American'
                                           THEN 'American Motors'
                                       WHEN SAFE_CAST(`REGEXP_EXTRACT`(title, r'(\S+)', 1, 2) AS STRING) = 'Aston'
                                           THEN 'Aston Martin'
                                       WHEN SAFE_CAST(`REGEXP_EXTRACT`(title, r'(\S+)', 1, 2) AS STRING) = 'Avanti'
                                           THEN 'Avanti Motors'
                                       ELSE SAFE_CAST(REGEXP_EXTRACT(title, r'(\S+)', 1, 2) AS STRING)
                                       END                                                                            AS brand,
                                   CASE
                                       WHEN SAFE_CAST(REGEXP_EXTRACT(title, r'(\S+)', 1, 2) AS STRING) = 'Land'
                                           THEN SAFE_CAST(REPLACE(REGEXP_REPLACE(title, r'^(\S+) (\S+) ', ''), 'Rover ', '') AS STRING)
                                       WHEN SAFE_CAST(REGEXP_EXTRACT(title, r'(\S+)', 1, 2) AS STRING) = 'Alfa'
                                           THEN SAFE_CAST(REPLACE(REGEXP_REPLACE(title, r'^(\S+) (\S+) ', ''), 'Romeo ', '') AS STRING)
                                       WHEN SAFE_CAST(REGEXP_EXTRACT(title, r'(\S+)', 1, 2) AS STRING) = 'Am'
                                           THEN SAFE_CAST(REPLACE(REGEXP_REPLACE(title, r'^(\S+) (\S+) ', ''), 'General ', '') AS STRING)
                                       WHEN SAFE_CAST(`REGEXP_EXTRACT`(title, r'(\S+)', 1, 2) AS STRING) = 'American'
                                           THEN SAFE_CAST(REPLACE(REGEXP_REPLACE(title, r'^(\S+) (\S+) ', ''), 'Motors ', '') AS STRING)
                                       WHEN SAFE_CAST(`REGEXP_EXTRACT`(title, r'(\S+)', 1, 2) AS STRING) = 'Aston'
                                           THEN SAFE_CAST(REPLACE(REGEXP_REPLACE(title, r'^(\S+) (\S+) ', ''), 'Martin ', '') AS STRING)
                                       WHEN SAFE_CAST(`REGEXP_EXTRACT`(title, r'(\S+)', 1, 2) AS STRING) = 'Avanti'
                                           THEN SAFE_CAST(REPLACE(REGEXP_REPLACE(title, r'^(\S+) (\S+) ', ''), 'Motors ', '') AS STRING)
                                       ELSE SAFE_CAST(REGEXP_REPLACE(title, r'^(\S+) (\S+) ', '') AS STRING)
                                       END                                                                            AS model,

                                   SAFE_CAST(REGEXP_EXTRACT(title, r'(\S+)', 1) AS INT64)                             AS `year`,
                                   SAFE_CAST(price_history AS STRING)                                                 AS price_history,
                                   SAFE_CAST(REGEXP_REPLACE(price_primary, r'[^0-9]+', '') AS int64)                  AS price_usd,
                                   SAFE_CAST(REGEXP_REPLACE(location, r', (\S+ \S+)', '') AS STRING)                  AS adress,
                                   CASE
                                       WHEN SAFE_CAST(REGEXP_EXTRACT(location, r', (\S+)') AS STRING) is NULL
                                           THEN SAFE_CAST('' AS STRING)
                                       ELSE SAFE_CAST(REGEXP_EXTRACT(location, r', (\S+)') AS STRING)
                                       END                                                                            AS state,
                                   CASE
                                       WHEN SAFE_CAST(REGEXP_EXTRACT(location, r' (\d{5})') AS STRING) is NULL
                                           THEN SAFE_CAST('' AS STRING)
                                       ELSE SAFE_CAST(REGEXP_EXTRACT(location, r' (\d{5})') AS STRING)
                                       END                                                                            AS zip_code,
                                   CASE
                                       WHEN REGEXP_EXTRACT(labels, r'Home Delivery') LIKE 'Home Delivery'
                                           THEN SAFE_CAST('Y' AS STRING)
                                       ELSE SAFE_CAST('N' AS STRING)
                                       END                                                                            AS home_delivery,
                                   CASE
                                       WHEN REGEXP_EXTRACT(labels, r'Virtual Appointments') LIKE 'Virtual Appointments'
                                           THEN SAFE_CAST('Y' AS STRING)
                                       ELSE SAFE_CAST('N' AS STRING)
                                       END                                                                            AS virtual_appointments,
                                   CASE
                                       WHEN REGEXP_EXTRACT(labels, r'Included warranty') LIKE 'Included warranty'
                                           THEN SAFE_CAST('Y' AS STRING)
                                       ELSE SAFE_CAST('N' AS STRING)
                                       END                                                                            AS included_warranty,
                                   SAFE_CAST(REGEXP_EXTRACT(labels, r'VIN: (\S{17})') AS STRING)                      AS VIN,
                                   SAFE_CAST(split(description, ',')[1] AS STRING)                                    AS transmission, --if NULL duplicate the value from transmission_type?
                                   CASE
                                       WHEN REGEXP_EXTRACT(description,
                                                           r'([AM]/[T]|[A][u]\S+|[M][a][n]\S+|w/\S+|CVT)') LIKE
                                            'Automatic,'
                                           THEN SAFE_CAST(SUBSTRING(REGEXP_EXTRACT(description,
                                                                                   r'([AM]/[T]|[A][u]\S+|[M][a][n]\S+|w/\S+|CVT)'),
                                                                    1,
                                                                    LENGTH(REGEXP_EXTRACT(description,
                                                                                          r'([AM]/[T]|[A][u]\S+|[M][a][n]\S+|w/\S+|CVT)')) -
                                                                    1) AS STRING)
                                       WHEN REGEXP_EXTRACT(description,
                                                           r'([AM]/[T]|[A][u]\S+|[M][a][n]\S+|w/\S+|CVT)') LIKE
                                            'Manual,'
                                           THEN SAFE_CAST(SUBSTRING(REGEXP_EXTRACT(description,
                                                                                   r'([AM]/[T]|[A][u]\S+|[M][a][n]\S+|w/\S+|CVT)'),
                                                                    1,
                                                                    LENGTH(REGEXP_EXTRACT(description,
                                                                                          r'([AM]/[T]|[A][u]\S+|[M][a][n]\S+|w/\S+|CVT)')) -
                                                                    1) AS STRING)
                                       WHEN REGEXP_EXTRACT(description,
                                                           r'([AM]/[T]|[A][u]\S+|[M][a][n]\S+|w/\S+|CVT)') LIKE 'A/T'
                                           THEN SAFE_CAST('Automatic' AS STRING)
                                       WHEN REGEXP_EXTRACT(description,
                                                           r'([AM]/[T]|[A][u]\S+|[M][a][n]\S+|w/\S+|CVT)') LIKE 'M/T'
                                           THEN SAFE_CAST('Manual' AS STRING)
                                       WHEN REGEXP_EXTRACT(description,
                                                           r'([AM]/[T]|[A][u]\S+|[M][a][n]\S+|w/\S+|CVT)') LIKE 'w/Dual'
                                           THEN SAFE_CAST('Automatic' AS STRING)
                                       WHEN REGEXP_EXTRACT(description,
                                                           r'([AM]/[T]|[A][u]\S+|[M][a][n]\S+|w/\S+|CVT)') LIKE 'CVT'
                                           THEN SAFE_CAST('Automatic' AS STRING)
                                       WHEN REGEXP_EXTRACT(description,
                                                           r'([AM]/[T]|[A][u]\S+|[M][a][n]\S+|w/\S+|CVT)') LIKE 'Auto'
                                           THEN SAFE_CAST('Automatic' AS STRING)
                                       WHEN REGEXP_EXTRACT(description, r'(Variable)') LIKE 'Variable'
                                           THEN SAFE_CAST('Variable' AS STRING)
                                       WHEN REGEXP_EXTRACT(description, r'(Turbo)') LIKE 'Turbo'
                                           THEN SAFE_CAST('Automatic' AS STRING)
                                       ELSE SAFE_CAST(REGEXP_EXTRACT(description,
                                                                     r'([AM]/[T]|[A][u]\S+|[M][a][n]\S+|w/\S+|CVT)') AS STRING)
                                       END                                                                            AS transmission_type,
                                   SAFE_CAST(split(description, ',')[2] AS STRING)                                    AS engine,
                                   CASE
                                       WHEN SAFE_CAST(REGEXP_EXTRACT(description, r'(\d{1}.\d{1})L') AS float64) *
                                            1000 is not NULL
                                           THEN SAFE_CAST(
                                                   SAFE_CAST(REGEXP_EXTRACT(description, r'(\d{1}.\d{1})L') AS float64) *
                                                   1000 AS int64)
                                       ELSE 0
                                       END                                                                            AS engine_vol,
                                   REGEXP_EXTRACT(split(description, ',')[3], r'([A-Z]\w+)')                          AS fuel,
                                   CASE
                                       WHEN SAFE_CAST(REGEXP_EXTRACT(description, r'[(](\S+)') AS STRING) is NULL
                                           THEN SAFE_CAST('' AS STRING)
                                       ELSE SAFE_CAST(REGEXP_EXTRACT(description, r'[(](\S+)') AS STRING)
                                       END                                                                            AS mpg,
                                   CASE
                                       WHEN SAFE_CAST(REPLACE(REGEXP_EXTRACT(description, r'(\d+ \d+) mi.'), ' ', '') AS INT64) is NULL
                                           THEN SAFE_CAST(REGEXP_EXTRACT(description, r'(\d+) mi') AS int64)
                                       ELSE SAFE_CAST(REPLACE(REGEXP_EXTRACT(description, r'(\d+ \d+) mi.'), ' ', '') AS INT64)
                                       END                                                                            AS mileage,
                                   CASE
                                       WHEN REGEXP_EXTRACT(description, r'mi') LIKE 'mi'
                                           THEN SAFE_CAST('mile' AS STRING)
                                       END                                                                            AS mileage_unit,
                                   CASE
                                       WHEN REGEXP_EXTRACT(description, r'mi. . (\S* \S+),') is NULL
                                           THEN SAFE_CAST(REGEXP_EXTRACT(description, r'mi. . (\S+),') AS STRING)
                                       ELSE SAFE_CAST(REGEXP_EXTRACT(description, r'[|] (\w+.\S+),') AS STRING)
                                       END                                                                            AS body,
                                   CASE
                                       WHEN SAFE_CAST(REGEXP_EXTRACT(description, r'[|] \w+.\S+, (\w+.\S+ Drive)') AS STRING) is NULL
                                           THEN SAFE_CAST(REGEXP_EXTRACT(description, r'(.[W][D])') AS STRING)
                                       --WHEN SAFE_CAST(REGEXP_EXTRACT(description, r'(.[W][D])') AS STRING) like 'FWD'
                                       --THEN SAFE_CAST('Front-wheel Drive' AS STRING)
                                       --WHEN SAFE_CAST(REGEXP_EXTRACT(description, r'(.[W][D])') AS STRING) like '4WD'
                                       --THEN SAFE_CAST('Four-wheel Drive' AS STRING)
                                       ELSE SAFE_CAST(REGEXP_EXTRACT(description, r'[|] \w+.\S+,.(\w+.\S+ Drive)') AS STRING)
                                       END                                                                            AS drive,
                                   SAFE_CAST(split(description, ',')[6] AS STRING)                                    AS color,
                                   CASE
                                       WHEN SAFE_CAST(REGEXP_EXTRACT(vehicle_history, r'1-owner vehicle: (\S{3})') AS STRING) is not Null
                                           THEN SAFE_CAST(REGEXP_EXTRACT(vehicle_history, r'1-owner vehicle: (\S{3})') AS STRING)
                                       ELSE "No"
                                       END                                                                            AS one_owner,
                                   CASE
                                       WHEN SAFE_CAST(REGEXP_EXTRACT(vehicle_history, r'Accidents or damage: (\S* \S+) [|]') AS STRING) is not Null
                                           THEN SAFE_CAST(REGEXP_EXTRACT(vehicle_history, r'Accidents or damage: (\S* \S+) [|]') AS STRING)
                                       WHEN SAFE_CAST(REGEXP_EXTRACT(vehicle_history, r'Accidents or damage: (\S* \S+) [|]') AS STRING) is Null
                                           THEN SAFE_CAST(REGEXP_EXTRACT(vehicle_history,
                                                                         r'Accidents or damage: (\S* \S+ \S+ \S+ \S+ \S+ \S+) [|]') AS STRING)
                                       END                                                                            AS accidents_or_damage,
                                   CASE
                                       WHEN SAFE_CAST(REGEXP_EXTRACT(vehicle_history, r'Clean title: (\S{3}) [|]') AS STRING) is not Null
                                           THEN SAFE_CAST(REGEXP_EXTRACT(vehicle_history, r'Clean title: (\S{3}) [|]') AS STRING)
                                       ELSE "No"
                                       END                                                                            AS clean_title,
                                   SAFE_CAST(REGEXP_EXTRACT(vehicle_history, r' Personal use only: (\S+)') AS STRING) AS personal_use_only,
                                   SAFE_CAST(comment AS STRING)                                                       AS comment,
                                   SAFE_CAST(scrap_date AS TIMESTAMP)                                                 AS scrap_date,
                                   SAFE_CAST(source_id AS STRING)                                                     AS source_id,
                                   CURRENT_TIMESTAMP()                                                                AS modified_date,
                                   SHA256(CONCAT(
                                           IFNULL(card_id, ''),
                                           IFNULL(title, ''),
                                           IFNULL(price_primary, ''),
                                           IFNULL(price_history, ''),
                                           IFNULL(location, ''),
                                           IFNULL(labels, ''),
                                           IFNULL(comment, ''),
                                           IFNULL(description, ''),
                                           IFNULL(vehicle_history, '')
                                       ))                                                                             AS row_hash
                            FROM `paid-project-346208`.car_ads_ds_landing.`cars_com_card_direct_300_Dima`),
         ordered_data as (SELECT row_id,
                                 card_id,
                                 brand,
                                 model,
                                 `year`,
                                 price_history,
                                 price_usd,
                                 adress,
                                 state,
                                 zip_code,
                                 home_delivery,
                                 virtual_appointments,
                                 included_warranty,
                                 VIN,
                                 transmission,
                                 transmission_type,
                                 engine,
                                 engine_vol,
                                 fuel,
                                 mpg,
                                 mileage,
                                 mileage_unit,
                                 body,
                                 drive,
                                 color,
                                 one_owner,
                                 accidents_or_damage,
                                 clean_title,
                                 personal_use_only,
                                 comment,
                                 scrap_date,
                                 source_id,
                                 modified_date,
                                 row_hash,
                                 ROW_NUMBER() OVER (PARTITION BY row_hash ORDER BY modified_date) AS rn
                          FROM tokenized_data)
    SELECT row_id,
           card_id,
           brand,
           model,
           `year`,
           price_history,
           price_usd,
           adress,
           state,
           zip_code,
           home_delivery,
           virtual_appointments,
           included_warranty,
           VIN,
           transmission,
           transmission_type,
           engine,
           engine_vol,
           fuel,
           mpg,
           mileage,
           mileage_unit,
           body,
           drive,
           color,
           one_owner,
           accidents_or_damage,
           clean_title,
           personal_use_only,
           comment,
           scrap_date,
           source_id,
           modified_date,
           row_hash,
           'i'
    FROM ordered_data
    WHERE rn = 1;
    --end transforming data

    SET inserted_row_count = @@row_count; --audit inserted rows

    SET message = ('Processed row count in Landing = ' || SAFE_CAST(processed_row_count AS STRING) || '.');

    SET metrics = (truncated_row_cound, inserted_row_count, NULL, NULL, message);

    -- start audit
    CALL `paid-project-346208`.meta_ds.usp_write_event_log(process_id, 'load data into stg1_cars_com_card_300', 'end');

    CALL `paid-project-346208`.meta_ds.usp_write_process_log('END', process_id,
                                                             'usp_stg1_cars_com_card_direct_tokenized_300_full_reload',
                                                             metrics);
    -- end audit
END