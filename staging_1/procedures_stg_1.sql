-- Transform data from landing to staging 1


-- Full load and tokenize from landing to staging_1
CREATE OR REPLACE PROCEDURE `paid-project-346208`.`car_ads_ds_staging_test`.usp_landing_staging1_av_by_card_tokenized_full_load()
BEGIN 
	-- start audit
	DECLARE process_id STRING;
	DECLARE truncate_row_count INT64;
	DECLARE insert_row_count INT64;
	DECLARE metrics STRUCT <truncated INT64, inserted INT64, updated INT64, mark_as_deleted INT64, message STRING>;

	CALL `paid-project-346208`.`meta_ds`.`usp_write_process_log`(
		"START",
		process_id,
		"usp_landing_staging1_av_by_card_tokenized_full_load", 
		NULL
	);

	TRUNCATE TABLE `paid-project-346208`.`car_ads_ds_staging_test`.`cars_av_by_card_tokenized`;

	--get quantity of rows which will be truncated
	SET truncate_row_count = @@row_count;

	INSERT INTO `paid-project-346208`.`car_ads_ds_staging_test`.`cars_av_by_card_tokenized`
	WITH t1 AS
	(
		SELECT
			card_id,
			title,
			price_secondary,
			location,
			labels,
			comment,
			description,
			exchange,
			scrap_date,
			ROW_NUMBER() OVER(
			PARTITION BY 
				card_id,
				title,
				price_secondary,
				location,
				labels,
				comment,
				description,
				exchange 
			ORDER BY scrap_date ASC
			) AS rn
		FROM `paid-project-346208`.`car_ads_ds_landing`.`lnd_cars-av-by_card_300`
	)
	SELECT
		GENERATE_UUID() AS row_id,
		SHA256(CONCAT(IFNULL(title, ""),
					IFNULL(price_secondary, ""),
					IFNULL(location, ""),
					IFNULL(labels, ""),
					IFNULL(comment, ""),
					IFNULL(description, ""),
					IFNULL(exchange, ""))
				) AS row_hash,
		SAFE_CAST(card_id AS STRING) AS card_id,
		CASE
			WHEN split(title, ' ')[1] = 'Lada' THEN 'Lada (ВАЗ)'
			WHEN split(title, ' ')[1] = 'Alfa' THEN 'Alfa Romeo'
			WHEN split(title, ' ')[1] = 'Dongfeng' THEN 'Dongfeng Honda'
			WHEN split(title, ' ')[1] = 'Great' THEN 'Great Wall'
			WHEN split(title, ' ')[1] = 'Iran' THEN 'Iran Khodro'
			WHEN split(title, ' ')[1] = 'Land' THEN 'Land Rover'
			ELSE split(title, ' ')[1]
		END AS brand,
		CASE
			WHEN split(title, ' ')[1] = 'Lada' THEN REGEXP_EXTRACT(title, r'Продажа Lada \(ВАЗ\) (.+)[,]')
			WHEN split(title, ' ')[1] = 'Alfa' THEN REGEXP_EXTRACT(title, r'Продажа Alfa Romeo (.+)[,]')
			WHEN split(title, ' ')[1] = 'Dongfeng' THEN REGEXP_EXTRACT(title, r'Продажа Dongfeng Honda (.+)[,]')
			WHEN split(title, ' ')[1] = 'Great' THEN REGEXP_EXTRACT(title, r'Продажа Great Wall (.+)[,]')
			WHEN split(title, ' ')[1] = 'Iran' THEN REGEXP_EXTRACT(title, r'Продажа Iran Khodro (.+)[,]')
			WHEN split(title, ' ')[1] = 'Land' THEN REGEXP_EXTRACT(title, r'Продажа Land Rover (.+)[,]')
			ELSE REGEXP_EXTRACT(title, r'Продажа [a-zA-Zа-яёА-ЯЁ-]+ (.+)[,]')
		END AS model,
		SAFE_CAST(REGEXP_EXTRACT(description, r'^(\d{4}) г.,') AS INT) AS year,
		SAFE_CAST(REGEXP_EXTRACT(REPLACE(price_secondary, ' ', ''), r'≈(\d+)\$') AS INT) AS price,
		REGEXP_EXTRACT(description, r',\s(\S+),.+') AS transmission,
		SAFE_CAST(REPLACE(REGEXP_EXTRACT(description, r',\s(\d+[ ]?\d*) км'), " ", "") AS INT) AS mileage,
		REGEXP_EXTRACT(description, r'\| ([А-Яа-я0-9. ]+)') AS body,
		SAFE_CAST(REGEXP_EXTRACT(REPLACE(split(description, ',')[2], '.', ''), r'[0-9]+') AS INT) * 100 AS engine_vol,
		CASE 
			WHEN INSTR(description, 'Запас хода') <> 0 THEN 'Electric'
			ELSE split(description, ',')[3]	
		END AS fuel,
		CASE 
			WHEN INSTR(exchange, 'Возможен обмен') <> 0 THEN 'Y'
			WHEN INSTR(exchange, 'Возможен обмен с моей доплатой') <> 0 THEN 'Y'
			WHEN INSTR(exchange, 'Возможен обмен с вашей доплатой') <> 0 THEN 'Y'
			WHEN INSTR(exchange, 'Обмен не интересует') <> 0 THEN 'N'
			ELSE 'N'
		END AS exchange,
		CASE 
			WHEN INSTR(labels, 'TOP') <> 0 THEN 'Y'
			ELSE 'N'
		END AS top,
		CASE
			WHEN INSTR(labels, 'VIN') <> 0 THEN 'Y'
			ELSE 'N'
		END as vin,
		CASE
			WHEN INSTR(labels, 'аварийный') <> 0 THEN 'Y'
			ELSE 'N'
		END AS crahed,
		CASE
			WHEN INSTR(labels, 'на запчасти') <> 0 THEN 'Y'
			ELSE 'N'
		END AS for_spare,
		CASE 
			WHEN INSTR(location, ',') <> 0 THEN TRIM(split(location, ',')[0])
			WHEN location IS NULL THEN ''
			ELSE location
		END AS city,
		CASE 
			WHEN INSTR(location, ',') <> 0 THEN TRIM(split(location, ',')[1])
			ELSE ''
		END AS region,	
		CASE 
			WHEN comment IS NULL THEN ''
			ELSE comment
		END AS comment,
		scrap_date,
		CURRENT_TIMESTAMP() AS modified_date,
		"N" AS deleted
	FROM t1
	WHERE t1.rn = 1; --delete duplicates
	
	--get quantity of rows which have been inserted
	SET insert_row_count = @@row_count;

	SET metrics = (truncate_row_count, insert_row_count, NULL, NULL, NULL);
	CALL `paid-project-346208`.`meta_ds`.`usp_write_process_log`(
		"END",
		process_id,
		"usp_landing_staging1_av_by_card_tokenized_full_load", 
		metrics
	);
END;

-- Full_merge and tokenize from landing to staging_1
CREATE OR REPLACE PROCEDURE `paid-project-346208`.`car_ads_ds_staging_test`.usp_landing_staging1_av_by_card_tokenized_full_merge()
BEGIN
	DECLARE process_id STRING;
	DECLARE insert_row_count INT64;
	DECLARE metrics STRUCT <truncated INT64, inserted INT64, updated INT64, mark_as_deleted INT64, message STRING>;
	
	-- start audit
	CALL `paid-project-346208`.`meta_ds`.`usp_write_process_log`("START", process_id, "usp_landing_staging1_av_by_card_tokenized_full_merge", NULL);

	CALL `paid-project-346208`.`meta_ds`.`usp_write_event_log`(process_id, "Deduplicate source data", "start");

	-- cretae temp table without duplicats
	CREATE TEMP TABLE lnd_wo_duplicats
	(
		card_id 		STRING NOT NULL,
		title 			STRING,
		price_secondary STRING,
		location 		STRING,
		labels 			STRING,
		comment 		STRING,
		description 	STRING,
		exchange 		STRING,
		scrap_date 		TIMESTAMP
	);
	--  insert deduplicated data 
	INSERT INTO lnd_wo_duplicats (
		card_id, 
		title,
		price_secondary,
		location,
		labels,
		comment,
		description,
		exchange,
		scrap_date
	)
	WITH src AS 
	(
		SELECT
		card_id,
		title,
		price_secondary,
		location,
		labels,
		comment,
		description,
		exchange,
		scrap_date,
		ROW_NUMBER() OVER(
		PARTITION BY 
			card_id,
			title,
			price_secondary,
			location,
			labels,
			comment,
			description,
			exchange
		ORDER BY scrap_date ASC
		) AS rn
		FROM `paid-project-346208`.`car_ads_ds_landing`.`lnd_cars-av-by_card_300`
	)
	SELECT 
		SAFE_CAST(card_id AS STRING) AS card_id,
		title,
		price_secondary,
		location,
		labels,
		comment,
		description,
		exchange,
		scrap_date
	FROM src
	WHERE src.rn = 1;

	CALL `paid-project-346208`.`meta_ds`.`usp_write_event_log`(process_id, "Deduplicate source data", "end");

	-- get new rows with card_id that were not in the stage_1 table
	CALL `paid-project-346208`.`meta_ds`.`usp_write_event_log`(process_id, "Extract new records", "start");

	CREATE TEMP TABLE row_for_insert
	(
		card_id 		STRING NOT NULL,
		title 			STRING,
		price_secondary STRING,
		location 		STRING,
		labels 			STRING,
		comment 		STRING,
		description 	STRING,
		exchange 		STRING,
		scrap_date 		TIMESTAMP,
		oper 			STRING
	);
	-- insert new records
	INSERT INTO row_for_insert (
		card_id, 
		title,
		price_secondary,
		location,
		labels,
		comment,
		description,
		exchange,
		scrap_date,
		oper
	)
	SELECT 
		lnd.card_id,
		lnd.title,
		lnd.price_secondary,
		lnd.location,
		lnd.labels,
		lnd.comment,
		lnd.description,
		lnd.exchange,
		lnd.scrap_date,
		"INSERTED" AS oper
	FROM lnd_wo_duplicats AS lnd
	LEFT JOIN `paid-project-346208`.`car_ads_ds_staging_test`.`cars_av_by_card_tokenized` AS stg
	ON lnd.card_id = stg.card_id
	WHERE stg.card_id IS NULL;
	
	CALL `paid-project-346208`.`meta_ds`.`usp_write_event_log`(process_id, "Extract new records", "end");

	-- get rows that were already in the stage_1 table, but with changed fields
	CALL `paid-project-346208`.`meta_ds`.`usp_write_event_log`(process_id, "Extract updated records", "start");

	INSERT INTO row_for_insert (
		card_id, 
		title,
		price_secondary,
		location,
		labels,
		comment,
		description,
		exchange,
		scrap_date,
		oper
	)
	SELECT 
		lnd.card_id,
		lnd.title,
		lnd.price_secondary,
		lnd.location,
		lnd.labels,
		lnd.comment,
		lnd.description,
		lnd.exchange,
		lnd.scrap_date,
		"UPDATED" As oper
	FROM lnd_wo_duplicats AS lnd
	INNER JOIN `paid-project-346208`.`car_ads_ds_staging_test`.`cars_av_by_card_tokenized` AS stg
	ON lnd.card_id = stg.card_id
	WHERE SHA256(CONCAT(IFNULL(lnd.title, ""),
					IFNULL(lnd.price_secondary, ""),
					IFNULL(lnd.location, ""),
					IFNULL(lnd.labels, ""),
					IFNULL(lnd.comment, ""),
					IFNULL(lnd.description, ""),
					IFNULL(lnd.exchange, ""))
				) NOT IN (SELECT row_hash 
							FROM `paid-project-346208`.`car_ads_ds_staging_test`.`cars_av_by_card_tokenized` AS stg_inner
							WHERE  stg.card_id = stg_inner.card_id);
	
	CALL `paid-project-346208`.`meta_ds`.`usp_write_event_log`(process_id, "Extract updated records", "end");

	-- ! создать временную таблицу для токенизированных записей
	-- ! вторым этапом залить данные в Stg_1. с проверкой уловий на bad data и записать в евент_лог
	-- tokinize car ads and insert stage_1
	CALL `paid-project-346208`.`meta_ds`.`usp_write_event_log`(process_id, "Transform & Load data", "start");

	INSERT INTO `paid-project-346208`.`car_ads_ds_staging_test`.`cars_av_by_card_tokenized`
	SELECT
		GENERATE_UUID(),
		SHA256(CONCAT(IFNULL(src.title, ""),
					IFNULL(src.price_secondary, ""),
					IFNULL(src.location, ""),
					IFNULL(src.labels, ""),
					IFNULL(src.comment, ""),
					IFNULL(src.description, ""),
					IFNULL(src.exchange, ""))
				),
		src.card_id,
		CASE
			WHEN split(src.title, ' ')[1] = 'Lada' THEN 'Lada (ВАЗ)'
			WHEN split(src.title, ' ')[1] = 'Alfa' THEN 'Alfa Romeo'
			WHEN split(src.title, ' ')[1] = 'Dongfeng' THEN 'Dongfeng Honda'
			WHEN split(src.title, ' ')[1] = 'Great' THEN 'Great Wall'
			WHEN split(src.title, ' ')[1] = 'Iran' THEN 'Iran Khodro'
			WHEN split(src.title, ' ')[1] = 'Land' THEN 'Land Rover'
			ELSE split(src.title, ' ')[1]
		END,
		CASE
			WHEN split(src.title, ' ')[1] = 'Lada' THEN REGEXP_EXTRACT(src.title, r'Продажа Lada \(ВАЗ\) (.+)[,]')
			WHEN split(src.title, ' ')[1] = 'Alfa' THEN REGEXP_EXTRACT(src.title, r'Продажа Alfa Romeo (.+)[,]')
			WHEN split(src.title, ' ')[1] = 'Dongfeng' THEN REGEXP_EXTRACT(src.title, r'Продажа Dongfeng Honda (.+)[,]')
			WHEN split(src.title, ' ')[1] = 'Great' THEN REGEXP_EXTRACT(src.title, r'Продажа Great Wall (.+)[,]')
			WHEN split(src.title, ' ')[1] = 'Iran' THEN REGEXP_EXTRACT(src.title, r'Продажа Iran Khodro (.+)[,]')
			WHEN split(src.title, ' ')[1] = 'Land' THEN REGEXP_EXTRACT(src.title, r'Продажа Land Rover (.+)[,]')
			ELSE REGEXP_EXTRACT(src.title, r'Продажа [a-zA-Zа-яёА-ЯЁ-]+ (.+)[,]')
		END,
		SAFE_CAST(REGEXP_EXTRACT(src.description, r'^(\d{4}) г.,') AS INT),
		SAFE_CAST(REGEXP_EXTRACT(REPLACE(src.price_secondary, ' ', ''), r'≈(\d+)\$') AS INT),
		REGEXP_EXTRACT(src.description, r',\s(\S+),.+'),
		SAFE_CAST(REPLACE(REGEXP_EXTRACT(src.description, r',\s(\d+[ ]?\d*) км'), " ", "") AS INT),
		REGEXP_EXTRACT(src.description, r'\| ([А-Яа-я0-9. ]+)'),
		SAFE_CAST(REGEXP_EXTRACT(REPLACE(split(src.description, ',')[2], '.', ''), r'[0-9]+') AS INT) * 100,
		CASE
			WHEN INSTR(src.description, 'Запас хода') <> 0 THEN 'Electric'
			ELSE split(src.description, ',')[3]	
		END,
		CASE
			WHEN INSTR(src.exchange, 'Возможен обмен') <> 0 THEN 'Y'
			WHEN INSTR(src.exchange, 'Возможен обмен с моей доплатой') <> 0 THEN 'Y'
			WHEN INSTR(src.exchange, 'Возможен обмен с вашей доплатой') <> 0 THEN 'Y'
			WHEN INSTR(src.exchange, 'Обмен не интересует') <> 0 THEN 'N'
			ELSE 'N'
		END,
		CASE 
			WHEN INSTR(src.labels, 'TOP') <> 0 THEN 'Y'
			ELSE 'N'
		END,
		CASE
			WHEN INSTR(src.labels, 'VIN') <> 0 THEN 'Y'
			ELSE 'N'
		END,
		CASE
			WHEN INSTR(src.labels, 'аварийный') <> 0 THEN 'Y'
			ELSE 'N'
		END,
		CASE
			WHEN INSTR(src.labels, 'на запчасти') <> 0 THEN 'Y'
			ELSE 'N'
		END,
		CASE 
			WHEN INSTR(src.location, ',') <> 0 THEN TRIM(split(src.location, ',')[0])
			WHEN src.location IS NULL THEN ''
			ELSE src.location
		END,
		CASE 
			WHEN INSTR(src.location, ',') <> 0 THEN TRIM(split(src.location, ',')[1])
			ELSE ''
		END,
		CASE 
			WHEN src.comment IS NULL THEN ''
			ELSE src.comment
		END,
		src.scrap_date,
		CURRENT_TIMESTAMP(),
		"N"
	FROM row_for_insert AS src;
	
	--get quantity of rows which have been inserted
	SET insert_row_count = @@row_count;

	CALL `paid-project-346208`.`meta_ds`.`usp_write_event_log`(process_id, "Transform & Load data", "end");

	SET metrics = (NULL, insert_row_count, NULL, NULL, NULL);
	CALL `paid-project-346208`.`meta_ds`.`usp_write_process_log`("END",process_id,"usp_landing_staging1_av_by_card_tokenized_full_merge", metrics);

END;
