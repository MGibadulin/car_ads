-- Create table for Stage 1
CREATE TABLE IF NOT EXISTS `paid-project-346208`.`car_ads_ds_staging_test`.`cars_av_by_card_tokenized`
(
	row_id			STRING NOT NULL,
	row_hash		BYTES(32) NOT NULL,
	card_id			STRING NOT NULL,
	brand			STRING NOT NULL,
	model			STRING NOT NULL,
	year			INT64 NOT NULL,
	price			INT64 NOT NULL,
	transmission 	STRING NOT NULL,
	mileage			INT64 NOT NULL,
	body			STRING NOT NULL,
	engine_vol		INT64 NOT NULL,
	fuel 			STRING NOT NULL,
	exchange		STRING NOT NULL,
	top				STRING NOT NULL,
	vin				STRING NOT NULL,
	crahed			STRING NOT NULL,
	for_spare		STRING NOT NULL,
	city			STRING NOT NULL,
	region			STRING NOT NULL,
	comment			STRING NOT NULL,
	scrap_date		TIMESTAMP NOT NULL,
	modified_date	TIMESTAMP NOT NULL,
	deleted			STRING NOT NULL,
	oper			STRING NOT NULL,
	PRIMARY KEY(row_id) NOT ENFORCED
);