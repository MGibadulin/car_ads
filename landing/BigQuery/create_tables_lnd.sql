--create tables for test
CREATE TABLE IF NOT EXISTS `paid-project-346208`.`car_ads_ds_landing`.`lnd_cars-av-by_card_300`
AS SELECT
	card_id,
	title,
	price_primary,
	price_secondary,
	location, labels,
	comment,
	description,
	exchange,
	scrap_date
FROM `paid-project-346208`.`car_ads_ds_landing`.`lnd_cars-av-by_card`
LIMIT 300;

CREATE TABLE IF NOT EXISTS `paid-project-346208`.`car_ads_ds_landing`.`lnd_cars-av-by_card_gallery_300`
AS SELECT
	card_id,
	ind, url,
	scrap_date
FROM `paid-project-346208`.`car_ads_ds_landing`.`lnd_cars-av-by_card_gallery`
LIMIT 300;

CREATE TABLE IF NOT EXISTS `paid-project-346208`.`car_ads_ds_landing`.`lnd_cars-av-by_card_options_300`
AS SELECT
	card_id, 
	category, 
	item, 
	scrap_date
FROM `paid-project-346208`.`car_ads_ds_landing`.`lnd_cars-av-by_card_options`
LIMIT 300;

CREATE TABLE IF NOT EXISTS `paid-project-346208`.`car_ads_ds_landing`.`lnd_cars-av-by_card_url_300`
AS SELECT
	card_id, 
	url, 
	scrap_date
FROM `paid-project-346208`.car_ads_ds_landing.`lnd_cars-av-by_card_url`
LIMIT 300;