-- test deduplication
WITH t1 AS
(
	SELECT
		card_id,
		title,
		price_primary,
		location,
		labels,
		comment,
		description,
		exchange,
		scrap_date
		ROW_NUMBER() OVER(PARTITION BY card_id ORDER BY scrap_date DESC) AS rn
	FROM `paid-project-346208`.`car_ads_ds_landing`.`lnd_cars-av-by_card_300`
)
SELECT card_id, title, description, scrap_date
FROM t1
WHERE t1.rn = 1;

SELECT SHA256(CONCAT(t1.title, t1.price_secondary, t1.location, t1.labels, t1.comment, t1.description, t1.exchange)) AS row_hash
FROM `paid-project-346208`.`car_ads_ds_landing`.`lnd_cars-av-by_card_300` AS t1;

UPDATE `paid-project-346208`.`car_ads_ds_landing`.`lnd_cars-av-by_card_300`
SET price_secondary = "≈ 153 $"
WHERE card_id = 104316191;

UPDATE `paid-project-346208`.`car_ads_ds_landing`.`lnd_cars-av-by_card_300`
SET scrap_date = CAST("2023-04-18 21:43:33.000" AS TIMESTAMP)
WHERE card_id = 104316191;