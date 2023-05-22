DROP TABLE `paid-project-346208`.`car_ads_ds_staging_test`.`cars_av_by_card_tokenized`;

CALL `paid-project-346208`.`car_ads_ds_staging_test`.usp_landing_staging1_av_by_card_tokenized_full_load();

