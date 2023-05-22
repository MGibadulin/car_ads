CREATE OR REPLACE PROCEDURE `paid-project-346208`.`car_ads_ds_staging_test`.usp_audit_start(process STRING, OUT id_row STRING)
BEGIN
	SET id_row = GENERATE_UUID();
	INSERT INTO `paid-project-346208`.`meta_ds`.`audit_log`(id, process, start_ts)
		VALUES(id_row, process, CURRENT_TIMESTAMP());

END;

CREATE OR REPLACE PROCEDURE `paid-project-346208`.`car_ads_ds_staging_test`.usp_audit_end(IN id_row STRING, 
	IN metrics STRUCT <truncated INT64, inserted INT64, updated INT64, mark_as_deleted INT64, message STRING>)
BEGIN
	UPDATE `paid-project-346208`.`meta_ds`.`audit_log`
	SET end_ts = CURRENT_TIMESTAMP(),
		truncated = metrics.truncated,
		inserted = metrics.inserted,
		updated = metrics.updated,
		mark_as_deleted = metrics.mark_as_deleted,
		message = metrics.message
	WHERE id = id_row;
END;