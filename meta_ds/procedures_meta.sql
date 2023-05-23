CREATE OR REPLACE PROCEDURE `paid-project-346208`.`car_ads_ds_staging_test`.usp_audit_start(process STRING, OUT id_row STRING)
BEGIN
	SET id_row = GENERATE_UUID();
	INSERT INTO `paid-project-346208`.`meta_ds`.`audit_log`(id, process, start_ts)
		VALUES(id_row, process, CURRENT_TIMESTAMP());

END;

CREATE OR REPLACE PROCEDURE `paid-project-346208`.`car_ads_ds_staging_test`.usp_audit_end(IN id_row STRING, 
	IN metrics STRUCT <truncated INT64, inserted INT64, updated INT64, mark_as_deleted INT64, message STRING>
)
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

CREATE OR REPLACE PROCEDURE `paid-project-346208`.`meta_ds`.`usp_write_process_log`(
	IN type_action STRING, 
	INOUT task_id STRING, 
	IN process_name STRING,
	IN metrics STRUCT <truncated INT64, inserted INT64, updated INT64, mark_as_deleted INT64, message STRING>
)
BEGIN
	IF type_action = "START"
		THEN
			SET task_id = GENERATE_UUID();
			INSERT INTO `paid-project-346208`.`meta_ds`.`audit_process_log`(process_log_id, process, start_ts, owner_by)
			VALUES(task_id, process_name, CURRENT_TIMESTAMP(), SESSION_USER());
	END IF;
    IF type_action = "END"
        THEN
            UPDATE `paid-project-346208`.`meta_ds`.`audit_process_log`
                SET end_ts = CURRENT_TIMESTAMP(),
                truncated = metrics.truncated,
                inserted = metrics.inserted,
                updated = metrics.updated,
                mark_as_deleted = metrics.mark_as_deleted,
                message = metrics.message
            WHERE process_log_id = task_id;
    END IF;

END;
