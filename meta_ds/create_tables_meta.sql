--Create table for audit
CREATE TABLE IF NOT EXISTS `paid-project-346208`.`meta_ds`.`audit_process_log`
(
	process_log_id	STRING NOT NULL,
	process			STRING NOT NULL,
	start_ts		TIMESTAMP NOT NULL,
	end_ts			TIMESTAMP,
	truncated 		INT64,
	inserted		INT64,
	updated 		INT64,
	mark_as_deleted INT64,
	message 		STRING,
	owner_by 		STRING NOT NULL
);