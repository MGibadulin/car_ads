SET NOCOUNT ON;
CREATE TABLE #test (
	ads_id int NOT NULL,
	source_id nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	card_url nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	ad_group_id int NOT NULL,
	process_log_id int NOT NULL,
	modify_date datetime DEFAULT getdate() NOT NULL,
	ad_status smallint NOT NULL,
	card_compressed varbinary(MAX) NULL,
	source_num tinyint NULL
);

INSERT INTO #test
(
    ads_id,
	source_id,
	card_url,
	ad_group_id,
	process_log_id,
	ad_status,
	card_compressed,
	source_num
)
SELECT TOP(100)
    ads_id,
	source_id,
	card_url,
	ad_group_id,
	process_log_id,
	ad_status,
	card_compressed,
	source_num
FROM Landing.dbo.ads_archive_test
WHERE ad_status = 2;

INSERT INTO  Landing.dbo.ads_archive_test
SELECT *
FROM #test;