IF OBJECT_ID('Landing.dbo.lnd_ads_archive', 'U') IS NOT NULL
DROP TABLE Landing.dbo.lnd_ads_archive

CREATE TABLE Landing.dbo.lnd_ads_archive (
	ads_id int NOT NULL,
	source_id nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	card_url nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	card_compressed varbinary(MAX) NULL,
	process_log_id int NOT NULL,
	source_date datetime NOT NULL,
	insert_date datetime DEFAULT getdate() NOT NULL
);