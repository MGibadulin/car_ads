CREATE TABLE Landing.dbo.process_log (
	process_log_id int IDENTITY(1,1) NOT NULL,
	process_desc nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[user] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	host nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	connection_id int NULL,
	start_date datetime DEFAULT getdate() NOT NULL,
	end_date datetime NULL,
	CONSTRAINT PK_process_log_id PRIMARY KEY (process_log_id)
);

CREATE TABLE Landing.dbo.event_log (
	event_log_id int IDENTITY(1,1) NOT NULL,
	log_date datetime DEFAULT getdate() NOT NULL,
	event_desc nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	status nvarchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	process_log_id int NOT NULL,
    CONSTRAINT PK_event_log_id PRIMARY KEY (event_log_id)
);

CREATE TABLE Landing.dbo.lnd_ads_archive (
	ads_id int NOT NULL,
	source_id nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	card_url nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	modify_date datetime DEFAULT getdate() NOT NULL,
	card_compressed varbinary(MAX) NULL,
	CONSTRAINT PK_ads_arch_ads_id PRIMARY KEY (ads_id,modify_date,source_num)
);