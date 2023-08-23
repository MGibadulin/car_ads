USE [Landing]
GO

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