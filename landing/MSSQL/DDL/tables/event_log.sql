USE [Landing]
GO

CREATE TABLE Landing.dbo.event_log (
	event_log_id int IDENTITY(1,1) NOT NULL,
	log_date datetime DEFAULT getdate() NOT NULL,
	event_desc nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	status nvarchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	process_log_id int NOT NULL,
    CONSTRAINT PK_event_log_id PRIMARY KEY (event_log_id)
);