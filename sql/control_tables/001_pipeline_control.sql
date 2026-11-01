-- Azure SQL Database; execute as Entra administrator. Re-runnable migration.
IF OBJECT_ID('dbo.pipeline_control') IS NULL
CREATE TABLE dbo.pipeline_control (
 source_name nvarchar(100) NOT NULL,
 entity_name nvarchar(100) NOT NULL,
 api_url nvarchar(2048) NOT NULL,
 key_vault_secret_name nvarchar(127) NULL,
 ingestion_type varchar(30) NOT NULL,
 watermark_column nvarchar(100) NULL,
 last_watermark_value nvarchar(100) NULL,
 target_folder nvarchar(300) NOT NULL,
 schedule_cron varchar(50) NOT NULL,
 is_active bit NOT NULL DEFAULT 1,
 created_datetime datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
 modified_datetime datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
 content_format varchar(20) NOT NULL,
 weather_kind varchar(20) NOT NULL DEFAULT 'forecast',
 last_success_at datetime2 NULL,
 CONSTRAINT PK_pipeline_control PRIMARY KEY (source_name,entity_name),
 CONSTRAINT CK_url_https CHECK (api_url LIKE 'https://%'),
 CONSTRAINT CK_schedule CHECK (schedule_cron IN ('*/5 * * * *','0 * * * *','0 2 * * *')),
 CONSTRAINT CK_ingestion CHECK (ingestion_type IN ('snapshot','incremental')),
 CONSTRAINT CK_format CHECK (content_format IN ('json','protobuf','gtfs_zip'))
);
GO
CREATE OR ALTER PROCEDURE dbo.get_due_sources @force bit = 0 AS
BEGIN
 SET NOCOUNT ON;
 SELECT source_name,entity_name,
   REPLACE(REPLACE(api_url,'{start_date}',COALESCE(LEFT(last_watermark_value,10),CONVERT(varchar(10),DATEADD(day,-7,SYSUTCDATETIME()),23))),
    '{end_date}',CONVERT(varchar(10),DATEADD(day,-2,SYSUTCDATETIME()),23)) api_url,
   key_vault_secret_name,ingestion_type,watermark_column,last_watermark_value,target_folder,
   schedule_cron,content_format,weather_kind
 FROM dbo.pipeline_control
 WHERE is_active=1 AND (@force=1 OR last_success_at IS NULL OR
 (schedule_cron='*/5 * * * *' AND DATEDIFF(minute,last_success_at,SYSUTCDATETIME())>=5) OR
 (schedule_cron='0 * * * *' AND DATEDIFF(hour,last_success_at,SYSUTCDATETIME())>=1) OR
 (schedule_cron='0 2 * * *' AND CONVERT(date,last_success_at)<CONVERT(date,SYSUTCDATETIME()) AND DATEPART(hour,SYSUTCDATETIME())>=2));
END;
GO
CREATE OR ALTER PROCEDURE dbo.mark_source_success
 @source_name nvarchar(100), @entity_name nvarchar(100), @watermark nvarchar(100) AS
BEGIN
 SET NOCOUNT ON;
 UPDATE dbo.pipeline_control SET last_success_at=SYSUTCDATETIME(),
 last_watermark_value=CASE WHEN last_watermark_value IS NULL OR last_watermark_value < @watermark THEN @watermark ELSE last_watermark_value END,
 modified_datetime=SYSUTCDATETIME()
 WHERE source_name=@source_name AND entity_name=@entity_name;
END;
GO
-- Grant only the control procedure surface to ADF; use scripts/bootstrap_sql.py.
