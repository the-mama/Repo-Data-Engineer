-- sqlcmd variables are nonsecret deployment configuration. Use a consistent provider for ALL GTFS feeds.
-- Example public provider: MBTA (US); configure Boston weather and America/New_York agency timezone.
MERGE dbo.pipeline_control AS t
USING (VALUES
 ('mbta','static','https://cdn.mbta.com/MBTA_GTFS.zip',NULL,'snapshot','gtfs_zip','0 2 * * *','forecast'),
 ('mbta','vehicle_positions','https://cdn.mbta.com/realtime/VehiclePositions.pb',NULL,'snapshot','protobuf','*/5 * * * *','forecast'),
 ('mbta','trip_updates','https://cdn.mbta.com/realtime/TripUpdates.pb',NULL,'snapshot','protobuf','*/5 * * * *','forecast'),
 ('boston_weather','weather','https://api.open-meteo.com/v1/forecast?latitude=42.36&longitude=-71.06&hourly=temperature_2m,precipitation,weather_code&timezone=UTC',NULL,'snapshot','json','0 * * * *','forecast'),
 ('boston_history','weather','https://archive-api.open-meteo.com/v1/archive?latitude=42.36&longitude=-71.06&start_date={start_date}&end_date={end_date}&hourly=temperature_2m,precipitation,weather_code&timezone=UTC',NULL,'incremental','json','0 2 * * *','historical')
) s(source_name,entity_name,api_url,key_vault_secret_name,ingestion_type,content_format,schedule_cron,weather_kind)
ON t.source_name=s.source_name AND t.entity_name=s.entity_name
WHEN NOT MATCHED THEN INSERT(source_name,entity_name,api_url,key_vault_secret_name,ingestion_type,
 watermark_column,target_folder,schedule_cron,is_active,content_format,weather_kind)
VALUES(s.source_name,s.entity_name,s.api_url,s.key_vault_secret_name,s.ingestion_type,
 'timestamp',CONCAT(s.source_name,'/',s.entity_name),s.schedule_cron,1,s.content_format,s.weather_kind);
GO
