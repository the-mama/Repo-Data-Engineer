-- Run in a user database on <workspace>-ondemand.sql.azuresynapse.net (never in master).
-- Render ${storage} and ${database_prefix} with scripts/render_serverless_sql.py.
-- Uses Microsoft Entra pass-through: each reader needs Blob Data Reader on the Gold container.
IF SCHEMA_ID('gold') IS NULL EXEC('CREATE SCHEMA gold');
GO
IF NOT EXISTS (SELECT 1 FROM sys.external_data_sources WHERE name = 'mobility_gold')
CREATE EXTERNAL DATA SOURCE mobility_gold
WITH (LOCATION = 'https://${storage}.dfs.core.windows.net/gold/${database_prefix}/gold');
GO
CREATE OR ALTER VIEW gold.dim_route AS
SELECT * FROM OPENROWSET(BULK 'dim_route', DATA_SOURCE='mobility_gold', FORMAT='DELTA')
WITH (
  [route_key] varchar(64),
  [source_system] varchar(100),
  [route_id] varchar(8000),
  [route_short_name] varchar(8000),
  [route_long_name] varchar(8000),
  [batch_id] varchar(100)
) AS rows;
GO
CREATE OR ALTER VIEW gold.dim_stop AS
SELECT * FROM OPENROWSET(BULK 'dim_stop', DATA_SOURCE='mobility_gold', FORMAT='DELTA')
WITH (
  [stop_key] varchar(64),
  [source_system] varchar(100),
  [stop_id] varchar(8000),
  [stop_name] varchar(8000),
  [latitude] float,
  [longitude] float,
  [batch_id] varchar(100)
) AS rows;
GO
CREATE OR ALTER VIEW gold.dim_date_time AS
SELECT * FROM OPENROWSET(BULK 'dim_date_time', DATA_SOURCE='mobility_gold', FORMAT='DELTA')
WITH (
  [date_time_key] bigint,
  [hour_utc] datetime2(6),
  [date] date,
  [hour] int,
  [weekday] varchar(20)
) AS rows;
GO
CREATE OR ALTER VIEW gold.dim_weather AS
SELECT * FROM OPENROWSET(BULK 'dim_weather', DATA_SOURCE='mobility_gold', FORMAT='DELTA')
WITH (
  [weather_key] varchar(64),
  [location_id] varchar(100),
  [hour_utc] datetime2(6),
  [weather_condition] varchar(20),
  [temperature_2m] float,
  [precipitation] float,
  [weather_kind] varchar(20)
) AS rows;
GO
CREATE OR ALTER VIEW gold.fact_trip_performance AS
SELECT * FROM OPENROWSET(BULK 'fact_trip_performance', DATA_SOURCE='mobility_gold', FORMAT='DELTA')
WITH (
  [fact_key] varchar(64),
  [source_system] varchar(100),
  [trip_id] varchar(8000),
  [service_date] varchar(8),
  [stop_sequence] varchar(30),
  [route_key] varchar(64),
  [stop_key] varchar(64),
  [date_time_key] bigint,
  [weather_key] varchar(64),
  [scheduled_arrival_time] datetime2(6),
  [actual_arrival_time] datetime2(6),
  [predicted_arrival_time] datetime2(6),
  [delay_minutes] float,
  [predicted_delay_minutes] float,
  [cancellation_flag] bit,
  [skipped_flag] bit,
  [vehicle_count] bigint,
  [distance_travelled_km] float,
  [weather_condition] varchar(20),
  [on_time_flag] bit,
  [arrival_time_kind] varchar(20),
  [timestamp] datetime2(6)
) AS rows;
GO
CREATE OR ALTER VIEW gold.agg_route_reliability AS
SELECT * FROM OPENROWSET(BULK 'agg_route_reliability', DATA_SOURCE='mobility_gold', FORMAT='DELTA')
WITH (
  [route_key] varchar(64),
  [stop_key] varchar(64),
  [date_time_key] bigint,
  [weather_condition] varchar(20),
  [reported_stop_events] bigint,
  [observed_stop_events] bigint,
  [on_time_performance_pct] float,
  [average_delay_minutes] float,
  [p95_delay_minutes] float,
  [average_predicted_delay_minutes] float,
  [cancellation_rate_pct] float
) AS rows;
GO

CREATE OR ALTER VIEW gold.v_trip_performance AS
SELECT f.*, r.route_id, r.route_short_name, r.route_long_name, s.stop_id, s.stop_name,
 d.[date], d.[hour], d.weekday, w.temperature_2m, w.precipitation, w.weather_kind
FROM gold.fact_trip_performance f
JOIN gold.dim_route r ON f.route_key=r.route_key
JOIN gold.dim_stop s ON f.stop_key=s.stop_key
JOIN gold.dim_date_time d ON f.date_time_key=d.date_time_key
LEFT JOIN gold.dim_weather w ON f.weather_key=w.weather_key;
GO
CREATE OR ALTER VIEW gold.v_route_reliability AS
SELECT a.*,r.source_system,r.route_id,r.route_short_name,s.stop_name,d.[date],d.[hour],d.weekday
FROM gold.agg_route_reliability a
JOIN gold.dim_route r ON a.route_key=r.route_key
JOIN gold.dim_stop s ON a.stop_key=s.stop_key
JOIN gold.dim_date_time d ON a.date_time_key=d.date_time_key;
GO
CREATE OR ALTER VIEW gold.v_weather_impact AS
WITH measures AS (
 SELECT source_system,route_id,stop_id,[hour],weekday,
 AVG(CASE WHEN weather_condition IN ('rain','snow','storm','fog') THEN delay_minutes END) poor_mean,
 AVG(CASE WHEN weather_condition='clear' THEN delay_minutes END) clear_mean,
 COUNT(CASE WHEN weather_condition IN ('rain','snow','storm','fog') THEN delay_minutes END) poor_samples,
 COUNT(CASE WHEN weather_condition='clear' THEN delay_minutes END) clear_samples
 FROM gold.v_trip_performance GROUP BY source_system,route_id,stop_id,[hour],weekday
)
SELECT *,CASE WHEN clear_mean>0 AND poor_samples>=10 AND clear_samples>=10
 THEN poor_mean/NULLIF(clear_mean,0) END weather_impact_index FROM measures;
GO
CREATE OR ALTER VIEW gold.v_trip_cancellation AS
WITH trips AS (
 SELECT source_system,route_key,service_date,trip_id,MAX(CAST(cancellation_flag AS int)) cancelled
 FROM gold.fact_trip_performance GROUP BY source_system,route_key,service_date,trip_id
)
SELECT source_system,route_key,service_date,COUNT_BIG(*) reported_trips,SUM(cancelled) cancelled_trips,
100.0*SUM(cancelled)/COUNT_BIG(*) cancellation_rate_pct
FROM trips GROUP BY source_system,route_key,service_date;
GO
