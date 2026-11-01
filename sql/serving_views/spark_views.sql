-- Spark SQL only. Render {database_prefix} before executing.
CREATE OR REPLACE VIEW {database_prefix}_gold.v_trip_performance AS
SELECT f.*, r.route_id, r.route_short_name, r.route_long_name, s.stop_id, s.stop_name,
       d.date, d.hour, d.weekday, w.temperature_2m, w.precipitation, w.weather_kind
FROM {database_prefix}_gold.fact_trip_performance f
JOIN {database_prefix}_gold.dim_route r USING(route_key)
JOIN {database_prefix}_gold.dim_stop s USING(stop_key)
JOIN {database_prefix}_gold.dim_date_time d USING(date_time_key)
LEFT JOIN {database_prefix}_gold.dim_weather w USING(weather_key);

CREATE OR REPLACE VIEW {database_prefix}_gold.v_route_reliability AS
SELECT a.*,r.source_system,r.route_id,r.route_short_name,s.stop_name,d.date,d.hour,d.weekday
FROM {database_prefix}_gold.agg_route_reliability a
JOIN {database_prefix}_gold.dim_route r USING(route_key)
JOIN {database_prefix}_gold.dim_stop s USING(stop_key)
JOIN {database_prefix}_gold.dim_date_time d USING(date_time_key);

CREATE OR REPLACE VIEW {database_prefix}_gold.v_weather_impact AS
WITH groups AS (
 SELECT source_system,route_id,stop_id,hour,weekday,
   AVG(CASE WHEN weather_condition IN ('rain','snow','storm','fog') THEN delay_minutes END) poor_mean,
   AVG(CASE WHEN weather_condition='clear' THEN delay_minutes END) clear_mean,
   COUNT(CASE WHEN weather_condition IN ('rain','snow','storm','fog') THEN delay_minutes END) poor_samples,
   COUNT(CASE WHEN weather_condition='clear' THEN delay_minutes END) clear_samples
 FROM {database_prefix}_gold.v_trip_performance
 GROUP BY source_system,route_id,stop_id,hour,weekday
)
SELECT *, CASE WHEN clear_mean>0 AND poor_samples>=10 AND clear_samples>=10
 THEN poor_mean/clear_mean END weather_impact_index FROM groups;

-- Trip-grain cancellation denominator: reported trips, not every scheduled trip.
CREATE OR REPLACE VIEW {database_prefix}_gold.v_trip_cancellation AS
WITH trips AS (
 SELECT source_system,route_key,service_date,trip_id,
   MAX(CAST(cancellation_flag AS INT)) cancelled
 FROM {database_prefix}_gold.fact_trip_performance GROUP BY source_system,route_key,service_date,trip_id
)
SELECT source_system,route_key,service_date,COUNT(*) reported_trips,
 SUM(cancelled) cancelled_trips,100.0*SUM(cancelled)/COUNT(*) cancellation_rate_pct
FROM trips GROUP BY source_system,route_key,service_date;
