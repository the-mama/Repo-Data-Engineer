-- Synapse serverless T-SQL, in the serving user database. Every query should return zero rows.
SELECT fact_key,COUNT_BIG(*) n FROM gold.fact_trip_performance GROUP BY fact_key HAVING COUNT_BIG(*)>1;
SELECT f.fact_key FROM gold.fact_trip_performance f
LEFT JOIN gold.dim_route r ON f.route_key=r.route_key
LEFT JOIN gold.dim_stop s ON f.stop_key=s.stop_key
LEFT JOIN gold.dim_date_time d ON f.date_time_key=d.date_time_key
LEFT JOIN gold.dim_weather w ON f.weather_key=w.weather_key
WHERE r.route_key IS NULL OR s.stop_key IS NULL OR d.date_time_key IS NULL
OR (f.weather_key IS NOT NULL AND w.weather_key IS NULL);
SELECT * FROM gold.dim_stop WHERE latitude NOT BETWEEN -90 AND 90 OR longitude NOT BETWEEN -180 AND 180;
SELECT fact_key FROM gold.fact_trip_performance WHERE scheduled_arrival_time IS NULL
OR ABS(delay_minutes)>180 OR ABS(predicted_delay_minutes)>180;
