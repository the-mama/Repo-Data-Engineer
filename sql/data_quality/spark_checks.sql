-- Each query must return zero rows. Configure alert thresholds separately from exploratory queries.
SELECT fact_key,COUNT(*) n FROM {database_prefix}_gold.fact_trip_performance GROUP BY fact_key HAVING n>1;
SELECT f.fact_key FROM {database_prefix}_gold.fact_trip_performance f
LEFT JOIN {database_prefix}_gold.dim_route r USING(route_key)
LEFT JOIN {database_prefix}_gold.dim_stop s USING(stop_key)
LEFT JOIN {database_prefix}_gold.dim_date_time d USING(date_time_key)
LEFT JOIN {database_prefix}_gold.dim_weather w USING(weather_key)
WHERE r.route_key IS NULL OR s.stop_key IS NULL OR d.date_time_key IS NULL
 OR (f.weather_key IS NOT NULL AND w.weather_key IS NULL);
SELECT * FROM {database_prefix}_gold.dim_stop WHERE latitude NOT BETWEEN -90 AND 90 OR longitude NOT BETWEEN -180 AND 180;
SELECT * FROM {database_prefix}_gold.fact_trip_performance
WHERE scheduled_arrival_time IS NULL OR ABS(delay_minutes)>180 OR ABS(predicted_delay_minutes)>180;
SELECT * FROM {database_prefix}_ops.reconciliation WHERE expanded_rows<>valid_rows+quarantined_rows;
SELECT * FROM {database_prefix}_ops.health WHERE age_minutes>30;
-- Bronze -> silver is expansion-aware (ZIP tables, hourly weather and stop updates fan out).
-- Gold has reported-stop-event grain; compare to latest valid updates expanded against static stop_times,
-- rather than asserting equality with raw envelope counts.

SELECT * FROM {database_prefix}_ops.gold_reconciliation WHERE expanded_candidate_rows<>accepted_fact_rows+quarantined_fact_rows;
