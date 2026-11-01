# Power BI model and report

Use the Azure Synapse Analytics SQL connector with `<workspace>-ondemand.sql.azuresynapse.net`
and the serving user database (e.g. mobility_dev_serving). Authenticate with the approved Microsoft
Entra organizational account/OAuth identity. Grant that report identity SQL SELECT on the Gold schema
and Storage Blob Data Reader on the Gold container, because views use Entra pass-through. Start with
Import for development; assess DirectQuery scan cost before enabling it. No dedicated SQL pool is used.
Power BI Desktop authors/publishes the report; this repo provides model/measures, not a fabricated PBIX.

Import Gold fact and dimensions or `v_trip_performance` for a simple flattened report. In a star model,
use 1:* single-direction relationships from dim_route.route_key, dim_stop.stop_key,
dim_date_time.date_time_key and dim_weather.weather_key. Date/hour and weekday are UTC; service_date
is the provider-local service date. Expose both and label them explicitly. Treat dim_date_time as an
hour dimension; create a separate contiguous date table for DAX time intelligence.

```dax
Observed Events = COUNT(fact_trip_performance[actual_arrival_time])
On-Time % =
DIVIDE(
    COUNTROWS(FILTER(fact_trip_performance, fact_trip_performance[on_time_flag] = TRUE())),
    COUNTROWS(FILTER(fact_trip_performance, NOT ISBLANK(fact_trip_performance[on_time_flag])))
)
Average Delay (min) = AVERAGE(fact_trip_performance[delay_minutes])
P95 Delay (min) =
PERCENTILEX.INC(
    FILTER(fact_trip_performance, NOT ISBLANK(fact_trip_performance[delay_minutes])),
    fact_trip_performance[delay_minutes], 0.95
)
Average Predicted Delay (min) = AVERAGE(fact_trip_performance[predicted_delay_minutes])
```

Format On-Time % as percentage (DAX ratio 0–1; SQL percentage fields are 0–100). Use
`v_trip_cancellation` for trip cancellation rates: SUM(cancelled_trips)/SUM(reported_trips).
Do not average percentages across unequal groups. Use `v_weather_impact` with sample counts rather
than recomputing ratios from already aggregated averages. Keep unknown observations visible as coverage.
GPS distance/vehicle_count repeat per stop: aggregate once per source + trip + service date before totals.

Pages: reliability overview with observed/predicted coverage; route/stop drilldown; UTC hour/weekday heatmap;
weather association with sample sizes; cancellation and feed freshness/quality. Add route, stop, service
date, weather and observation-kind slicers. Schedule refresh only after a successful Gold build, and
rebind connections per environment. Capture screenshots only from a real connected report.
