# Data contracts — version 1

## Common rules

IDs are strings (leading zeroes preserved), trimmed without case folding. Every key is namespaced by
`source_system`; surrogate keys are SHA-256 of canonical component arrays. Timestamps are UTC. A naive
non-UTC local time in a DST overlap/gap is rejected; GTFS schedules use service-day local noon minus
12 elapsed hours, allowing `25:10:00`. Unknown source fields stay in immutable Bronze `payload` and
require a reviewed Silver schema migration before being promoted. No automatic schema evolution.

Bronze `records`: one decoded source record per entity, append-only Delta. Mandatory audit fields:
`ingestion_timestamp` timestamp, `batch_id`, `source_system`, `source_url`, `source_file_name`,
`record_hash` SHA-256 and `load_status` (`loaded` or `decode_failed`). Additional fields: `entity_name`,
`payload` JSON string, `weather_kind`. Decoded landing Parquet has the same envelope and unmodified
source record values. API secrets must be headers, never query parameters in persisted `source_url`.

Silver tables share an explicit nullable field superset, but validate mandatory fields by entity.
Keys below also include source; static keys additionally include `batch_id` to preserve snapshots.
Coordinates are numeric in [-90,90] / [-180,180]. Event timestamps must be >=2000 and at most 17 days
ahead of processing time (to accommodate forecasts). Delay thresholds default to ±180 minutes.

| Source / Silver entity | Grain and mandatory key | Payload and normalization | Freshness / delivery |
|---|---|---|---|
| GTFS `agency` | agency_id | agency_name, agency_url, agency_timezone (IANA); single omitted agency_id → `default` | Daily static ZIP snapshot |
| GTFS `routes` | route_id | agency_id optional for one agency, route_short_name, route_long_name, route_type | Same complete static batch |
| GTFS `stops` | stop_id | stop_name, stop_lat/stop_lon → latitude/longitude | Same static batch |
| GTFS `trips` | trip_id | route_id, service_id, trip_headsign, agency inferred from route | Same static batch |
| GTFS `stop_times` | trip_id + stop_sequence | stop_id, arrival_time, departure_time; hours may exceed 24 | Same static batch |
| GTFS `calendar` | service_id | weekday flags, start_date/end_date YYYYMMDD | Same static batch |
| GTFS `calendar_dates` (optional) | service_id + date | exception_type; retained for future schedule-coverage expansion | Same static ZIP |
| GTFS realtime `vehicle_positions` | trip_id + vehicle_id + timestamp | route_id, service_date YYYYMMDD, latitude/longitude | Poll 5 min; source event age <=30 min |
| GTFS realtime `trip_updates` | trip_id + service_date + stop_sequence + timestamp | route_id, stop_id, reported_arrival_time UTC, reported_delay_seconds; cancellation/skipped flags | FULL_DATASET only; poll 5 min |
| Open-Meteo `weather` | location_id + timestamp + weather_kind | location rounded to 0.001°, hourly temperature_2m °C, precipitation mm, WMO weather_code; time requested UTC | Forecast hourly; archive daily with 2-day lag |

GTFS-Realtime protobuf is decoded using official bindings; canonical camelCase JSON is also accepted.
Service date and stop sequence are required for update matching in v1. Differential feeds and deleted
entities are rejected explicitly rather than losing state. Missing required keys, malformed values and
out-of-range fields are quarantined. CANCELED creates a wildcard stop row, expanded against the schedule.
A skipped stop is distinct from a cancelled trip. An absent event does not mean cancellation.

## Gold contracts

| Table | Key / grain | Required relationships and measures |
|---|---|---|
| `dim_route` | route_key; source + route_id + static batch | route_id, names, source_system, batch_id; snapshot versions retained |
| `dim_stop` | stop_key; source + stop_id + static batch | stop_id, name, latitude/longitude, source_system, batch_id |
| `dim_date_time` | date_time_key UTC YYYYMMDDHH | hour_utc, date, hour 0–23, weekday; UTC weekday, not local service weekday |
| `dim_weather` | weather_key; location + UTC hour | weather_condition, temperature_2m, precipitation, weather_kind; archive preferred to forecast |
| `fact_trip_performance` | fact_key; source + trip_id + service_date + stop_sequence | route_key, stop_key, date_time_key required; weather_key nullable; measures below |
| `agg_route_reliability` | route_key + stop_key + date_time_key + weather_condition | reported/observed stop event counts, on_time_performance_pct, average_delay_minutes, p95_delay_minutes, average_predicted_delay_minutes, cancellation_rate_pct at stop grain |

Fact measures: `scheduled_arrival_time` required; `actual_arrival_time` observed-only nullable;
`predicted_arrival_time` prediction-only nullable; `delay_minutes=(actual-scheduled)/60`;
`predicted_delay_minutes=(prediction-scheduled)/60`; explicit `cancellation_flag`; separate `skipped_flag`;
`vehicle_count` distinct vehicles and `distance_travelled_km` GPS path length at **trip** grain repeated
on stop rows; `weather_condition` defaults `unknown`; `on_time_flag` is null when unknown/cancelled/skipped,
otherwise true when observed delay is within [-1,+5]. `arrival_time_kind` preserves prediction/observation provenance.

Do not sum repeated trip distance or vehicle_count across stops. On-time denominator is non-null
on_time_flag. P95 and average observed delay ignore nulls. Trip cancellation rate uses distinct reported
trip instances via `v_trip_cancellation`; stop-grain aggregates expose reported stop cancellation rate.
Weather impact is poor-weather mean observed delay / clear-weather mean at route, stop, UTC hour and
weekday, requiring >=10 observations per group and a positive clear-weather baseline; otherwise NULL.
It does not establish causation or adjust for all service-selection effects.

## Operational contracts

Quarantine path: `abfss://quarantine@<storage>.dfs.core.windows.net/<database_prefix>/<entity>/`.
Rows include original payload or failed fact fields, `failure_reason`, `failed_at`, source and stable keys.
Reconciliation grain: source + batch + entity, with input distinct hashes, expanded_rows, valid_rows,
quarantined_rows, checked_at. Duplicate suppression explains valid-output versus current-table row differences.
Malformed ZIP/protobuf responses retain their bytes in landing and emit a decode_failed envelope.
A file missing a mandatory static table cannot become a complete Gold static snapshot.

Changing keys, units, observation semantics or required fields requires a contract version and migration.
Do not replace a source's static and realtime provider independently: natural IDs are only consistent within
one provider's feed family. Optional carbon enrichment is intentionally outside v1 (not required for KPIs).

## Synapse namespace and storage

The Synapse pipeline/notebook parameter is `database_prefix` (e.g. mobility_dev), not a Unity Catalog catalog.
Spark tables use two-part names: mobility_dev_bronze.records, mobility_dev_silver.trips,
mobility_dev_gold.fact_trip_performance and mobility_dev_ops.reconciliation. Each database has an
explicit ADLS LOCATION: `<container>/<database_prefix>/<layer>/<table>`. Ops uses the Gold container.
Serverless SQL uses a separate user database and explicit OPENROWSET(FORMAT='DELTA') T-SQL views in
schema gold. Spark SQL and serverless T-SQL are separate dialects. Gold protocol is reader 1/writer 2.

## Delta control contract

`<prefix>_ops.pipeline_control` has the original source/entity/API/secret-name/ingestion/watermark/
target/schedule/active/created/modified fields, plus content_format, weather_kind, last_success_at and
config_hash. Key: source_name + entity_name. Config JSON is the sole desired configuration; MERGE
preserves progress, deactivates removed sources and never resets watermarks. Updates happen through
Synapse Spark, not serverless SQL or an external transactional SQL database.
