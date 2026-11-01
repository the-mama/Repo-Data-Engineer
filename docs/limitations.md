# Scope and production gates

- The repository targets Synapse Spark 3.5 and serverless SQL, replacing the earlier Databricks requirement
  at the user's request. Existing deployed Databricks data/resources are not automatically migrated/deleted.
  No cloud resources have been deployed by this work.
- Exact globally unique storage names prevent independent dev/prod lakes using the same name. Choose one
  installation or approve separate names; do not retarget dev resources during production deployment.
- Serverless SQL requires compatible Delta reader protocol 1. Gold uses writer 2, classic checkpoints,
  no deletion vectors and no column mapping. Existing upgraded tables must be rebuilt; changing defaults
  does not downgrade their protocol. Use a fresh prefix or a reviewed data migration for existing tables.
- Cloud acceptance, private networking/DNS, tenant permissions, Synapse data-plane grants, GitHub reviewer
  policies, serverless user-database setup and Power BI publication require environment-specific setup.
- Standard GTFS-Realtime is predictive. Actual-arrival KPIs remain null without a verified observation
  adapter. Absence is not cancellation; reported-trip denominators do not describe all scheduled service.
- Polling misses short-lived events; FULL_DATASET is required. Differential/deletion state is rejected.
  Service date and stop sequence are required for updates in v1; provider adapters must resolve omissions.
- Static snapshot selection is the latest complete snapshot at Gold build time. Effective-dated schedule
  selection is still needed for rigorous historical schedule-change backfills. Calendar/calendar_dates
  are retained but not expanded into every possible scheduled trip/day; calendar_dates-only feeds need an adapter.
- Decoding is bounded per file (128 MiB compressed). Large feeds require a streaming decoder and capacity test.
- Table atomicity is per Delta table; pause reports on failed Gold builds and replay downstream steps.
- Weather is nearest grid/hour within 100 km, with archive preference. Weather impact is associative,
  not causal, and suppresses small groups. Trip distance/vehicle counts repeat across stop facts.
- Spark pool library updates apply to new sessions. Drain ADF before release; use versioned immutable wheels.
- A three-node Spark pool and frequent scheduling can be expensive. Auto-pause cannot help an active workload.
- Optional UK Carbon Intensity enrichment remains deferred.
