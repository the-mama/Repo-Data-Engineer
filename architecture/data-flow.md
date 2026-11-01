# Unified data flow

1. A Synapse schedule/manual trigger runs MobilityMaster, concurrency 1. Lookup reads the JSON source
   configuration from landing/<prefix>/config/pipeline_control.json through the Lake linked service.
2. The control_plan notebook validates configuration, MERGEs it into <prefix>_ops.pipeline_control without
   resetting progress, and returns due active source rows through mssparkutils.notebook.exit. Supported
   UTC cron: every five minutes, hourly, daily 02:00. Force overrides due time but not inactive status.
3. Sequential ForEach passes each row to IngestSource via Execute Pipeline. Optional Web activity reads
   a header secret from Key Vault using the workspace identity. Copy stores response bytes in a unique
   run-ID batch under landing/<prefix>/<target_folder>/<batch_id>/response.bin.
4. TransformSource runs local Bronze and Silver notebooks. Bronze decodes JSON/ZIP/protobuf into raw
   Parquet and immutable Delta envelopes. Silver enforces explicit schemas, quarantines invalid records,
   deduplicates and MERGEs valid rows. New fields remain in raw Bronze until a reviewed schema migration.
5. control_commit MERGEs last_success_at and a monotonic watermark after Silver. Configuration refresh
   does not reset progress. Historical weather overlaps its last archive date and ends two days behind.
6. After sources, Gold builds versioned dimensions, reported stop-event facts and aggregates. Invalid
   references/schedules/delays quarantine. Observations and predictions remain separate. Gold protocol
   compatibility is checked for serverless SQL. Health checks feed Synapse Monitor/Azure Monitor alerts.
7. Serverless T-SQL views query compatible Gold Delta paths; Power BI connects to the built-in endpoint.

The project has no standalone ADF or transactional SQL control database. Lookup, Web, Copy, ForEach,
Execute Pipeline and notebook activities all execute in Synapse Pipelines. ADLS/Key Vault remain external
platform services by design, not separate orchestration engines.

Replay is serialized with scheduling. Delta atomicity is per table, not a cross-table transaction. A
source watermark commits after Silver; rerun Gold from retained data if only Gold fails. Pause report
refresh during recovery. Raw bytes remain a replay boundary; the per-file decoder limit is 128 MiB.
