# Data flow

1. A UTC five-minute ADF trigger calls `MobilityMaster` (concurrency 1). Lookup executes
   `dbo.get_due_sources`, returning active due metadata. Supported UTC cron schedules are every
   five minutes, hourly and daily at 02:00. Unsupported schedules are rejected by SQL constraints.
   A `force` parameter permits a manual full refresh. Cron is interpreted by the SQL dispatcher,
   not by creating one trigger per row.
2. A sequential ForEach passes each metadata object to `IngestSource` through Execute Pipeline.
   This bounds API/cluster concurrency and prevents simultaneous table creation. Optional Web activity
   reads an HTTP header block from Key Vault using ADF's managed identity. Secure input/output hides
   the header and authenticated copy configuration from activity logs.
3. Copy uses parameterized HTTP Binary and ADLS Binary datasets. The unique factory run ID identifies
   the batch: `landing/<database_prefix>/<target_folder>/<batch_id>/response.bin`. A copy has no automatic
   retry so an uncertain HTTP request is not silently overwritten. Rerun the master for a new batch;
   use the replay procedure for an existing landing file.
4. `TransformSource` runs bronze then silver notebooks. Bronze decodes ZIP CSV, protobuf or JSON
   into raw JSON envelopes, stores decoded raw Parquet beside the response, then appends Delta audit
   envelopes. Its Delta transaction ID makes a same-batch retry a no-op. API bytes remain untouched.
5. Silver parses explicit contracts, validates mandatory keys/ranges and writes malformed records to
   Delta quarantine with `failure_reason` and `failed_at`. Unknown fields remain in bronze. Valid rows
   MERGE by source + entity keys; static rows also include batch, preserving complete schedule snapshots.
   A correction wins by ingestion timestamp and record hash. Reconciliation records expanded rows,
   valid rows, quarantined rows and distinct input hashes.
6. Source success/watermark commits only after Silver completes. Forecast and GTFS snapshots are full
   refresh polls. Historical weather uses `{start_date}` and `{end_date}` URL tokens, a two-day archive
   lag and overlapping end-day watermark; MERGE safely handles overlap. Gold failure does not roll back
   source watermarks: rerun Gold from retained Silver rather than fetching APIs again.
7. After all due sources complete, Gold joins valid latest trip updates with a complete static snapshot,
   calculates UTC schedules, dimensions, arrival measures, GPS distance and weather. Invalid references
   and excessive delays go to fact quarantine. Facts MERGE at stop-event grain, followed by aggregate
   table refresh. Synapse serverless SQL views expose descriptive labels and trip-grain cancellation measures.
8. `CheckFreshness` / `CheckQuality` validates telemetry event-time freshness, recent quarantine percentage, reconciliation,
   and Gold uniqueness. ADF failures flow to resource-specific Log Analytics tables and Azure Monitor.
   Monitor alerts use the health activity failure for freshness/DQ, and dedicated Copy/pipeline alerts.

## Replay and recovery boundaries

A master run is serialized; external callers must not run replay concurrently against the same database prefix.
Bronze uses append-only Delta, with a ten-year transaction-id retention window; operational replay beyond
that window requires an input manifest and duplicate audit before execution. Silver and Gold use MERGE;
Gold aggregates are deterministic rebuilds of facts. Tables are individually atomic, not a cross-table
transaction. Pause Power BI refresh while recovering a failed Gold run, then rerun Gold and serving views.

Archive decoding is per input file on Spark workers, with a 128 MiB compressed input limit. Split or replace
this decoder for larger feeds; never increase the limit without checking driver/executor memory and ZIP
expansion size. Landing raw bytes provide a full replay boundary even when decoding fails.

## Synapse execution

ADF uses native SynapseNotebook activities with an AzureSynapseArtifacts MSI linked service.
Notebooks have a tagged parameters cell, pool mobilityspark and the workspace-managed identity storage
context. Wheels are workspace packages attached to the pool; no widget API or workspace bundle is used.
Explicit two-part Spark database names and storage locations replace Unity Catalog. Serverless views
are rendered T-SQL over compatible Gold Delta paths and are not automatically created by Spark views.
