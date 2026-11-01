# Validation and acceptance

## Local

Run Ruff, pytest, scripts/validate_repo.py and Bicep compilation as shown in README. The existing unit
suite covers UTC/DST, overnight schedules, dedup, delay, weather, protobuf, validation and sample data.
Notebook checks validate real tagged parameter cells and compile the code inside the imported ipynb files.
Native Synapse artifacts are validated for references/parameters locally and published through the workspace API.

The Java 17 / Spark 3.5.3 / Delta 3.2.1 smoke test uses local explicit database locations, executes Bronze
retry, Silver retry/dedup/quarantine, malformed ZIP recovery, invalid-reference quarantine, Gold MERGE and
reconciliation, and Spark SQL view queries. It asserts protocol reader 1/writer 2 on every Gold table.
The integration also tests Delta control planning, commit and configuration refresh without resetting
watermarks. Local Spark cannot execute serverless T-SQL or test Synapse identity/session/library behavior.

## Azure acceptance before production

1. Deploy Bicep in the selected exact-name environment. Verify Synapse workspace/Spark pool/MI grants;
   no standalone Data Factory, Azure SQL control server, Databricks resource or dedicated SQL pool should be created.
2. Bootstrap Synapse data-plane roles and landing metadata publication. Confirm Synapse Pipelines cannot read unrelated vaults or
   containers or alter unrelated Delta control state. Verify deployment principal artifact/package permissions.
3. Upload wheel/requirements, wait for successful pool installation, import notebooks and inspect tagged
   parameter cells. Run as workspace identity and verify reads/writes across all platform containers.
4. Force Synapse Pipelines master: verify five due feeds, raw bytes/decoded Parquet, Bronze audit fields, six mandatory
   static Silver tables, realtime/weather records, Gold dimensions/facts and successful health checks.
5. Replay same input/batch and corrections: Bronze count stable, Silver keys unique, Gold counts stable.
   Inject malformed data and unknown schema fields; invalid rows quarantine, raw payload retains drift.
6. Confirm protocol details in Spark and query all six Gold Delta tables through **built-in serverless SQL**.
   After a fact MERGE, verify serverless sees only the current row, not old/deleted Parquet versions.
   Test column types, UTC timestamps, nullable booleans and cancellation/observation denominators.
7. Execute T-SQL data_quality/checks.sql in serving database; run spark_checks.sql with prefix substitution
   in Spark. Compare Bronze/Silver fanout, Gold candidate reconciliation and quarantine counts.
8. Trigger API, freshness, quality and scheduler-heartbeat alerts. Confirm receipt and resolution.
9. Connect Power BI with organizational OAuth and Gold-reader RBAC; verify SQL and storage permissions,
   filter behavior, percent scales, null observations and trip-grain cancellation measures.
10. Test prod reviewer enforcement, rollback package version, drain/new-session behavior and restore from
    retained Delta/landing data. Record commit SHA, environment, time and acceptance evidence.
