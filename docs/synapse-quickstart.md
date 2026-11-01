# One workspace: where to look

Open your Azure Synapse workspace in Azure Portal, then **Open Synapse Studio**.

1. **Integrate → Pipelines**: MobilityMaster is the entry point. IngestSource and TransformSource are its
   child pipelines. Open the master to see Lookup → control planning → ForEach → Gold → health.
2. **Manage → Linked services**: Lake, Http and KeyVault connect sources/storage using the workspace
   managed identity. There is no cross-workspace Synapse linked service and no control SQL connection.
3. **Develop → Notebooks → mobility**: Bronze, Silver, Gold, health, control_plan and control_commit.
   Their first tagged cell contains defaults; pipeline activities supply actual values. Use mobilityspark.
4. **Data → Linked → ADLS**: landing/mobility_dev/config/pipeline_control.json is the editable source
   configuration. Publish changes through GitHub/deploy scripts. Secrets never belong in this file.
5. **Data → Workspace / Lake databases**: inspect mobility_dev_bronze/silver/gold/ops. In Spark:

```python
spark.table("mobility_dev_ops.pipeline_control").show(truncate=False)
spark.table("mobility_dev_gold.fact_trip_performance").show(truncate=False)
```

Two optional utility pipelines help with learning: **ConnectionCheck** tests Lookup/Copy without Spark;
**ControlPreview** starts Spark to initialize/preview due control rows without fetching all transit feeds.

6. **Integrate → MobilityMaster → Add trigger → Trigger now**: force=true loads every active source.
   This starts billable Spark/API work. For low-cost learning, keep EveryFiveMinutes stopped and run
   manual batches. Monitor the run before enabling any schedule.
7. **Monitor → Pipeline runs / Activity runs / Apache Spark applications**: investigate Copy, notebook,
   validation and freshness failures. Invalid rows go to Delta quarantine; their reasons are retained.
8. **Develop → SQL script**, connection **Built-in**: as the configured Entra SQL admin, create a user
   database once, select it, and run the rendered serverless views. Spark databases and this SQL user
   database are separate namespaces; do not paste Spark SQL into the built-in editor.

```sql
CREATE DATABASE mobility_dev_serving; -- run once as SQL admin in master, then switch database
```

9. Connect Power BI to `<workspace>-ondemand.sql.azuresynapse.net` and the serving database. Give the
   report identity SQL SELECT and Gold-container Blob Data Reader.
10. End notebook sessions and stop the scheduler after learning. Spark auto-pause works only while idle.

Manual notebook execution can use your user identity. Choose **Run as managed identity** to use the
workspace's configured storage access; your user needs Compute Operator and Credential User permissions.
Pipeline executions use the workspace identity. Portal/Studio access does not alone grant SQL/data access.
