# Getting comfortable with Synapse

There are two different compute engines in this project. **Spark** runs Python and writes Delta tables.
**Serverless SQL** reads Gold files using T-SQL for Power BI. You do not need a dedicated SQL pool.
Data Factory remains the scheduler, so the ingestion screen is still in ADF Studio.

After the README deployment steps:

1. Open the Azure Synapse workspace in the Azure portal and select **Open Synapse Studio**.
2. In **Manage → Apache Spark pools**, find `mobilityspark`. Check that its runtime is 3.5,
   auto-pause is enabled, and the mobility wheel/library installation has succeeded.
3. In **Develop → Notebooks → mobility**, open `mobility_bronze`, `mobility_silver`, `mobility_gold`
   or `mobility_health`. The first cell has tagged parameter defaults; ADF supplies actual values.
   To test manually, set a real retained `landing_file`, source, format and batch ID. Run Bronze,
   then Silver, then Gold after a complete static/realtime/weather bootstrap exists.
4. Select **Run as managed identity** when manually executing notebooks, and use the configured pool.
   Your user needs Synapse Compute Operator and Synapse Credential User for the workspace identity.
   Pipeline notebook executions use the workspace identity. An ordinary interactive session can instead
   use your own identity; in that case your user also needs storage RBAC.
5. Under **Data → Workspace → Lake database**, inspect databases such as `mobility_dev_bronze`,
   `mobility_dev_silver`, `mobility_dev_gold` and `mobility_dev_ops`. In a Spark notebook:

```python
spark.table("mobility_dev_gold.fact_trip_performance").show(truncate=False)
spark.table("mobility_dev_ops.reconciliation").show(truncate=False)
```

6. Open a **SQL script**, choose **Built-in** as the connection and run in `master` once:

```sql
CREATE DATABASE mobility_dev_serving;
```

   Then switch the script database to `mobility_dev_serving`. Execute the rendered
   `.local/serverless_views.sql` file. This is a SQL serving database, separate from Spark's databases.
   Query `gold.v_trip_performance`. Do not execute Spark SQL files in the built-in SQL editor.
7. In **Monitor**, inspect Spark applications and session logs. In **ADF Studio → Monitor**, inspect
   `MobilityMaster`, its child pipelines and notebook activity outputs. For bad records, inspect Delta
   quarantine under the configured ADLS quarantine container or the Spark ops tables.
8. When finished learning, stop the ADF trigger. Spark auto-pause helps only after sessions become idle;
   five-minute scheduled workloads can keep a pool active and incur continuous charges.

First troubleshooting checks: correct user/database/SQL connection, completed pool library installation,
storage RBAC for the execution identity, and a complete static schedule load before Gold.
