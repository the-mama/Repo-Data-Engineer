# Deployment and operations runbook

## Environment configuration

Use README deployment order: infrastructure → Azure SQL control bootstrap → Synapse execution roles →
workspace wheel/pool/notebooks → ADF → first data load → serverless views → Power BI.
Create protected GitHub environments; configure prod required reviewers explicitly in GitHub settings.
Azure deployment uses OIDC. A Synapse Administrator must grant the runtime and deployment data-plane
roles; Azure resource Contributor alone does not grant Synapse artifact permissions.

| GitHub/environment variable | Value/source |
|---|---|
| AZURE_CLIENT_ID, AZURE_TENANT_ID, AZURE_SUBSCRIPTION_ID | Azure OIDC deployment principal and tenant/subscription |
| RESOURCE_GROUP | Existing resource group |
| SQL_ADMIN_OBJECT_ID, SQL_ADMIN_LOGIN | Entra SQL administrator group ID/name |
| ALERT_EMAIL | Operations recipient |
| SQL_SERVER, KEY_VAULT_URI | Bicep outputs for control SQL and Key Vault |
| SYNAPSE_WORKSPACE | Bicep synapseWorkspaceName output |
| SYNAPSE_ENDPOINT | Bicep synapseEndpoint output |
| SYNAPSE_RESOURCE_ID | Bicep synapseWorkspaceId output |
| DATABASE_PREFIX | mobility_dev or mobility_prod |
| SPARK_POOL | mobilityspark (default) |
| ADF_PRINCIPAL_ID | factoryPrincipalId output; used by role bootstrap |

No Databricks variables or CLI are used. Infrastructure outputs also include serverlessSqlEndpoint.
The workspace SQL administrator is the configured Entra group. Grant the deployment principal
Synapse Artifact Publisher, appropriate package-management rights (Synapse Apache Spark Administrator),
and ARM permission to update its Spark pool. The resource creator/Synapse Administrator runs
scripts/bootstrap_synapse.sh to grant ADF Artifact User, Compute Operator and Credential User roles.

The SQL control bootstrap needs ODBC Driver 18, azure-identity and pyodbc, and the administrator's fixed
IP in sqlAdminIps. The Azure SQL server identity needs Directory Readers or narrower equivalent Graph
permissions to resolve CREATE USER FROM EXTERNAL PROVIDER. Those tenant permissions are operator-owned.

## Library and notebook release

Stop the ADF dispatcher and drain active/queued runs. Bump the Python package version when code changes,
build one wheel, then run deploy_synapse.py. It uploads missing versioned workspace packages, replaces
only older mobility package attachments on the pool, preserves other project packages and imports all
notebooks. Keep old workspace wheel files for rollback. Wait for pool provisioning/library installation
to succeed; new notebook sessions pick up packages, existing sessions retain old packages.
Deploy ADF only after the new notebooks/package exist. Resume the trigger after a force=true acceptance
run. GitHub workflows run independently, so initial bootstrap and multi-component releases must follow
this order; do not rely on simultaneous first-run workflows to sequence themselves.

For feeds requiring authentication, store a complete HTTP header block in Key Vault and put only its
secret name in pipeline_control. Public seed feeds need none. Never include API keys in persisted URLs.
Copy and secret-related activities have secure input/output enabled.

## First load and serverless setup

Run MobilityMaster with force=true. Check complete static tables, realtime and weather Silver tables,
Gold facts/dimensions, ops reconciliation, and health activity results. Archive weather intentionally has
a two-day lag. Create the serving user database through the built-in endpoint, render the T-SQL script,
and execute it in that user database. Report identities need SELECT on schema gold and Blob Data Reader
on the Gold container because the selected access pattern is Entra pass-through.

Changing storage or database prefix requires a new serving database/data source configuration. Do not
silently repoint an existing external data source to another environment. Power BI refresh follows a
successful Gold build; stop refresh during recovery because table updates are individually atomic.

## Diagnose and recover

| Failure | Check | Recover |
|---|---|---|
| API Copy | HTTP status, provider quotas, Key Vault permission and headers | Fix configuration, run a new master batch; keep old landing files |
| Synapse notebook cannot start | ADF Synapse RBAC, workspace firewall, pool quota/provisioning | Correct roles/network, wait for a ready pool; do not add passwords |
| Module not found | Workspace wheel attachment, requirements install, old session | Redeploy package, wait for successful install, start new session |
| Storage denied | Workspace identity container RBAC; interactive user identity versus MI | Use Run as managed identity or grant the intended user appropriate RBAC |
| Malformed feed/DQ | Bronze load_status, failure_reason, quarantine and reconciliation | Fix parser/contract; replay retained input through tagged notebook parameters |
| Incomplete static snapshot | Six mandatory entities in one valid batch | Force static ingestion and inspect validation; do not publish partial schedules |
| Gold fails | FK/schedule/delay quarantine, Delta history | Pause reports, fix/redeploy, rerun Gold then health without refetching APIs |
| Serverless wrong/duplicate rows | Gold Delta protocol and unsupported features | Stop reporting; rebuild compatible serving tables from retained valid data |
| Serverless access denied | SQL SELECT, correct user database, Gold storage RBAC | Grant both layers to report identity and reconnect with organizational OAuth |
| Freshness alert | Source event timestamp, scheduler, provider outage | Restore event progress; new polling alone is insufficient |
| Deployment failure | Generated ARM, dependency and library status | Triggers remain stopped; correct deployment before explicit restart |

## Replay

Open mobility_bronze in Synapse Studio, choose the same environment prefix/storage and retained input
path, source, format, source URL and batch ID; run as workspace managed identity. Then run mobility_silver
with the same source/batch, followed by mobility_gold and mobility_health. A same-batch Bronze retry is
idempotent, Silver/Gold MERGE. A decoder correction requires a new correction batch and provenance audit.
Never run manual replay concurrently with ADF against the same databases.

Source watermarks commit after Silver, not Gold. If only Gold fails, rerun it from retained Silver.
Bronze transaction IDs are retained ten years; older replay needs explicit duplicate audit. Avoid VACUUM
under seven days, and adopt a documented retention policy before deleting raw data or Bronze history.

## Alerts and shutdown

Azure Monitor routes master/Copy/freshness/quality failures and a 15-minute no-master-run heartbeat.
Expect log-ingestion delay and startup alerts before tables/logs exist. Test notification delivery with
an API failure, invalid record and stale feed; confirm resolution after recovery.
Stop the ADF trigger and cancel queued runs when not developing. End notebook sessions and let the pool
auto-pause. There is no dedicated SQL warehouse to stop; serverless SQL is billed for data processed.


## Existing storage account / AccountPropertyCannotBeUpdated

Before deployment run `python scripts/preflight_infra.py --resource-group "$RESOURCE_GROUP"`.
If the account already exists and has hierarchical namespace enabled, use
`--reuse-existing true` for preflight and `reuseExistingStorage=true` for Bicep. This preserves the
account's redundancy, firewall/security settings and Blob service retention policies. Containers and
project identity grants are still deployed. Set REUSE_EXISTING_STORAGE=true for GitHub infrastructure CI.

If HNS is false/missing, do not delete the account or retry an ordinary update. Azure requires its
separate HNS migration workflow. Start with read-only eligibility validation:

```bash
az storage account hns-migration start --type validation \
  --name dltmmobilitydev --resource-group "$RESOURCE_GROUP"
```

Validation can report incompatible features such as containerDeleteRetentionPolicy. Review recovery
protection and any workloads before approving changes. Disabling soft-delete protection and the
one-way HNS upgrade are explicit operator decisions, never automatic actions of this template or CI.
An HNS upgrade cannot be reverted and temporarily blocks writes; coordinate a maintenance window.
After a separately approved successful upgrade, confirm isHnsEnabled=true, restore supported protections,
then deploy with reuseExistingStorage=true. Alternatively approve a new globally unique storage name.
See Microsoft's [upgrade procedure](https://learn.microsoft.com/en-us/azure/storage/blobs/upgrade-to-data-lake-storage-gen2-how-to).


## Selected development resources

The approved replacement ADLS Gen2 account is `dltmmobilitydev`; `dftmprojects` is retained because it
is the platform scheduler. The legacy non-HNS account `dltmprojects` is not a platform dependency.
Accounts created by infra/modules/storage.bicep carry mobility-storage-managed=true; preflight permits
normal managed-account redeploys. Untagged existing accounts still require explicit reuse mode and HNS.
