# Unified Synapse operations

## Deployment configuration

Deploy infrastructure first and wait for Succeeded before publishing data-plane artifacts. Use Synapse
Studio Integrate, Develop, Data and Monitor for this platform. There is no separate ADF factory or Azure
SQL control database. Synapse's built-in SQL endpoint still needs an Entra administrator for serving setup.

| Variable | Purpose |
|---|---|
| AZURE_CLIENT_ID / TENANT_ID / SUBSCRIPTION_ID | GitHub OIDC deployment identity and target |
| RESOURCE_GROUP | Target resource group |
| SQL_ADMIN_OBJECT_ID / SQL_ADMIN_LOGIN | Synapse SQL Entra administrator group ID/name |
| ALERT_EMAIL | Azure Monitor action-group recipient |
| SYNAPSE_WORKSPACE | Workspace name from Bicep output |
| STORAGE_ACCOUNT | dltmmobilitydev for selected development installation |
| KEY_VAULT_URI | Bicep output |
| DATABASE_PREFIX | mobility_dev or environment-specific prefix |
| SPARK_POOL | mobilityspark by default |

Configure GitHub prod required reviewers and protected branches in settings. The deployment principal
needs ARM resource/RBAC permissions, Synapse artifact/package-management rights and scoped Blob Data
Contributor on landing for configuration upload. It must be allowed through the workspace firewall.
Synapse Artifact Publisher plus appropriate Spark/package rights can replace Administrator after bootstrap.
The workspace managed identity has data access to project containers and Key Vault secret reads.

Normal main-branch deployment runs infrastructure and then artifacts sequentially in synapse-ci.yml.
The infrastructure-only workflow is manual recovery tooling, so it does not race artifact publishing.
A new pool must be created before installing requirements. Build exactly one versioned mobility wheel
in dist, run deploy_synapse.py, wait for library installation, then run deploy_synapse_pipelines.py.
The latter stops the managed trigger if running and leaves it stopped by default. Publishing failures
must be investigated before enabling it. New sessions see updated packages; drain existing runs/sessions
before a release. Never publish pool changes while infrastructure is still provisioning the workspace.

## Control metadata

Edit synapse/config/pipeline_control.json and publish it to landing through the deployment script. These
rows are configuration only; API secrets belong in Key Vault. Planning creates/updates the Delta control
table under `<prefix>_ops.pipeline_control`. Refresh preserves last_success_at and last_watermark_value.
Removed rows become inactive. Duplicate keys, unsupported cron/format, unsafe paths and non-HTTPS URLs
are rejected before dispatch. Source success commits only after Silver. Gold failure does not reset polling.

Static/realtime feeds must use one provider namespace. Forecast weather is hourly; archive weather uses
a two-day lag and an overlapping date watermark. Default public seeds require no keys. Authenticated
feeds use a Key Vault secret containing an HTTP header block, referenced by secret name only.

## First run and reports

Run MobilityMaster manually with force=true. Check Lookup configuration, PlanSources output, child Copy
and Spark runs, six complete static entities, realtime/weather Silver tables, Gold facts/dimensions and
health activities. Native notebook outputs drive due-source iteration. Validate supported Delta protocol
and reconciliation before connecting reports. Keep scheduled dispatch stopped for low-cost learning.

Create the serving user database in the built-in SQL endpoint, render the serverless T-SQL views and
execute them in that database, never master. Report identities need SQL SELECT and Gold-container Blob
Data Reader for Entra pass-through. A user with Synapse artifact permissions is not automatically a SQL
administrator or member of the configured Entra SQL administrator group.

## Recovery

| Failure | Diagnose | Recovery |
|---|---|---|
| Workspace API denied | Client IP / Synapse RBAC | Add approved specific client IP, grant appropriate artifact permissions |
| Lookup config unavailable | Landing file and workspace identity RBAC | Republish config and verify Lake linked service; no SQL login needed |
| PlanSources fails | Tagged params, config validation, Delta control state | Correct config/library; preserve existing progress |
| API failure | Provider HTTP status, rate limit, optional KV permission | Fix endpoint/header; rerun new batch; keep old landing bytes |
| Notebook import/library | Pool installation state / old sessions | Wait for Succeeded, start new session, deploy reviewed version |
| Invalid records | Quarantine failure_reason and reconciliation | Correct data/parser, replay with retained provenance |
| Gold failure | Schedule/FK/delay quarantine and Delta history | Pause reports; rerun Gold and health from retained Silver |
| Serverless wrong rows | Reader protocol, DV/column mapping/checkpoints | Stop reports and rebuild compatible tables; defaults do not downgrade old tables |
| Freshness | Source event time, trigger, provider | Restore actual event progress; successful polls alone are insufficient |

Manual replay: run Bronze with retained landing path/source/format/batch, then Silver, then Gold and health.
Do not replay concurrently with the master. A same-batch Bronze retry is idempotent; decoder corrections
need a new correction batch and provenance audit. Transaction IDs are retained ten years. Retention,
VACUUM and deletion need an agreed policy; avoid VACUUM below seven days.

## Monitoring and cost control

Synapse IntegrationPipelineRuns/ActivityRuns/TriggerRuns diagnostics go to resource-specific Log Analytics
tables. Alerts cover pipeline/Copy/health failures and a 15-minute scheduler heartbeat. Keep the heartbeat
alert disabled during planned development shutdown; a stopped trigger otherwise intentionally alerts.
Test notification delivery and resolution. Stop the dispatcher, end sessions and allow the Spark pool to
pause when not learning. A five-minute loop can keep three Spark nodes active continuously.

## Existing storage

Preflight is read-only. Project-created HNS accounts have mobility-storage-managed=true and can be
redeployed. Untagged HNS accounts require reuseExistingStorage=true; account settings are preserved.
Non-HNS adoption is a separate one-way migration requiring explicit approval. Development uses
`dltmmobilitydev`; the empty non-HNS legacy `dltmprojects` account was removed with authorization.
