# Validation evidence — Synapse migration

Validated locally on 2026-11-01.

| Check | Result |
|---|---|
| Unit tests | 24 passed, including Synapse notebook/library parameters, ADF configuration and T-SQL rendering |
| Ruff, notebook/Python syntax, workflow YAML and ADF references | Passed |
| Bicep compilation | Passed; Synapse workspace/pool replace Databricks resources |
| Microsoft ADF utility v1.0.3 | Native SynapseNotebook validation passed; ARM export succeeded |
| Wheel build | urban_mobility-0.2.0-py3-none-any.whl built |
| Spark 3.5.3 / Delta 3.2.1 / Java 17 integration | Bronze/Silver retries, quarantine, malformed ZIP, FK quarantine, Gold MERGE/reconciliation and Spark views passed |
| Serverless compatibility | All six Gold tables asserted reader 1/writer 2; explicit T-SQL columns matched actual Spark schemas |
| Offline sample | 19 raw envelopes, 18 valid deduplicated rows, 1 quarantine, 4 stop-event facts |

Local tests ran with Python 3.12; CI uses Python 3.11 to match Synapse's runtime. Maven artifacts were
provided from Maven Central through DELTA_JARS on this host. The integration can also resolve them
through delta-spark normally. These checks do not execute the serverless SQL engine.

The initial checks did not test Azure deployment. Infrastructure has since been deployed successfully
as recorded below; notebook sessions, pool package installation, managed-identity data access,
serverless T-SQL execution, alert delivery and Power BI connectivity remain untested.
Follow test-plan.md for cloud acceptance before operational use.


## Azure storage replacement

On 2026-11-01 the selected development account was changed to dltmmobilitydev. Azure confirmed
isHnsEnabled=true, Standard_LRS and provisioningState=Succeeded, with landing/bronze/silver/gold/
quarantine/synapse containers. The legacy dltmprojects account had no active or soft-deleted containers
or file shares, no queues/tables, and no other Resource Graph references; it was deleted with the
user's authorization. dftmprojects was retained as the platform scheduler. Pipeline/notebook execution
and serverless serving remain separate acceptance steps.


## Azure infrastructure deployment

The development `main` infrastructure deployment subsequently completed with state Succeeded.
Synapse workspace and Spark pool creation were validated in Azure. Initial API validation failures
were corrected: Spark libraries are installed after pool creation, the trusted-Azure Synapse firewall
rule uses AllowAllWindowsAzureIps, and workspace creation explicitly waits for storage/its filesystem.
Spark pool requirements and mobility wheel installation remain part of the separate Synapse publishing
step; no pipeline or Spark processing session was started as part of this infrastructure check.
