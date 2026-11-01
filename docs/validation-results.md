# Unified Synapse validation — 2026-11-01

| Check | Result |
|---|---|
| Unit tests | 34 passed, including metadata scheduling, watermark dates, invalid config, notebook params and CLI failure detection |
| Ruff / Python and notebook syntax / workflow YAML / artifact references | Passed; 12 native Synapse orchestration assets |
| Bicep compilation | Passed; no Data Factory or separate Azure SQL control resource declarations |
| Local Spark 3.5.3 / Delta 3.2.1 integration | Passed: control planning/commit, preserved watermarks, Bronze/Silver retries, quarantine, Gold replay/reconciliation, protocol and serverless column mappings |
| Azure Spark pool / wheel | Pool Succeeded; mobility 0.3.0 and GTFS requirement installed; notebook import/execution succeeded |
| Azure native artifacts | Five pipelines, three project linked services, three datasets, six notebooks, one stopped schedule |
| Azure ConnectionCheck | Succeeded: read landing configuration and copied an Open-Meteo weather response to ADLS |
| Azure ControlPreview | Succeeded: workspace Spark created/read Delta pipeline_control and returned five due sources to the pipeline expression |
| Retirement | Empty dftmprojects and its project RBAC grants removed; unused Azure SQL control server/database removed |

The selected ADLS account dltmmobilitydev has HNS enabled and all six platform containers. Synapse owns
orchestration, Spark processing, Delta control state and serverless serving. EveryFiveMinutes is stopped
and the development no-scheduler heartbeat is disabled to support intentional low-cost shutdown.

No full live five-feed MobilityMaster processing batch, Gold serverless query acceptance, production
network isolation, notification-delivery drill or Power BI publication was performed in this migration.
See test-plan.md for those remaining operational acceptance checks. Standard GTFS-Realtime predictions
still require a verified observation adapter before actual-arrival KPIs represent observed reliability.
