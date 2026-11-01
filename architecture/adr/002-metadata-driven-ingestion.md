# Metadata-driven ingestion

Status: Accepted

Decision: Use Azure SQL pipeline_control and one ADF dispatcher with parameterized child pipelines.

Rationale: Configuration and due-time checks avoid duplicated pipelines; a constrained UTC cron subset is explicit and testable.

Consequences: Synapse Spark and serverless SQL require workspace identity and serving-view bootstrap. Atomicity is per table; recovery must rerun downstream materializations. Gold is served with Synapse serverless SQL to retain native Delta compatibility.
