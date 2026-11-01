# Repository instructions

Use Python/PySpark and SQL, modular Bicep, and parameterized ADF JSON. Preserve required resource
names dftmprojects and dltmmobilitydev. Never commit credentials, API tokens, PATs, subscription IDs or
connection strings. Supply nonsecret deployment IDs through GitHub environment variables and secrets
through Key Vault; prefer managed identity and Azure OIDC.

Keep source namespaces in keys. Preserve strings and leading zeroes. Normalize to UTC and explicitly
handle GTFS service times >24:00 and DST. Never classify missing realtime events as cancellations or
label predictions as observed arrivals. Keep actual and predicted KPIs separate.

Bronze is append-only; no UPDATE/DELETE/MERGE there. Silver/Gold use deterministic deduplication and
MERGE. Validate explicit schemas; retain new fields in Bronze; quarantine bad records with failure_reason
and failed_at. Update contracts and migration notes for schema changes. Never suppress unexpected failures.

Run ruff check ., pytest -q, python scripts/validate_repo.py and az bicep build --file infra/main.bicep.
Run scripts/spark_smoke.py when changing distributed transformations and cloud acceptance checks when
changing ADF, identity, networking or deployment configuration. Use the supported ADF export utility;
do not hand-edit generated ARM artifacts. Keep tagged-parameter notebooks thin and logic in the tested library.

Use native SynapseNotebook activities, ipynb parameter cells and workspace managed identity.
No dbutils, Unity Catalog, Asset Bundles or Databricks workspace resources. Use names such as
`mobility_dev_gold.fact_trip_performance`, explicit ADLS database locations and versioned pool wheels.
Gold must remain Delta reader 1 / writer 2 with classic checkpoints, no column mapping/deletion vectors.
Do not execute Spark SQL in the serverless editor; serving views are T-SQL OPENROWSET Delta views.
