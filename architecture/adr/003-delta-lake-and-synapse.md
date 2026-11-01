# Delta Lake and Azure Synapse

Status: Accepted; replaces ADR 003's earlier Unity Catalog decision at the user's request.

Use Synapse Spark 3.5 to transform Delta tables and Synapse serverless SQL for Power BI serving.
Use workspace managed identity, explicit ADLS database locations and Synapse data-plane RBAC.
Keep Gold at Delta reader 1 / writer 2, disable deletion vectors and column mapping, and use classic
checkpoints. Check Gold protocol after each build. Use explicit OPENROWSET Delta views rather than
relying on automatic Spark table synchronization. Spark and T-SQL views are maintained separately.

Consequences: familiar Azure workspace tooling replaces Databricks-specific setup, but a three-node
Spark floor remains and serverless protocol limits must be enforced. Existing upgraded tables require
a reviewed rebuild; changing writer defaults does not downgrade existing tables.
