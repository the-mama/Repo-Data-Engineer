# Synapse architecture

```mermaid
flowchart TB
  Feeds[GTFS static/realtime + Open-Meteo] --> ADF[ADF dftmprojects]
  Control[(Azure SQL pipeline_control)] --> ADF
  KV[Key Vault] -. ADF managed identity .-> ADF
  ADF --> Landing[(dltmmobilitydev / landing)]
  ADF --> NB[Synapse notebooks / mobilityspark]
  Landing --> NB
  NB --> Bronze[(Bronze Delta)]
  Bronze --> Silver[(Silver Delta)]
  Silver --> Gold[(Gold Delta)]
  Silver --> Q[(Quarantine Delta)]
  MI[Synapse workspace managed identity] -. ADLS RBAC .-> NB
  Gold --> SQL[Synapse built-in serverless SQL]
  SQL --> PBI[Power BI / Entra OAuth]
  ADF --> Logs[Log Analytics / Azure Monitor]
  CI[GitHub Actions / Azure OIDC] -. Bicep + wheel/notebook + ADF ARM .-> ADF
```

The workspace has a three-node Small Spark pool with auto-pause and a built-in SQL endpoint. There is
no dedicated SQL pool. Spark writes protocol-compatible Delta; serverless views use Entra pass-through.
The `synapse` filesystem supports workspace operations; medallion data stays in separate named containers.
