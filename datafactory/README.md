# Metadata-driven ingestion design

Authored folders retain the requested plural names. `scripts/configure_adf.py` materializes Microsoft's
required singular folders (`pipeline`, `dataset`, `linkedService`, `trigger`) into a temporary export tree.
It substitutes environment-specific hosts, workspace/cluster IDs, Synapse workspace/pool and database prefix.
Endpoints and nonsecret secret-name references come from dbo.pipeline_control; credentials stay in Key Vault.

- MobilityMaster: Lookup due rows → sequential ForEach/Execute Pipeline → Gold → freshness → quality.
- IngestSource: optional secure WebActivity to Key Vault → parameterized Binary Copy → Execute Pipeline
  TransformSource → stored-procedure success/watermark commit.
- TransformSource: Bronze append/decode → Silver validation/quarantine/MERGE.

Notebook activities use SynapseNotebook and MSI AzureSynapseArtifacts linked services. Versioned
workspace wheels must be installed on the Spark pool before ADF runs. `main` is the collaboration branch; CI publishes ARM through the
Microsoft export utility. Git-backed authoring in the portal should use the rendered singular tree,
not change the authored plural directories independently.

```bash
python scripts/configure_adf.py /tmp/adf-validation --validation
cd datafactory
npm ci --ignore-scripts
npm run build validate /tmp/adf-validation "$FACTORY_RESOURCE_ID"
npm run build export /tmp/adf-validation "$FACTORY_RESOURCE_ID" arm-template/generated
```

The exporter interprets its output folder relative to its working directory; use a relative path.
For deployment omit --validation and supply the documented environment variables. ARM artifacts are
published by CI under arm-template/generated and are ignored in git. Never hand-edit generated ARM.
Triggers are stopped during deployment; removed entities need an explicit migration rather than
unreviewed deletion. Start the dispatcher only after identity, Synapse roles, SQL and wheel bootstrap succeeds.
