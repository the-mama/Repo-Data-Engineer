# Native Synapse artifacts

Pipelines, datasets, linkedServices and triggers live next to notebooks/config/libs/tests. The renderer
materializes singular Synapse export folders in a temporary directory with environment-specific URLs,
Spark pool and prefix. Notebook references are local to this workspace, with no self-linked service.

Deploy notebooks/wheel after infrastructure is Succeeded, then publish orchestration with
scripts/deploy_synapse_pipelines.py. It uploads source metadata, stops the managed trigger if running,
creates linked services/datasets, publishes child pipelines before the master, and creates a stopped
trigger. Activation is an explicit --start-trigger option or a Synapse Studio action after acceptance.

No ADF publishing utility, ADF ARM template or separate factory identity is needed. Monitor runs under
Synapse Studio → Monitor. The source JSON has no credentials; optional authentication is a Key Vault
header secret. Delta control state and all data transformations run under the workspace identity.
