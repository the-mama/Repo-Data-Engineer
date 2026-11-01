# ADF publishing artifacts
The collaboration branch is `main`. CI uses Microsoft's ADF utilities to validate and export the authored
pipelines, datasets, linked services and triggers, then uploads the resulting ARM template as a build artifact.
Generated templates are deliberately not manually maintained. `scripts/configure_adf.py` renders a temporary
source tree from environment variables before export; no connection secrets are materialized.
Deploy with `scripts/deploy_adf.sh`; it stops triggers, deploys in Incremental mode, then starts the environment trigger.
On failure triggers stay stopped for investigation. Existing removed resources are not silently deleted.
