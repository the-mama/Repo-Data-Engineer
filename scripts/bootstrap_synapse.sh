#!/usr/bin/env bash
set -euo pipefail
: "${SYNAPSE_WORKSPACE:?}" "${ADF_PRINCIPAL_ID:?}"
# Execute as Synapse Administrator. These are Synapse data-plane roles, not Azure ARM roles.
for role in 'Synapse Artifact User' 'Synapse Compute Operator' 'Synapse Credential User'; do
  count=$(az synapse role assignment list --workspace-name "$SYNAPSE_WORKSPACE" --assignee-object-id "$ADF_PRINCIPAL_ID" --role "$role" --query 'length(@)' -o tsv)
  if [ "$count" = 0 ]; then
    az synapse role assignment create --workspace-name "$SYNAPSE_WORKSPACE" --role "$role" --assignee-object-id "$ADF_PRINCIPAL_ID" --assignee-principal-type ServicePrincipal
  fi
done
