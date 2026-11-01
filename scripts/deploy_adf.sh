#!/usr/bin/env bash
set -euo pipefail
: "${RESOURCE_GROUP:?}" "${FACTORY_NAME:?}" "${ADF_TEMPLATE_DIR:?}"
while IFS= read -r trigger; do
  [ -z "$trigger" ] || az datafactory trigger stop --resource-group "$RESOURCE_GROUP" --factory-name "$FACTORY_NAME" --name "$trigger"
done < <(az datafactory trigger list -g "$RESOURCE_GROUP" --factory-name "$FACTORY_NAME" --query '[].name' -o tsv)
az deployment group create -g "$RESOURCE_GROUP" --template-file "$ADF_TEMPLATE_DIR/ARMTemplateForFactory.json" --parameters "@$ADF_TEMPLATE_DIR/ARMTemplateParametersForFactory.json" --parameters factoryName="$FACTORY_NAME"
az datafactory trigger start -g "$RESOURCE_GROUP" --factory-name "$FACTORY_NAME" --name EveryFiveMinutes
