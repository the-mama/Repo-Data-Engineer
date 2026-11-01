"""Materialize environment-specific Synapse source outside the tracked source tree."""

import json
import os
from pathlib import Path


def render(destination):
    mapping = {
        "https://configure.dfs.core.windows.net": f"https://{os.environ['STORAGE_ACCOUNT']}.dfs.core.windows.net",
        "https://configure.vault.azure.net/": os.environ["KEY_VAULT_URI"].rstrip("/") + "/",
        "mobilityspark": os.environ.get("SPARK_POOL", "mobilityspark"),
        "mobility_dev": os.environ["DATABASE_PREFIX"],
        "dltmmobilitydev": os.environ["STORAGE_ACCOUNT"],
    }
    for folder in ("pipelines", "datasets", "linkedServices", "triggers"):
        for path in Path("synapse", folder).glob("*.json"):
            text = path.read_text()
            for old, new in mapping.items():
                text = text.replace(old, new)
            document = json.loads(text)  # Reject malformed render before export.
            target = (
                destination
                / {
                    "pipelines": "pipeline",
                    "datasets": "dataset",
                    "linkedServices": "linkedService",
                    "triggers": "trigger",
                }[folder]
                / path.name
            )
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(json.dumps(document, indent=2))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("destination", type=Path)
    parser.add_argument("--validation", action="store_true")
    args = parser.parse_args()
    if args.validation:
        defaults = {
            "STORAGE_ACCOUNT": "dltmmobilitydev",
            "KEY_VAULT_URI": "https://configure.vault.azure.net/",
            "SPARK_POOL": "mobilityspark",
            "DATABASE_PREFIX": "mobility_dev",
        }
        os.environ.update(defaults)
    render(args.destination)
