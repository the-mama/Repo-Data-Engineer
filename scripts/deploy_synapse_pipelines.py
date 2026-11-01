"""Publish same-workspace Synapse orchestration. Triggers remain stopped unless explicitly requested.
Requires a ready control database, Synapse wheel/notebooks and the configured workspace identity.
"""

import argparse
import os
import tempfile
from pathlib import Path
from configure_synapse import render
from azure_cli import az


def deploy(start_trigger=False):
    from mobility.spark_pipeline import identifier

    for key in ("SYNAPSE_WORKSPACE", "STORAGE_ACCOUNT", "DATABASE_PREFIX", "KEY_VAULT_URI"):
        if not os.environ.get(key):
            raise ValueError(f"Missing deployment setting: {key}")
    identifier(os.environ["DATABASE_PREFIX"])
    workspace = os.environ["SYNAPSE_WORKSPACE"]
    triggers = az("synapse", "trigger", "list", "--workspace-name", workspace, capture=True)
    managed = next((t for t in triggers if t["name"] == "EveryFiveMinutes"), None)
    if (
        managed
        and managed.get("properties", {}).get("runtimeState", managed.get("runtimeState")) == "Started"
    ):
        az("synapse", "trigger", "stop", "--workspace-name", workspace, "--name", "EveryFiveMinutes")
    az(
        "storage",
        "blob",
        "upload",
        "--auth-mode",
        "login",
        "--account-name",
        os.environ["STORAGE_ACCOUNT"],
        "--container-name",
        "landing",
        "--name",
        os.environ["DATABASE_PREFIX"] + "/config/pipeline_control.json",
        "--file",
        "synapse/config/pipeline_control.json",
        "--overwrite",
        "--output",
        "none",
    )
    with tempfile.TemporaryDirectory(prefix="mobility-synapse-") as temp:
        source = Path(temp)
        render(source)
        for folder, resource in [("linkedService", "linked-service"), ("dataset", "dataset")]:
            for path in sorted((source / folder).glob("*.json")):
                az(
                    "synapse",
                    resource,
                    "create",
                    "--workspace-name",
                    workspace,
                    "--name",
                    path.stem,
                    "--file",
                    "@" + str(path),
                )
        # Publish children before the parent; do not rely on API accepting unresolved references.
        for name in (
            "TransformSource",
            "IngestSource",
            "MobilityMaster",
            "ConnectionCheck",
            "ControlPreview",
        ):
            path = source / "pipeline" / (name + ".json")
            az(
                "synapse",
                "pipeline",
                "create",
                "--workspace-name",
                workspace,
                "--name",
                name,
                "--file",
                "@" + str(path),
            )
        for path in sorted((source / "trigger").glob("*.json")):
            az(
                "synapse",
                "trigger",
                "create",
                "--workspace-name",
                workspace,
                "--name",
                path.stem,
                "--file",
                "@" + str(path),
            )
    if start_trigger:
        az("synapse", "trigger", "start", "--workspace-name", workspace, "--name", "EveryFiveMinutes")
    print(
        "Synapse linked services, datasets, pipelines and trigger published. Trigger "
        + ("started." if start_trigger else "left stopped for acceptance and cost control.")
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start-trigger", action="store_true", help="Use only after a successful acceptance batch"
    )
    deploy(parser.parse_args().start_trigger)
