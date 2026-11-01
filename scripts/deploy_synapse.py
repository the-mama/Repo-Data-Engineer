"""Deploy versioned workspace wheels and notebooks via Azure CLI (OIDC / az login).
Stop/drain ADF before updating pool packages; active sessions retain old libraries.
"""

import json
import os
import subprocess
from pathlib import Path


def az(*args, capture=False):
    command = ["az", *args, "--only-show-errors"]
    if capture:
        return json.loads(subprocess.check_output(command + ["--output", "json"]))
    subprocess.run(command, check=True)


def deploy():
    workspace = os.environ["SYNAPSE_WORKSPACE"]
    group = os.environ["RESOURCE_GROUP"]
    pool = os.environ.get("SPARK_POOL", "mobilityspark")
    wheels = list(Path("dist").glob("urban_mobility-*.whl"))
    if len(wheels) != 1:
        raise ValueError("dist must contain exactly one release wheel; remove stale build outputs first")
    wheel = wheels[0]
    packages = az("synapse", "workspace-package", "list", "--workspace-name", workspace, capture=True)
    if wheel.name not in {p["name"] for p in packages}:
        az("synapse", "workspace-package", "upload", "--workspace-name", workspace, "--package", str(wheel))
    # Preserve other projects' pool packages; replace only previous versions of this library.
    existing = az(
        "synapse",
        "spark",
        "pool",
        "show",
        "--resource-group",
        group,
        "--workspace-name",
        workspace,
        "--name",
        pool,
        capture=True,
    )
    old = [
        p["name"]
        for p in existing.get("customLibraries", [])
        if p["name"].startswith("urban_mobility-") and p["name"] != wheel.name
    ]
    if old:
        az(
            "synapse",
            "spark",
            "pool",
            "update",
            "--resource-group",
            group,
            "--workspace-name",
            workspace,
            "--name",
            pool,
            "--package-action",
            "Remove",
            "--package",
            *old,
        )
    az(
        "synapse",
        "spark",
        "pool",
        "update",
        "--resource-group",
        group,
        "--workspace-name",
        workspace,
        "--name",
        pool,
        "--package-action",
        "Add",
        "--package",
        wheel.name,
        "--library-requirements",
        "synapse/requirements.txt",
    )
    for path in sorted(Path("synapse/notebooks").glob("*.ipynb")):
        az(
            "synapse",
            "notebook",
            "import",
            "--workspace-name",
            workspace,
            "--name",
            "mobility_" + path.stem,
            "--file",
            "@" + str(path.resolve()),
            "--spark-pool-name",
            pool,
            "--folder-path",
            "mobility",
        )
    print(
        "Synapse notebooks and pool package deployed; run an ADF force=true acceptance batch before enabling the trigger."
    )


if __name__ == "__main__":
    deploy()
