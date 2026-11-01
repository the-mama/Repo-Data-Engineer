"""Read-only storage checks before deployment. Never migrate or modify an existing account."""

import argparse
import json
import subprocess


def check(resource_group, account, reuse_existing):
    accounts = json.loads(
        subprocess.check_output(
            ["az", "storage", "account", "list", "--resource-group", resource_group, "--output", "json"]
        )
    )
    existing = next((a for a in accounts if a["name"] == account), None)
    if existing:
        if existing.get("isHnsEnabled") is not True:
            raise SystemExit(
                f"{account} already exists without hierarchical namespace enabled. "
                "Stop: an ordinary Bicep update cannot enable it. Validate a separately approved "
                "HNS migration, or choose an approved new account name. Do not delete the account."
            )
        if not reuse_existing:
            raise SystemExit(
                f"{account} already exists. Set reuseExistingStorage=true to preserve its "
                "redundancy, network/security settings and retention policies."
            )
    elif reuse_existing:
        raise SystemExit(f"{account} does not exist in the selected resource group; cannot reuse it.")
    else:
        available = json.loads(
            subprocess.check_output(
                ["az", "storage", "account", "check-name", "--name", account, "--output", "json"]
            )
        )
        if not available["nameAvailable"]:
            raise SystemExit(f"{account} is unavailable: {available.get('message', 'name already used')}")
    print("Storage preflight passed; no account settings were changed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--resource-group", required=True)
    parser.add_argument("--storage", default="dltmprojects")
    parser.add_argument("--reuse-existing", choices=["true", "false"], default="false")
    args = parser.parse_args()
    check(args.resource_group, args.storage, args.reuse_existing == "true")
