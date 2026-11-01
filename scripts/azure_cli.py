"""Checked Azure CLI wrapper for Synapse artifact publishing and management."""

import json
import subprocess


def az(*args, capture=False):
    command = ["az", *args, "--only-show-errors"]
    if "--output" not in args and "-o" not in args:
        command += ["--output", "json"]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    payload = json.loads(result.stdout) if result.stdout.strip() else None
    # Some Synapse CLI LRO responses report an API failure while returning process exit code 0.
    if isinstance(payload, dict) and (payload.get("error") or payload.get("status") == "Failed"):
        raise RuntimeError(f"Synapse publish failed: {payload.get('error', payload)}")
    if capture:
        return payload
    print("Completed: " + " ".join(command[1:4]), flush=True)
