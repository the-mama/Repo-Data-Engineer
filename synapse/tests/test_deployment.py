"""Regression: Azure Synapse CLI can return process success with a failed API operation."""

import json
import subprocess
from pathlib import Path
import pytest


def test_cli_zero_exit_api_failure_is_not_published(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "scripts"))
    from azure_cli import az

    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *a, **k: subprocess.CompletedProcess(
            a, 0, json.dumps({"status": "Failed", "error": {"code": "BadRequest"}}), ""
        ),
    )
    with pytest.raises(RuntimeError, match="BadRequest"):
        az("synapse", "pipeline", "create")


def test_cli_capture_handles_resource_lists(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "scripts"))
    from azure_cli import az

    monkeypatch.setattr(subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(a, 0, "[]", ""))
    assert az("synapse", "pipeline", "list", capture=True) == []
