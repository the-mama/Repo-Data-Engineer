"""Contract tests for the migrated Synapse deployment surface."""

import ast
import inspect
import json
import subprocess
import sys
from pathlib import Path
import pytest
from mobility.spark_pipeline import bronze, silver, gold, identifier

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("layer,function", [("bronze", bronze), ("silver", silver), ("gold", gold)])
def test_notebook_parameters_match_library(layer, function):
    notebook = json.loads((ROOT / f"synapse/notebooks/{layer}.ipynb").read_text())
    assert notebook["cells"][0]["metadata"]["tags"] == ["parameters"]
    defaults = {}
    exec("".join(notebook["cells"][0]["source"]), {}, defaults)
    assert set(defaults) <= set(inspect.signature(function).parameters)
    tree = ast.parse("".join(notebook["cells"][1]["source"]))
    call = next(
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == layer
    )
    assert {k.arg for k in call.keywords} == set(defaults)


def test_serverless_renderer_has_tsql_delta_paths(tmp_path):
    output = tmp_path / "views.sql"
    subprocess.run(
        [
            sys.executable,
            "scripts/render_serverless_sql.py",
            "--database-prefix",
            "mobility_prod",
            "--output",
            str(output),
        ],
        cwd=ROOT,
        check=True,
    )
    sql = output.read_text()
    assert "/gold/mobility_prod/gold" in sql
    assert sql.count("FORMAT='DELTA'") == 6
    assert "CREATE OR REPLACE" not in sql
    assert "${" not in sql
    with pytest.raises(ValueError):
        identifier("unsafe';DROP TABLE x")


def test_adf_uses_synapse_and_environment_prefix():
    def activities(nodes):
        for node in nodes:
            yield node
            props = node.get("typeProperties", {})
            for key in ["activities", "ifTrueActivities", "ifFalseActivities"]:
                yield from activities(props.get(key, []))

    for path in (ROOT / "datafactory/pipelines").glob("*.json"):
        document = json.loads(path.read_text())
        assert "catalog" not in document["properties"]["parameters"]
        for node in activities(document["properties"]["activities"]):
            assert node["type"] != "DatabricksNotebook"
            if node["type"] == "SynapseNotebook":
                assert node["linkedServiceName"]["referenceName"] == "Synapse"
                assert "database_prefix" in node["typeProperties"]["parameters"]
