"""Static dependency checks complement Synapse artifact publishing and Bicep compiler."""

import ast
import json
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]


def validate():
    documents = {}
    for folder in ("pipelines", "datasets", "linkedServices", "triggers"):
        for file in (ROOT / "synapse" / folder).glob("*.json"):
            obj = json.loads(file.read_text())
            assert obj["name"] == file.stem, file
            assert "properties" in obj, file
            documents[(folder, obj["name"])] = obj

    def walk(node):
        if isinstance(node, dict):
            if node.get("type") == "SynapseNotebook":
                name = node["typeProperties"]["notebook"]["referenceName"]
                assert name.startswith("mobility_")
                path = ROOT / "synapse/notebooks" / (name.removeprefix("mobility_") + ".ipynb")
                assert path.exists(), name
                notebook = json.loads(path.read_text())
                defaults = {}
                exec(compile("".join(notebook["cells"][0]["source"]), str(path), "exec"), {}, defaults)
                assert set(node["typeProperties"].get("parameters", {})) <= set(defaults), name
            if node.get("type") == "SynapseNotebook":
                assert "linkedServiceName" not in node, (
                    "Local notebooks must not reference a cross-workspace service"
                )
            if "referenceName" in node:
                folder = {
                    "PipelineReference": "pipelines",
                    "DatasetReference": "datasets",
                    "LinkedServiceReference": "linkedServices",
                }.get(node.get("type"))
                if folder:
                    assert (folder, node["referenceName"]) in documents, node
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    for path in (ROOT / "synapse/notebooks").glob("*.ipynb"):
        notebook = json.loads(path.read_text())
        assert "parameters" in notebook["cells"][0]["metadata"]["tags"]
        for cell in notebook["cells"]:
            if cell["cell_type"] == "code":
                ast.parse("".join(cell["source"]), filename=str(path))
    for doc in documents.values():
        walk(doc)
    for file in ROOT.rglob("*.py"):
        if not set(file.parts) & {".venv", "build", ".git"}:
            ast.parse(file.read_text(), filename=str(file))
    for file in list((ROOT / ".github/workflows").glob("*.yml")):
        yaml.safe_load(file.read_text())
    print(f"Validated {len(documents)} Synapse assets, Python/notebook syntax and workflow YAML")


if __name__ == "__main__":
    validate()
