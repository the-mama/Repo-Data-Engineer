"""Render serverless T-SQL without credentials. Supply --storage and --database-prefix."""

import argparse
from pathlib import Path
from mobility.spark_pipeline import identifier


def render(storage, database_prefix):
    identifier(storage)
    identifier(database_prefix)
    return (
        Path("sql/serving_views/001_serverless_views.sql")
        .read_text()
        .replace("${storage}", storage)
        .replace("${database_prefix}", database_prefix)
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--storage", default="dltmprojects")
    parser.add_argument("--database-prefix", default="mobility_dev")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.write_text(render(args.storage, args.database_prefix))
