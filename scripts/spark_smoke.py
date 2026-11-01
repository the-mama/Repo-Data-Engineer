"""Opt-in local Delta integration test: python scripts/spark_smoke.py.
Set DELTA_JARS to local delta-spark and delta-storage JAR paths if Maven cannot be reached.
"""

import io
import json
import os
import tempfile
import zipfile
from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from mobility.spark_pipeline import bronze, silver, gold

root = Path(__file__).resolve().parents[1]
with tempfile.TemporaryDirectory(prefix="mobility-spark-") as temp:
    work = Path(temp)
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("mobility-integration")
        .config("spark.sql.warehouse.dir", str(work / "warehouse"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.ui.enabled", "false")
    )
    jars = os.environ.get("DELTA_JARS")
    spark = (
        builder.config("spark.jars", jars) if jars else configure_spark_with_delta_pip(builder)
    ).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("mobility.localRoot", str(work / "lake"))
    try:
        from mobility.control import plan_sources, commit_source
        from datetime import datetime, timezone

        config = json.loads((root / "synapse/config/pipeline_control.json").read_text())
        assert len(plan_sources(spark, "mobility_test", "unused", config, force=True)) == 5
        commit_source(spark, "mobility_test", "unused", "mbta", "trip_updates", "2026-11-01T00:00:00Z")
        planned = plan_sources(spark, "mobility_test", "unused", config, now=datetime.now(timezone.utc))
        assert not any(r["source_name"] == "mbta" and r["entity_name"] == "trip_updates" for r in planned)
        saved = (
            spark.table("mobility_test_ops.pipeline_control")
            .where("source_name='mbta' AND entity_name='trip_updates'")
            .first()
        )
        assert saved.last_watermark_value == "2026-11-01T00:00:00Z", "config refresh reset progress"
        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w") as zipped:
            for path in (root / "samples/gtfs_static_sample").glob("*.txt"):
                zipped.writestr(path.name, path.read_bytes())
        realtime = json.loads((root / "samples/gtfs_realtime_sample.json").read_text())
        realtime["entity"].append(
            {
                "id": "invalid-reference",
                "trip_update": {
                    "trip": {"trip_id": "T1", "route_id": "R1", "start_date": "20261031"},
                    "timestamp": realtime["header"]["timestamp"],
                    "stop_time_update": [
                        {"stop_sequence": 99, "stop_id": "UNKNOWN", "arrival": {"delay": 0}}
                    ],
                },
            }
        )
        files = [
            ("static", archive.getvalue(), "gtfs_zip"),
            ("trip_updates", json.dumps(realtime).encode(), "json"),
            ("vehicle_positions", (root / "samples/vehicle_positions_sample.json").read_bytes(), "json"),
            ("weather", (root / "samples/weather_sample.json").read_bytes(), "json"),
        ]
        for entity, content, fmt in files:
            folder = work / entity
            folder.mkdir()
            path = folder / "response.bin"
            path.write_bytes(content)
            bronze(spark, "mobility_test", str(path), "sample", "https://example.org", entity, fmt, "batch1")
        count = spark.table("mobility_test_bronze.records").count()
        bronze(
            spark,
            "mobility_test",
            str(work / "trip_updates/response.bin"),
            "sample",
            "https://example.org",
            "trip_updates",
            "json",
            "batch1",
        )
        assert spark.table("mobility_test_bronze.records").count() == count, (
            "bronze retry appended duplicates"
        )
        silver(spark, "mobility_test", "unused", "batch1", "sample", quarantine_root=str(work / "quarantine"))
        assert spark.table("mobility_test_ops.quarantine_vehicle_positions").count() == 1
        assert spark.table("mobility_test_silver.vehicle_positions").count() == 2
        silver(spark, "mobility_test", "unused", "batch1", "sample", quarantine_root=str(work / "quarantine"))
        assert spark.table("mobility_test_silver.vehicle_positions").count() == 2, (
            "silver replay duplicated positions"
        )
        malformed = work / "malformed"
        malformed.mkdir()
        (malformed / "response.bin").write_bytes(b"invalid zip")
        bronze(
            spark,
            "mobility_test",
            str(malformed / "response.bin"),
            "malformed",
            "https://example.org",
            "static",
            "gtfs_zip",
            "badbatch",
        )
        silver(
            spark,
            "mobility_test",
            "unused",
            "badbatch",
            "malformed",
            quarantine_root=str(work / "quarantine"),
        )
        assert spark.table("mobility_test_ops.quarantine_static").count() == 1
        gold(spark, "mobility_test", "unused", quarantine_root=str(work / "quarantine"))
        assert spark.table("mobility_test_ops.quarantine_fact_trip_performance").count() == 1
        facts = spark.table("mobility_test_gold.fact_trip_performance")
        assert facts.count() == 4
        assert facts.where("delay_minutes=5").count() == 1
        assert facts.where("actual_arrival_time IS NOT NULL").count() == 1
        assert facts.where("cancellation_flag").count() == 2
        gold(spark, "mobility_test", "unused", quarantine_root=str(work / "quarantine"))
        assert spark.table("mobility_test_gold.fact_trip_performance").count() == 4, (
            "gold replay duplicated facts"
        )
        # Spark views are separate from T-SQL serverless views; test Spark-side semantics here.
        sql_text = "\n".join(
            line
            for line in (root / "sql/serving_views/spark_views.sql").read_text().splitlines()
            if not line.lstrip().startswith("--")
        )
        for statement in sql_text.replace("{database_prefix}", "mobility_test").split(";"):
            if statement.strip():
                spark.sql(statement)
        assert spark.table("mobility_test_gold.v_trip_performance").count() == 4
        # Reader v1 / writer v2 must survive MERGE and repeat Gold builds.
        from mobility.spark_pipeline import assert_serving_protocol

        for name in (
            "fact_trip_performance",
            "dim_route",
            "dim_stop",
            "dim_date_time",
            "dim_weather",
            "agg_route_reliability",
        ):
            assert_serving_protocol(spark, f"mobility_test_gold.{name}")
        # Verify the explicit serverless column mappings match real Parquet/Spark types.
        import re

        serving_sql = (root / "sql/serving_views/001_serverless_views.sql").read_text()
        mapping = {
            "string": "varchar",
            "long": "bigint",
            "integer": "int",
            "double": "float",
            "boolean": "bit",
            "date": "date",
            "timestamp": "datetime2",
        }
        for name, definition in re.findall(
            r"CREATE OR ALTER VIEW gold\.(\w+) AS\s+SELECT \* FROM OPENROWSET.*?WITH \((.*?)\) AS rows;",
            serving_sql,
            re.S,
        ):
            columns = dict(re.findall(r"\[(\w+)\] (\w+)", definition))
            actual = spark.table(f"mobility_test_gold.{name}").schema
            assert set(columns) == {field.name for field in actual}, name
            for field in actual:
                assert columns[field.name] == mapping[field.dataType.typeName()], (name, field)
        print(
            "PASS: Delta retries, malformed ZIP/record quarantine, FK quarantine, gold reconciliation/replay, Gold protocol and serverless column mappings"
        )
    finally:
        spark.stop()
