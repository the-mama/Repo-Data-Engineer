"""Metadata-driven dispatch and polling progress in a Synapse-managed Delta control table."""

from datetime import datetime, timedelta, timezone
from mobility.core import digest
from mobility.spark_pipeline import identifier, setup

SCHEDULES = {"*/5 * * * *": 5, "0 * * * *": 60, "0 2 * * *": 1440}


def prepare_sources(config, now=None):
    if not isinstance(config, list) or not 0 < len(config) <= 1000:
        raise ValueError("configuration must contain 1..1000 sources")
    seen = set()
    for row in config:
        for field in ("source_name", "entity_name"):
            identifier(row[field])
        key = (row["source_name"], row["entity_name"])
        if key in seen:
            raise ValueError(f"duplicate control key: {key}")
        seen.add(key)
        if row["schedule_cron"] not in SCHEDULES:
            raise ValueError("unsupported UTC cron schedule")
        if not row["api_url"].startswith("https://"):
            raise ValueError("HTTPS source URL required")
        if row["content_format"] not in ("gtfs_zip", "protobuf", "json"):
            raise ValueError("unsupported feed format")
        if row["ingestion_type"] not in ("snapshot", "incremental"):
            raise ValueError("unsupported ingestion type")
        if row["target_folder"].startswith("/") or ".." in row["target_folder"].split("/"):
            raise ValueError("unsafe target folder")
    return config


def is_due(row, now, force=False):
    if not row["is_active"]:
        return False
    last = row.get("last_success_at")
    if force or last is None:
        return True
    if isinstance(last, str):
        last = datetime.fromisoformat(last.replace("Z", "+00:00"))
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    if row["schedule_cron"] == "0 2 * * *":
        return now.hour >= 2 and last.date() < now.date()
    interval = SCHEDULES[row["schedule_cron"]]
    # Cron buckets prevent drift when a batch completes a few minutes after the clock boundary.
    return int(now.timestamp()) // (interval * 60) > int(last.timestamp()) // (interval * 60)


def resolve_source(row, now):
    result = dict(row)
    start = (row.get("last_watermark_value") or (now - timedelta(days=7)).date().isoformat())[:10]
    end = (now - timedelta(days=2)).date().isoformat()
    result["api_url"] = row["api_url"].replace("{start_date}", start).replace("{end_date}", end)
    for field in ("created_datetime", "modified_datetime", "last_success_at"):
        result.pop(field, None)
    result.pop("config_hash", None)
    return result


def plan_sources(spark, database_prefix, storage, config, force=False, now=None):
    from delta.tables import DeltaTable
    from pyspark.sql import types as T

    setup(spark, database_prefix, storage)
    now = now or datetime.now(timezone.utc)
    prepare_sources(config)
    strings = [
        "source_name",
        "entity_name",
        "api_url",
        "key_vault_secret_name",
        "ingestion_type",
        "watermark_column",
        "last_watermark_value",
        "target_folder",
        "schedule_cron",
        "content_format",
        "weather_kind",
        "config_hash",
    ]
    schema = T.StructType(
        [T.StructField(k, T.StringType()) for k in strings]
        + [
            T.StructField("is_active", T.BooleanType()),
            T.StructField("created_datetime", T.TimestampType()),
            T.StructField("modified_datetime", T.TimestampType()),
            T.StructField("last_success_at", T.TimestampType()),
        ]
    )
    rows = []
    for item in config:
        row = {key: item.get(key) for key in strings}
        row.update(
            config_hash=digest(item),
            is_active=bool(item.get("is_active", True)),
            weather_kind=item.get("weather_kind", "forecast"),
            created_datetime=now,
            modified_datetime=now,
            last_success_at=None,
        )
        rows.append(row)
    frame = spark.createDataFrame(rows, schema)
    table = f"{database_prefix}_ops.pipeline_control"
    if not spark.catalog.tableExists(table):
        frame.write.format("delta").mode("errorifexists").saveAsTable(table)
    else:
        fields = [k for k in strings if k not in ("source_name", "entity_name", "last_watermark_value")] + [
            "is_active",
            "modified_datetime",
        ]
        (
            DeltaTable.forName(spark, table)
            .alias("t")
            .merge(frame.alias("s"), "t.source_name=s.source_name AND t.entity_name=s.entity_name")
            .whenMatchedUpdate(condition="t.config_hash <> s.config_hash", set={k: f"s.{k}" for k in fields})
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceUpdate(set={"is_active": "false"})
            .execute()
        )
    active = [r.asDict() for r in spark.table(table).collect()]
    return [
        resolve_source(r, now)
        for r in sorted(active, key=lambda r: (r["source_name"], r["entity_name"]))
        if is_due(r, now, force)
    ]


def commit_source(spark, database_prefix, storage, source_name, entity_name, watermark):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    setup(spark, database_prefix, storage)
    table = f"{database_prefix}_ops.pipeline_control"
    if (
        not spark.table(table)
        .where((F.col("source_name") == source_name) & (F.col("entity_name") == entity_name))
        .limit(1)
        .count()
    ):
        raise ValueError("cannot commit an unknown control source")
    progress = spark.createDataFrame(
        [(source_name, entity_name, watermark)], "source_name string, entity_name string, watermark string"
    )
    (
        DeltaTable.forName(spark, table)
        .alias("t")
        .merge(progress.alias("s"), "t.source_name=s.source_name AND t.entity_name=s.entity_name")
        .whenMatchedUpdate(
            set={
                "last_watermark_value": "greatest(t.last_watermark_value,s.watermark)",
                "last_success_at": "current_timestamp()",
                "modified_datetime": "current_timestamp()",
            }
        )
        .execute()
    )
