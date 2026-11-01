"""Delta I/O and distributed transformations. Runtime: Azure Synapse Spark 3.5 / Delta 3.2."""

import json
from google.protobuf.message import DecodeError
from zipfile import BadZipFile
from datetime import datetime, timezone
from mobility.core import STATIC_KEYS, KEYS, canonical, decode_response, digest, normalize, validate

AUDIT = [
    "ingestion_timestamp",
    "batch_id",
    "source_system",
    "source_url",
    "source_file_name",
    "record_hash",
    "load_status",
]
ENTITIES = list(KEYS)


def identifier(value):
    import re

    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"invalid SQL identifier: {value}")
    return value


def setup(spark, database_prefix, storage=None):
    """Use Hive-compatible two-part database names and explicit ADLS locations."""
    identifier(database_prefix)
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.timestampType", "TIMESTAMP_LTZ")
    if storage:
        identifier(storage)
        spark.conf.set("mobility.storage", storage)
    local = spark.conf.get("mobility.localRoot", "")
    storage = spark.conf.get("mobility.storage", "")
    if not local and not storage:
        raise ValueError("storage must be configured before creating Synapse databases")
    # These are open-source Delta configuration names, also used by Synapse's bundled Delta runtime.
    for name, value in {
        "minReaderVersion": "1",
        "minWriterVersion": "2",
        "enableDeletionVectors": "false",
        "columnMapping.mode": "none",
        "checkpointPolicy": "classic",
    }.items():
        spark.conf.set(f"spark.databricks.delta.properties.defaults.{name}", value)
    for layer in ("bronze", "silver", "gold", "ops"):
        container = "gold" if layer == "ops" else layer
        location = (
            f"{local}/{database_prefix}/{layer}"
            if local
            else f"abfss://{container}@{storage}.dfs.core.windows.net/{database_prefix}/{layer}"
        )
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_prefix}_{layer} LOCATION '{location}'")


def assert_serving_protocol(spark, table):
    from delta.tables import DeltaTable

    detail = DeltaTable.forName(spark, table).detail().first()
    if detail.minReaderVersion != 1 or detail.minWriterVersion != 2:
        raise ValueError(f"serverless SQL incompatible Delta protocol: {table}")


def merge(spark, frame, table, keys):
    """No schema auto-evolution: contract changes require a reviewed migration."""
    from delta.tables import DeltaTable

    if not spark.catalog.tableExists(table):
        frame.write.format("delta").mode("errorifexists").saveAsTable(table)
        return
    condition = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
    (
        DeltaTable.forName(spark, table)
        .alias("t")
        .merge(frame.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def bronze(
    spark,
    database_prefix,
    landing_file,
    source,
    source_url,
    entity,
    content_format,
    batch_id,
    weather_kind="forecast",
    max_file_bytes=134217728,
    storage=None,
):
    """Append immutable envelopes. A retry uses the same Delta transaction identifier."""
    setup(spark, database_prefix, storage)
    from pyspark.sql import functions as F, types as T

    schema = T.StructType(
        [
            T.StructField(k, T.StringType(), False)
            for k in (
                "entity_name",
                "payload",
                "batch_id",
                "source_system",
                "source_url",
                "source_file_name",
                "record_hash",
                "load_status",
                "weather_kind",
                "ingestion_timestamp",
            )
        ]
    )
    now = datetime.now(timezone.utc).isoformat()

    def parse_files(files):
        for file in files:
            if file.length > max_file_bytes:
                raise ValueError("feed exceeds configured per-file safety limit; use a streaming decoder")
            try:
                for name, record in decode_response(bytes(file.content), entity, content_format):
                    yield (
                        name,
                        canonical(record),
                        batch_id,
                        source,
                        source_url,
                        file.path,
                        digest(record),
                        "loaded",
                        weather_kind,
                        now,
                    )
            except (
                ValueError,
                KeyError,
                OSError,
                TypeError,
                IndexError,
                BadZipFile,
                DecodeError,
                UnicodeDecodeError,
            ) as error:
                # Preserve the bad envelope for silver quarantine; original bytes remain in landing.
                yield (
                    entity,
                    canonical({"_decode_error": str(error)}),
                    batch_id,
                    source,
                    source_url,
                    file.path,
                    digest({"path": file.path}),
                    "decode_failed",
                    weather_kind,
                    now,
                )

    binary = spark.read.format("binaryFile").load(landing_file)
    if binary.limit(1).count() == 0:
        raise ValueError("no landing files matched")
    frame = spark.createDataFrame(binary.rdd.mapPartitions(parse_files), schema)
    frame = frame.withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp")).cache()
    # Preserve decoded raw records in Parquet, alongside byte-for-byte source archives.
    frame.write.mode("ignore").parquet(landing_file.rsplit("/", 1)[0] + "/decoded")
    table = f"{database_prefix}_bronze.records"
    (
        frame.write.format("delta")
        .mode("append")
        .option("txnAppId", f"bronze:{source}:{entity}:{batch_id}")
        .option("txnVersion", 0)
        .option("delta.appendOnly", "true")
        .saveAsTable(table)
    )
    spark.sql(
        f"ALTER TABLE {table} SET TBLPROPERTIES ('delta.appendOnly' = 'true', 'delta.setTransactionRetentionDuration' = 'interval 3650 days')"
    )
    frame.unpersist()


# Flat, explicit schema: new API fields stay in bronze until a reviewed contract migration.
STRING_FIELDS = "agency_id agency_name agency_url agency_timezone route_id route_short_name route_long_name route_type stop_id stop_name trip_id service_id trip_headsign direction_id arrival_time departure_time stop_sequence monday tuesday wednesday thursday friday saturday sunday start_date end_date date exception_type vehicle_id service_date timestamp reported_arrival_time arrival_time_kind location_id weather_condition weather_kind".split()
DOUBLE_FIELDS = "latitude longitude temperature_2m precipitation weather_code reported_delay_seconds delay_minutes".split()
BOOL_FIELDS = ["cancellation_flag", "skipped_flag"]


def silver(spark, database_prefix, storage, batch_id, source, max_delay=180, quarantine_root=None):
    from pyspark.sql import functions as F, types as T, Window

    setup(spark, database_prefix, storage)
    bronze_frame = spark.table(f"{database_prefix}_bronze.records").where(
        (F.col("batch_id") == batch_id) & (F.col("source_system") == source)
    )
    fields = [T.StructField(k, T.StringType()) for k in STRING_FIELDS]
    fields += [T.StructField(k, T.DoubleType()) for k in DOUBLE_FIELDS]
    fields += [T.StructField(k, T.BooleanType()) for k in BOOL_FIELDS]
    flat_schema = T.StructType(fields)
    envelope_schema = (
        "entity_name string, normalized string, failure_reason string, payload string, "
        + ", ".join(f"{k} string" for k in AUDIT + ["input_hash"])
    )

    def process(partition):
        for record in partition:
            audit = {k: str(record[k]) if record[k] is not None else None for k in AUDIT}
            raw = json.loads(record.payload)
            try:
                if record.load_status != "loaded":
                    raise ValueError(raw.get("_decode_error", "decode failed"))
                for row in normalize(record.entity_name, raw, source, record.weather_kind):
                    errors = validate(record.entity_name, row, max_delay)
                    # Convert to explicit contract types; a bad numeric field becomes quarantine.
                    flat = {k: str(row[k]).strip() if row.get(k) is not None else None for k in STRING_FIELDS}
                    flat.update({k: float(row[k]) if row.get(k) is not None else None for k in DOUBLE_FIELDS})
                    flat.update({k: row.get(k) for k in BOOL_FIELDS})
                    yield {
                        **audit,
                        "entity_name": record.entity_name,
                        "normalized": canonical(flat),
                        "failure_reason": ";".join(errors) or None,
                        "payload": record.payload,
                        "input_hash": record.record_hash,
                        "record_hash": digest(row),
                    }
            except (ValueError, KeyError, TypeError, OverflowError, OSError) as error:
                yield {
                    **audit,
                    "entity_name": record.entity_name,
                    "normalized": "{}",
                    "failure_reason": f"parse:{type(error).__name__}:{error}",
                    "payload": record.payload,
                    "input_hash": record.record_hash,
                }

    processed = spark.createDataFrame(bronze_frame.rdd.mapPartitions(process), envelope_schema).cache()
    failures = processed.where("failure_reason IS NOT NULL").withColumn("failed_at", F.current_timestamp())
    # Quarantine is Delta and idempotent for the same source/batch/input/failure.
    for name in [r.entity_name for r in processed.select("entity_name").distinct().collect()]:
        identifier(name)
        bad = failures.where(F.col("entity_name") == name)
        if bad.limit(1).count():
            path = (
                quarantine_root or f"abfss://quarantine@{storage}.dfs.core.windows.net/{database_prefix}"
            ) + f"/{name}"
            table = f"{database_prefix}_ops.quarantine_{name}"
            if not spark.catalog.tableExists(table):
                (bad.write.format("delta").mode("errorifexists").option("path", path).saveAsTable(table))
            else:
                merge(spark, bad, table, ["source_system", "batch_id", "input_hash", "failure_reason"])
        valid = (
            processed.where((F.col("entity_name") == name) & F.col("failure_reason").isNull())
            .withColumn("data", F.from_json("normalized", flat_schema))
            .select("data.*", *AUDIT, "input_hash")
            .withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp"))
        )
        if name not in KEYS or not valid.limit(1).count():
            continue
        if name in ("vehicle_positions", "trip_updates", "weather"):
            valid = valid.withColumn("timestamp", F.to_timestamp("timestamp"))
        keys = ["source_system", *KEYS[name]] + (["batch_id"] if name in STATIC_KEYS else [])
        window = Window.partitionBy(*keys).orderBy(F.desc("ingestion_timestamp"), F.desc("record_hash"))
        valid = valid.withColumn("rank", F.row_number().over(window)).where("rank = 1").drop("rank")
        # Corrections with an older ingestion time must not overwrite a later replay.
        table = f"{database_prefix}_silver.{name}"
        if spark.catalog.tableExists(table):
            from delta.tables import DeltaTable

            condition = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
            (
                DeltaTable.forName(spark, table)
                .alias("t")
                .merge(valid.alias("s"), condition)
                .whenMatchedUpdateAll(
                    condition="s.ingestion_timestamp > t.ingestion_timestamp OR (s.ingestion_timestamp = t.ingestion_timestamp AND s.record_hash >= t.record_hash)"
                )
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            merge(spark, valid, table, keys)
    # Reconciliation counts envelopes as well as expanded output; they are intentionally different grains.
    counts = processed.groupBy("entity_name").agg(
        F.count("*").alias("expanded_rows"),
        F.countDistinct("input_hash").alias("input_records"),
        F.sum(F.when(F.col("failure_reason").isNull(), 1).otherwise(0)).alias("valid_rows"),
        F.sum(F.when(F.col("failure_reason").isNotNull(), 1).otherwise(0)).alias("quarantined_rows"),
    )
    counts = counts.withColumn("batch_id", F.lit(batch_id)).withColumn("source_system", F.lit(source))
    counts = counts.withColumn("checked_at", F.current_timestamp())
    merge(
        spark, counts, f"{database_prefix}_ops.reconciliation", ["source_system", "batch_id", "entity_name"]
    )
    processed.unpersist()


def gold(
    spark, database_prefix, storage, max_delay=180, early_minutes=-1, late_minutes=5, quarantine_root=None
):
    from pyspark.sql import functions as F, Window, types as T
    from mobility.core import gtfs_arrival, haversine_km

    setup(spark, database_prefix, storage)
    # Require a coherent static snapshot: six mandatory entities in the same successful batch.
    static = spark.table(f"{database_prefix}_ops.reconciliation").where(
        "entity_name IN ('agency','routes','stops','trips','stop_times','calendar') AND quarantined_rows = 0 AND valid_rows > 0"
    )
    complete = (
        static.groupBy("source_system", "batch_id")
        .agg(F.countDistinct("entity_name").alias("n"), F.max("checked_at").alias("checked_at"))
        .where("n = 6")
    )
    w = Window.partitionBy("source_system").orderBy(F.desc("checked_at"), F.desc("batch_id"))
    snapshots = (
        complete.withColumn("rn", F.row_number().over(w)).where("rn=1").select("source_system", "batch_id")
    )
    if not snapshots.limit(1).count():
        raise ValueError("no complete static snapshot available")

    def latest(name):
        return spark.table(f"{database_prefix}_silver.{name}").join(
            snapshots, ["source_system", "batch_id"], "inner"
        )

    def key(*columns):
        return F.sha2(F.to_json(F.array(*[F.col(c).cast("string") for c in columns])), 256)

    routes = latest("routes").withColumn("route_key", key("source_system", "route_id"))
    stops = latest("stops").withColumn("stop_key", key("source_system", "stop_id"))
    # Version static keys to preserve historical relationships when schedules change.
    routes = routes.withColumn("route_key", key("source_system", "route_id", "batch_id"))
    stops = stops.withColumn("stop_key", key("source_system", "stop_id", "batch_id"))
    merge(
        spark,
        routes.select(
            "route_key", "source_system", "route_id", "route_short_name", "route_long_name", "batch_id"
        ),
        f"{database_prefix}_gold.dim_route",
        ["route_key"],
    )
    merge(
        spark,
        stops.select(
            "stop_key", "source_system", "stop_id", "stop_name", "latitude", "longitude", "batch_id"
        ),
        f"{database_prefix}_gold.dim_stop",
        ["stop_key"],
    )
    trip = latest("trips").select("source_system", "trip_id", "route_id", "agency_id")
    agency = latest("agency").select("source_system", "agency_id", "agency_timezone")
    # A single-agency feed may omit agency_id in routes/trips.
    route_agency = latest("routes").select(
        "source_system", "route_id", F.col("agency_id").alias("route_agency_id")
    )
    trip = trip.join(route_agency, ["source_system", "route_id"], "left")
    single = agency.groupBy("source_system").agg(
        F.count("*").alias("n"), F.first("agency_id").alias("single_id")
    )
    trip = (
        trip.join(single, "source_system")
        .withColumn(
            "agency_id",
            F.coalesce("agency_id", "route_agency_id", F.when(F.col("n") == 1, F.col("single_id"))),
        )
        .join(agency, ["source_system", "agency_id"], "left")
    )
    stop_times = latest("stop_times").select(
        "source_system", "trip_id", "stop_id", "stop_sequence", "arrival_time", "batch_id"
    )
    updates = spark.table(f"{database_prefix}_silver.trip_updates")
    wu = Window.partitionBy("source_system", "trip_id", "service_date", "stop_sequence").orderBy(
        F.desc("timestamp"), F.desc("record_hash")
    )
    updates = updates.withColumn("rn", F.row_number().over(wu)).where("rn=1").drop("rn")
    updates = updates.select(
        "source_system",
        "trip_id",
        "service_date",
        F.col("stop_sequence").alias("update_sequence"),
        "timestamp",
        "reported_arrival_time",
        "reported_delay_seconds",
        "arrival_time_kind",
        "cancellation_flag",
        "skipped_flag",
    )
    # Keep unknown trip/stop references for quarantine rather than dropping them in an inner join.
    base = updates.join(stop_times, ["source_system", "trip_id"], "left").where(
        (F.col("update_sequence") == "*")
        | (F.col("update_sequence") == F.col("stop_sequence"))
        | F.col("stop_sequence").isNull()
    )
    # Catch updates whose trip exists but stop sequence is unknown.
    matched = base.select(
        "source_system", "trip_id", "service_date", "update_sequence", "timestamp"
    ).distinct()
    unmatched = updates.join(
        matched, ["source_system", "trip_id", "service_date", "update_sequence", "timestamp"], "left_anti"
    )
    base = base.unionByName(unmatched, allowMissingColumns=True).join(
        trip, ["source_system", "trip_id"], "left"
    )
    base = base.join(
        routes.select("source_system", "route_id", "batch_id", "route_key"),
        ["source_system", "route_id", "batch_id"],
        "left",
    )
    base = base.join(
        stops.select("source_system", "stop_id", "batch_id", "stop_key", "latitude", "longitude"),
        ["source_system", "stop_id", "batch_id"],
        "left",
    )

    @F.udf(T.TimestampType())
    def schedule(day, time, zone):
        try:
            return gtfs_arrival(day, time, zone) if day and time and zone else None
        except (ValueError, KeyError):
            return None

    base = base.withColumn(
        "scheduled_arrival_time", schedule("service_date", "arrival_time", "agency_timezone")
    )
    base = base.withColumn(
        "reported_arrival_time",
        F.coalesce(
            F.to_timestamp("reported_arrival_time"),
            F.to_timestamp(
                F.from_unixtime(
                    F.col("scheduled_arrival_time").cast("long") + F.col("reported_delay_seconds")
                )
            ),
        ),
    )
    # Trip cancellation and stop updates can coexist in different snapshots; latest event wins.
    wf = Window.partitionBy("source_system", "trip_id", "service_date", "stop_sequence").orderBy(
        F.desc("timestamp"), F.desc("cancellation_flag")
    )
    base = base.withColumn("rn", F.row_number().over(wf)).where("rn=1").drop("rn")
    base = base.withColumn(
        "actual_arrival_time",
        F.when(F.col("arrival_time_kind") == "observed", F.col("reported_arrival_time")),
    )
    base = base.withColumn(
        "predicted_arrival_time",
        F.when(F.col("arrival_time_kind") == "prediction", F.col("reported_arrival_time")),
    )
    base = base.withColumn(
        "delay_minutes",
        (F.col("actual_arrival_time").cast("long") - F.col("scheduled_arrival_time").cast("long")) / 60,
    )
    base = base.withColumn(
        "predicted_delay_minutes",
        (F.col("predicted_arrival_time").cast("long") - F.col("scheduled_arrival_time").cast("long")) / 60,
    )
    bad_condition = (
        F.col("route_key").isNull()
        | F.col("stop_key").isNull()
        | F.col("scheduled_arrival_time").isNull()
        | (F.abs(F.col("delay_minutes")) > max_delay)
        | (F.abs(F.col("predicted_delay_minutes")) > max_delay)
    )
    bad_condition = F.coalesce(bad_condition, F.lit(False))
    rejected = base.where(bad_condition).withColumn(
        "failure_reason", F.lit("referential_integrity_or_schedule_or_delay")
    )
    rejected = rejected.withColumn("failed_at", F.current_timestamp()).withColumn(
        "failure_key", key("source_system", "trip_id", "service_date", "update_sequence", "timestamp")
    )
    if rejected.limit(1).count():
        table = f"{database_prefix}_ops.quarantine_fact_trip_performance"
        if not spark.catalog.tableExists(table):
            rejected.write.format("delta").option(
                "path",
                (quarantine_root or f"abfss://quarantine@{storage}.dfs.core.windows.net/{database_prefix}")
                + "/fact_trip_performance",
            ).saveAsTable(table)
        else:
            merge(spark, rejected, table, ["failure_key"])
    facts = base.where(~bad_condition).withColumn(
        "fact_key", key("source_system", "trip_id", "service_date", "stop_sequence")
    )
    facts = facts.withColumn("hour_utc", F.date_trunc("hour", "scheduled_arrival_time"))
    weather = spark.table(f"{database_prefix}_silver.weather").withColumn(
        "hour_utc", F.date_trunc("hour", "timestamp")
    )
    ww = Window.partitionBy("location_id", "hour_utc").orderBy(
        F.when(F.col("weather_kind") == "historical", 0).otherwise(1), F.desc("ingestion_timestamp")
    )
    weather = weather.withColumn("rn", F.row_number().over(ww)).where("rn=1").drop("rn")
    weather = weather.withColumn("weather_key", key("location_id", "hour_utc"))
    weather_dim = weather.select(
        "weather_key",
        "location_id",
        "hour_utc",
        "weather_condition",
        "temperature_2m",
        "precipitation",
        "weather_kind",
    )
    merge(spark, weather_dim, f"{database_prefix}_gold.dim_weather", ["weather_key"])

    @F.udf(T.DoubleType())
    def distance(a, b, c, d):
        return haversine_km(a, b, c, d) if all(v is not None for v in (a, b, c, d)) else None

    candidates = (
        facts.alias("f")
        .join(weather.alias("w"), F.col("f.hour_utc") == F.col("w.hour_utc"), "left")
        .select(
            "f.*",
            "w.weather_key",
            "w.weather_condition",
            "w.location_id",
            distance(
                F.col("f.latitude"), F.col("f.longitude"), F.col("w.latitude"), F.col("w.longitude")
            ).alias("weather_distance_km"),
        )
    )
    wc = Window.partitionBy("fact_key").orderBy(F.asc_nulls_last("weather_distance_km"), "location_id")
    facts = candidates.withColumn("rn", F.row_number().over(wc)).where("rn=1").drop("rn")
    facts = facts.withColumn("weather_key", F.when(F.col("weather_distance_km") <= 100, F.col("weather_key")))
    facts = facts.withColumn(
        "weather_condition",
        F.when(F.col("weather_key").isNotNull(), F.col("weather_condition")).otherwise("unknown"),
    )
    positions = spark.table(f"{database_prefix}_silver.vehicle_positions").where("service_date IS NOT NULL")
    wp = Window.partitionBy("source_system", "trip_id", "service_date", "vehicle_id").orderBy("timestamp")
    positions = positions.withColumn("prev_lat", F.lag("latitude").over(wp)).withColumn(
        "prev_lon", F.lag("longitude").over(wp)
    )
    movement = positions.groupBy("source_system", "trip_id", "service_date").agg(
        F.countDistinct("vehicle_id").alias("vehicle_count"),
        F.sum(distance("latitude", "longitude", "prev_lat", "prev_lon")).alias("distance_travelled_km"),
    )
    facts = facts.join(movement, ["source_system", "trip_id", "service_date"], "left")
    facts = facts.withColumn(
        "on_time_flag",
        F.when(
            ~F.col("cancellation_flag") & ~F.col("skipped_flag") & F.col("actual_arrival_time").isNotNull(),
            F.col("delay_minutes").between(early_minutes, late_minutes),
        ),
    )
    facts = facts.withColumn("date_time_key", F.date_format("hour_utc", "yyyyMMddHH").cast("long"))
    dim_time = (
        facts.select("date_time_key", "hour_utc")
        .distinct()
        .withColumn("date", F.to_date("hour_utc"))
        .withColumn("hour", F.hour("hour_utc"))
        .withColumn("weekday", F.date_format("hour_utc", "EEEE"))
    )
    merge(spark, dim_time, f"{database_prefix}_gold.dim_date_time", ["date_time_key"])
    columns = "fact_key source_system trip_id service_date stop_sequence route_key stop_key date_time_key weather_key scheduled_arrival_time actual_arrival_time predicted_arrival_time delay_minutes predicted_delay_minutes cancellation_flag skipped_flag vehicle_count distance_travelled_km weather_condition on_time_flag arrival_time_kind timestamp".split()
    merge(spark, facts.select(*columns), f"{database_prefix}_gold.fact_trip_performance", ["fact_key"])
    spark.sql(f"""CREATE OR REPLACE TABLE {database_prefix}_gold.agg_route_reliability USING DELTA AS
    WITH measures AS (
      SELECT route_key, stop_key, date_time_key, weather_condition,
        COUNT(*) AS reported_stop_events,
        COUNT(actual_arrival_time) AS observed_stop_events,
        CAST(100.0 * SUM(CASE WHEN on_time_flag THEN 1 ELSE 0 END) / NULLIF(COUNT(on_time_flag),0) AS DOUBLE) AS on_time_performance_pct,
        AVG(delay_minutes) AS average_delay_minutes,
        percentile_approx(delay_minutes,0.95) AS p95_delay_minutes,
        AVG(predicted_delay_minutes) AS average_predicted_delay_minutes,
        CAST(100.0 * SUM(CASE WHEN cancellation_flag THEN 1 ELSE 0 END) / COUNT(*) AS DOUBLE) AS cancellation_rate_pct
      FROM {database_prefix}_gold.fact_trip_performance
      GROUP BY route_key,stop_key,date_time_key,weather_condition)
    SELECT * FROM measures""")
    for name in (
        "fact_trip_performance",
        "dim_route",
        "dim_stop",
        "dim_date_time",
        "dim_weather",
        "agg_route_reliability",
    ):
        assert_serving_protocol(spark, f"{database_prefix}_gold.{name}")
    # Gold reconciliation uses the expanded candidate grain, not raw API row counts.
    reconciliation = spark.createDataFrame(
        [
            (
                datetime.now(timezone.utc).date().isoformat(),
                updates.count(),
                base.count(),
                facts.count(),
                rejected.count(),
                spark.table(f"{database_prefix}_gold.fact_trip_performance").count(),
            )
        ],
        "run_date string, latest_update_rows long, expanded_candidate_rows long, accepted_fact_rows long, quarantined_fact_rows long, total_fact_rows long",
    )
    reconciliation = reconciliation.withColumn("checked_at", F.current_timestamp())
    merge(spark, reconciliation, f"{database_prefix}_ops.gold_reconciliation", ["run_date"])
    # Expose usable freshness and DQ metrics in DBSQL; Azure Monitor queries ADF failures independently.
    spark.sql(f"""CREATE OR REPLACE VIEW {database_prefix}_ops.health AS
      SELECT source_system, MAX(ingestion_timestamp) AS last_ingestion,
        timestampdiff(MINUTE, MAX(ingestion_timestamp), current_timestamp()) AS age_minutes
      FROM {database_prefix}_bronze.records GROUP BY source_system""")
