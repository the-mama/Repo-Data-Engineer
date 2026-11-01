"""Checks shared by Synapse notebook and integration tests."""

from pyspark.sql import functions as F
from mobility.spark_pipeline import identifier


def check_health(spark, database_prefix, check_mode="all", max_age=30, max_pct=5):
    identifier(database_prefix)
    # Transit freshness is measured at source event time, not just poll time.
    if check_mode in ("all", "freshness"):
        for entity in ("vehicle_positions", "trip_updates"):
            latest = (
                spark.table(f"{database_prefix}_silver.{entity}")
                .agg(F.max("timestamp").alias("ts"))
                .first()
                .ts
            )
            age = (
                spark.sql(f"SELECT timestampdiff(MINUTE, TIMESTAMP '{latest}', current_timestamp()) age")
                .first()
                .age
                if latest
                else None
            )
            if age is None or age > max_age:
                raise RuntimeError(f"freshness_breach:{entity}:age_minutes={age}")
    if check_mode in ("all", "quality"):
        recent = spark.table(f"{database_prefix}_ops.reconciliation").where(
            "checked_at >= current_timestamp() - INTERVAL 30 MINUTES"
        )
        failed = recent.where(
            (F.col("valid_rows") + F.col("quarantined_rows") != F.col("expanded_rows"))
            | (100 * F.col("quarantined_rows") / F.col("expanded_rows") > max_pct)
        ).count()
        if failed:
            raise RuntimeError(f"data_quality_failure:{failed} batches")
        for name in ("fact_trip_performance", "dim_route", "dim_stop", "dim_date_time", "dim_weather"):
            table = spark.table(f"{database_prefix}_gold.{name}")
            key = {
                "fact_trip_performance": "fact_key",
                "dim_route": "route_key",
                "dim_stop": "stop_key",
                "dim_date_time": "date_time_key",
                "dim_weather": "weather_key",
            }[name]
            if table.groupBy(key).count().where("count > 1").limit(1).count():
                raise RuntimeError(f"data_quality_failure:duplicate:{name}")

    if check_mode in ("all", "quality"):
        if (
            spark.table(f"{database_prefix}_ops.gold_reconciliation")
            .where(
                "expanded_candidate_rows <> accepted_fact_rows + quarantined_fact_rows OR quarantined_fact_rows > 0"
            )
            .limit(1)
            .count()
        ):
            raise RuntimeError("data_quality_failure:gold_reconciliation")
