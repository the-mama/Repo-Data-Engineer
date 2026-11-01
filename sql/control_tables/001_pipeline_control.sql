-- Spark SQL contract. plan_sources() creates/manages this Delta table with explicit ADLS database locations.
-- Render {database_prefix}; run in Spark, not serverless SQL.
CREATE TABLE IF NOT EXISTS {database_prefix}_ops.pipeline_control (
 source_name STRING, entity_name STRING, api_url STRING, key_vault_secret_name STRING,
 ingestion_type STRING, watermark_column STRING, last_watermark_value STRING, target_folder STRING,
 schedule_cron STRING, is_active BOOLEAN, created_datetime TIMESTAMP, modified_datetime TIMESTAMP,
 content_format STRING, weather_kind STRING, last_success_at TIMESTAMP, config_hash STRING
) USING DELTA;
-- Seed configuration is synapse/config/pipeline_control.json. Watermarks are never reset on config refresh.
