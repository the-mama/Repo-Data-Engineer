# Metadata-driven ingestion in Synapse

Status: Accepted; updated for the user's all-Synapse consolidation request.

Keep versioned configuration JSON in landing and mutable polling metadata in the Delta table
<prefix>_ops.pipeline_control. Synapse Lookup reads configuration, control_plan validates/MERGEs it and
returns due rows, and control_commit persists success only after Silver. Configuration refresh must
preserve watermarks. Unsupported schedules, duplicate keys and unsafe paths are rejected.

This removes the external Azure SQL control database and its fixed charge, with additional Spark
planning work/session startup. All orchestration is in one workspace; scheduling remains stopped by
default for development cost control. No dedicated SQL pool is introduced merely to store metadata.
