# Medallion architecture

Status: Accepted

Decision: Separate raw landing, immutable Bronze, validated Silver and business Gold.

Rationale: Replay, auditability and independent recovery boundaries outweigh duplicated development storage.

Consequences: Synapse Spark and serverless SQL require workspace identity and serving-view bootstrap. Atomicity is per table; recovery must rerun downstream materializations. Gold is served with Synapse serverless SQL to retain native Delta compatibility.
