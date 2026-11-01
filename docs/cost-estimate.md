# Development cost model — Synapse

Planning assumptions dated 2026-11-01, USD, not a regional Azure quote. Verify rates in the
[Azure calculator](https://azure.microsoft.com/en-us/pricing/calculator/) and
[Synapse pricing](https://azure.microsoft.com/en-us/pricing/details/synapse-analytics/).

The Small Spark pool has **three nodes**, not a single-node cluster. Budget for a minimum 12 vCores
while active (3 × 4). At an illustrative assumed $0.12–$0.18/vCore-hour and 44 active hours/month,
Spark alone is roughly **$63–95/month**, before session startup/idle time. These are arithmetic planning
assumptions, not retrieved SKU rates. Auto-pause is 15 minutes after idle; five-minute polling can keep
Spark active all month. At 720 active hours, the same assumptions imply ~$1,037–1,555 for Spark alone.

| Component | Illustrative monthly development allowance |
|---|---:|
| Synapse Spark, 44 hours | $63–95 |
| Serverless SQL scans | $1–10, depending on bytes scanned/minimum query charge |
| ADLS and transactions | $2–10 for <=50 GB |
| Azure SQL Basic control | $5–15 |
| ADF | $10–70 depending on activity count and Copy usage |
| Logs, alerts and Key Vault | $3–20 |
| Total illustrative Azure allowance | $84–220 |

Power BI licensing, tax, networking, egress and support are excluded. There is no Databricks DBU charge
and no dedicated SQL pool. The three-node Spark floor can still be costly for tiny workloads.
Stop the ADF trigger outside learning sessions. Track billed Spark/session time, scan costs and ADF
activity counts, set Azure budgets, filter serverless queries and keep retention bounded.
