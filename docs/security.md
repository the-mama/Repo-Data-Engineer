# Unified Synapse security

The Synapse workspace system identity performs Copy, Key Vault Web reads and pipeline Spark I/O. It has
container-scoped Blob Data Contributor on landing/bronze/silver/gold/quarantine/synapse and Secrets User
on the project vault. There is no Data Factory identity, SQL control user or SQL password. Configuration
is public endpoint metadata; polling progress lives in Delta in the ops database.

The deployment identity needs scoped landing write permission to upload metadata and Synapse artifact/
package permissions. Do not equate ARM Contributor with Synapse data-plane access. Manual notebooks can
run under the user or workspace identity; select the intended context explicitly.

Serverless serving uses Entra pass-through. Report readers need SQL SELECT and Gold-container Blob Data
Reader. That storage permission allows direct Gold file reading too; use a separately reviewed SQL-only
workspace credential pattern if this is unacceptable. Synapse SQL administrator group membership is
separate from Synapse Studio artifact administration.

GitHub OIDC avoids long-lived deployment secrets; prod needs explicit required reviewers. Optional API
headers remain in Key Vault and secret/Copy activity input/output logging is secured. Never put keys in
URLs. New storage accounts disable shared keys/public blobs; reused accounts preserve settings and must
be audited separately. Vault RBAC, purge protection, TLS and diagnostics are enabled.

The template is a public-endpoint development baseline. Add organization-approved private endpoints,
DNS, network restrictions and API egress controls before a regulated production release. Permit only
approved client IPs for administration. Drain jobs before package changes and protect source branches.
