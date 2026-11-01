# Security

ADF system identity writes landing, reads only the configured Key Vault's secrets, and executes only
two control SQL procedures. Native Synapse notebook activities authenticate with ADF MSI. Synapse
Artifact User, Compute Operator and Credential User roles authorize notebook execution; these are
Synapse data-plane roles, distinct from ARM Contributor.

Synapse workspace managed identity has Blob Data Contributor at the six project containers: landing,
bronze, silver, gold, quarantine and the workspace filesystem synapse. It handles pipeline Spark I/O.
Manual notebooks must explicitly run as managed identity or the interactive user's storage RBAC applies.
The workspace default lake does not supply Unity Catalog; Spark databases use explicit ADLS locations.

Serverless SQL serving views use Microsoft Entra pass-through. Every report identity needs SQL SELECT
on the serving Gold schema and Storage Blob Data Reader on the Gold container. That storage grant also
permits direct Gold-file reading; this is a deliberate access boundary. For SQL-only isolation, separately
implement and test a workspace managed-identity credential pattern and narrower SQL grants.

Azure SQL control and Synapse SQL are Entra-only. GitHub Actions use OIDC, with required reviewers configured
for prod. No tokens, subscription IDs, passwords, SAS or connection strings are committed. Optional API
credentials live in Key Vault as header blocks. Secure activity inputs/outputs prevent header logging.
ADLS shared keys and public blobs are disabled; HTTPS/TLS, vault RBAC, soft deletion and purge protection
are enabled. Operational vehicle IDs require access review even without passenger/driver data.

## Network production gate

The development template uses public service endpoints protected by Entra/RBAC and trusted-Azure SQL/
Synapse firewall rules. This is not a private-network production perimeter. Add organization-approved
managed VNet/private endpoints and DNS for workspace dev/SQL, storage DFS/blob, Key Vault and control SQL,
managed ADF networking and explicit public API egress before a regulated release. Validate Synapse Spark
and serverless access from that network. Avoid broad firewall bypasses in production.

Pin reviewed dependencies/Actions, protect main, require PR checks and scan releases. Pause/drain ADF
before changing pool packages. Feed licensing and attribution must be reviewed for production use.
