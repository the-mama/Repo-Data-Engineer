"""Provision ADF's contained database user using an Entra access token, then apply migrations.
Requires azure-identity and pyodbc + Microsoft ODBC Driver 18 for SQL Server.
"""

import argparse
import struct
from pathlib import Path

if __name__ == "__main__":
    from azure.identity import DefaultAzureCredential
    import pyodbc

    parser = argparse.ArgumentParser()
    parser.add_argument("--server", required=True)
    parser.add_argument("--factory", default="dftmprojects")
    args = parser.parse_args()
    if not args.server.endswith(".database.windows.net") or not args.factory.replace("-", "").isalnum():
        raise ValueError("invalid server or identity name")
    token = (
        DefaultAzureCredential().get_token("https://database.windows.net/.default").token.encode("utf-16-le")
    )
    connection = pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={args.server};DATABASE=mobility_control;Encrypt=yes;TrustServerCertificate=no",
        attrs_before={1256: struct.pack(f"<I{len(token)}s", len(token), token)},
        autocommit=True,
    )
    import re

    for path in sorted(Path("sql/control_tables").glob("*.sql")):
        for statement in re.split(r"^GO\s*$", path.read_text(), flags=re.MULTILINE):
            if statement.strip():
                connection.execute(statement)
    connection.execute(
        f"IF NOT EXISTS(SELECT 1 FROM sys.database_principals WHERE name='{args.factory}') CREATE USER [{args.factory}] FROM EXTERNAL PROVIDER"
    )
    for procedure in ("get_due_sources", "mark_source_success"):
        connection.execute(f"GRANT EXECUTE ON dbo.{procedure} TO [{args.factory}]")
    connection.close()
