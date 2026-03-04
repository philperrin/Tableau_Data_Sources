"""
Tableau Cloud - Datasource Table Lineage Extractor
Reads tableau_datasources.csv (produced by "tableau cloud data sources.py"),
then queries the Tableau Metadata API (GraphQL) to retrieve the upstream
database, schema, and table for each datasource LUID.
"""

import requests
import csv
import json
from pathlib import Path
import os

# ---------------------------------------------------------------------------
# Configuration — keep in sync with "tableau cloud data sources.py"
# ---------------------------------------------------------------------------

TABLEAU_SERVER_URL = os.environ.get('TABLEAU_SERVER_URL')  # e.g. https://prod-useast-a.online.tableau.com
SITE_NAME = os.environ.get('TABLEAU_SITE_NAME')          # Content URL / site name (empty string for Default site)
TOKEN_NAME = os.environ.get('TABLEAU_PAT_NAME')        # Personal Access Token name
PAT = os.environ.get('TABLEAU_PAT_SECRET')    # Personal Access Token value

# Input: CSV produced by the datasources extractor script
INPUT_CSV = Path(__file__).parent / "tableau_datasources.csv"

# Output: written to the same folder as this script
OUTPUT_CSV = Path(__file__).parent / "tableau_datasource_tables.csv"

# How many LUIDs to send in a single Metadata API request (max ~50 recommended)
BATCH_SIZE = 50

API_VERSION = "3.27"

# ---------------------------------------------------------------------------
# GraphQL query — fetches upstream tables for a batch of datasource LUIDs
# ---------------------------------------------------------------------------

UPSTREAM_TABLES_QUERY = """
query getUpstreamTables($luids: [String!]!) {
  publishedDatasourcesConnection(filter: {luidWithin: $luids}) {
    nodes {
      luid
      name
      upstreamTables {
        name
        schema
        database {
          name
          connectionType
        }
      }
    }
  }
}
"""

# ---------------------------------------------------------------------------
# Auth helpers (mirrors the main datasources script)
# ---------------------------------------------------------------------------

def sign_in(server_url: str, site_name: str, token_name: str, pat: str) -> tuple[str, str]:
    """Authenticate with Tableau Cloud and return (auth_token, site_id)."""
    url = f"{server_url}/api/{API_VERSION}/auth/signin"
    payload = {
        "credentials": {
            "personalAccessTokenName": token_name,
            "personalAccessTokenSecret": pat,
            "site": {"contentUrl": site_name},
        }
    }
    response = requests.post(
        url,
        json=payload,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    response.raise_for_status()
    try:
        data = response.json()
    except Exception:
        raise RuntimeError(
            f"Sign-in failed. HTTP {response.status_code}. Response: {response.text[:500]}"
        )
    auth_token = data["credentials"]["token"]
    site_id = data["credentials"]["site"]["id"]
    print(f"Signed in successfully. Site ID: {site_id}")
    return auth_token, site_id


def sign_out(server_url: str, auth_token: str) -> None:
    """Invalidate the auth token."""
    url = f"{server_url}/api/{API_VERSION}/auth/signout"
    requests.post(url, headers={"x-tableau-auth": auth_token})
    print("Signed out.")


# ---------------------------------------------------------------------------
# Metadata API
# ---------------------------------------------------------------------------

def query_metadata(server_url: str, auth_token: str, luids: list[str]) -> list[dict]:
    """
    Query the Tableau Metadata API for the upstream tables of the given LUIDs.
    Returns a list of datasource nodes, each containing 'luid', 'name',
    and 'upstreamTables'.
    """
    url = f"{server_url}/api/metadata/graphql"
    payload = {
        "query": UPSTREAM_TABLES_QUERY,
        "variables": {"luids": luids},
    }
    response = requests.post(
        url,
        json=payload,
        headers={
            "x-tableau-auth": auth_token,
            "Content-Type": "application/json",
        },
    )
    response.raise_for_status()
    data = response.json()

    if "errors" in data:
        raise RuntimeError(f"Metadata API errors: {json.dumps(data['errors'], indent=2)}")

    return (
        data.get("data", {})
        .get("publishedDatasourcesConnection", {})
        .get("nodes", [])
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def read_input_csv(path: Path) -> list[dict]:
    """Read the datasources CSV and return a list of row dicts."""
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def build_luid_index(rows: list[dict]) -> dict[str, dict]:
    """
    Return a mapping of LUID -> first matching row from the input CSV.
    Used to enrich Metadata API results with the original CSV fields.
    """
    index: dict[str, dict] = {}
    for row in rows:
        luid = row.get("LUID", "").strip()
        if luid and luid not in index:
            index[luid] = row
    return index


def extract_table_lineage(
    server_url: str,
    site_name: str,
    token_name: str,
    pat: str,
    input_csv: Path,
    output_csv: Path,
    batch_size: int = BATCH_SIZE,
) -> list[dict]:
    server_url = server_url.rstrip("/")
    rows = read_input_csv(input_csv)
    print(f"Read {len(rows)} rows from {input_csv.name}")

    luid_index = build_luid_index(rows)
    all_luids = list(luid_index.keys())
    print(f"Unique LUIDs to query: {len(all_luids)}")

    auth_token, _ = sign_in(server_url, site_name, token_name, pat)
    records: list[dict] = []

    try:
        # Process LUIDs in batches to stay within API limits
        for i in range(0, len(all_luids), batch_size):
            batch = all_luids[i : i + batch_size]
            print(f"Querying Metadata API for LUIDs {i + 1}–{i + len(batch)} of {len(all_luids)} ...")
            nodes = query_metadata(server_url, auth_token, batch)

            for node in nodes:
                luid = node.get("luid", "")
                ds_name = node.get("name", "")
                source_row = luid_index.get(luid, {})
                folder_path = source_row.get("Folder Path", "")
                upstream_tables = node.get("upstreamTables", [])

                if upstream_tables:
                    for table in upstream_tables:
                        db = table.get("database", {}) or {}
                        records.append({
                            "LUID": luid,
                            "Datasource Name": ds_name,
                            "Folder Path": folder_path,
                            "Database": db.get("name", ""),
                            "Connection Type": db.get("connectionType", ""),
                            "Schema": table.get("schema", ""),
                            "Table": table.get("name", ""),
                        })
                else:
                    # Datasource found but no upstream table info available
                    records.append({
                        "LUID": luid,
                        "Datasource Name": ds_name,
                        "Folder Path": folder_path,
                        "Database": "",
                        "Connection Type": "",
                        "Schema": "",
                        "Table": "",
                    })

    finally:
        sign_out(server_url, auth_token)

    print(f"Total table records: {len(records)}")

    if records:
        fieldnames = list(records[0].keys())
        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)
        print(f"Results written to: {output_csv}")
    else:
        print("No records to write.")

    return records


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    results = extract_table_lineage(
        server_url=TABLEAU_SERVER_URL,
        site_name=SITE_NAME,
        token_name=TOKEN_NAME,
        pat=PAT,
        input_csv=INPUT_CSV,
        output_csv=OUTPUT_CSV,
    )

    print("\n--- Preview (first 5 records) ---")
    for record in results[:5]:
        print(json.dumps(record, indent=2))
