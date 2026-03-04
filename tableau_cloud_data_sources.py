"""
Tableau Cloud - Published Datasources Extractor
Uses the Tableau REST API to retrieve published datasources and their details.
"""

import os
import requests
import json
import csv
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Configuration — set these environment variables before running:
#   export TABLEAU_SERVER_URL="https://us-east-1.online.tableau.com"
#   export TABLEAU_SITE_NAME="penske"
#   export TABLEAU_PAT_NAME="api_calls"
#   export TABLEAU_PAT_SECRET="your-token-value"
# ---------------------------------------------------------------------------

TABLEAU_SERVER_URL = os.environ.get("TABLEAU_SERVER_URL")
SITE_NAME         = os.environ.get("TABLEAU_SITE_NAME")
TOKEN_NAME        = os.environ.get("TABLEAU_PAT_NAME")
PAT               = os.environ.get("TABLEAU_PAT_SECRET")

_missing = [k for k, v in {
    "TABLEAU_SERVER_URL": TABLEAU_SERVER_URL,
    "TABLEAU_SITE_NAME":  SITE_NAME,
    "TABLEAU_PAT_NAME":   TOKEN_NAME,
    "TABLEAU_PAT_SECRET": PAT,
}.items() if not v]
if _missing:
    raise EnvironmentError(
        f"Missing required environment variable(s): {', '.join(_missing)}\n"
        "Set them in your shell before running the script."
    )

# ---------------------------------------------------------------------------
# Datasource type filter
# Supported values (case-insensitive):
#   "all"        – return every datasource
#   "extract"    – only extract-based datasources
#   "live"       – only live-connection datasources
# ---------------------------------------------------------------------------
DATASOURCE_TYPE_FILTER = "all"

# ---------------------------------------------------------------------------
# Database / connector type filter
# Filters by the underlying connection type reported by the Tableau API.
# Common values (case-insensitive): "snowflake", "mysql", "postgres",
#   "excel-direct", "textscan", "sqlserver", "bigquery", "oracle", "redshift"
# Set to "all" to include every connector type.
# ---------------------------------------------------------------------------
DATABASE_TYPE_FILTER = "snowflake"

# Output CSV file path — saves to the same directory as this script.
# Change the filename here if needed, or set to None to skip CSV export.
OUTPUT_CSV = Path(__file__).parent / "tableau_datasources.csv"

# ---------------------------------------------------------------------------
# Tableau REST API helpers
# ---------------------------------------------------------------------------

XMLNS = "http://tableau.com/api"
API_VERSION = "3.27"


def _headers(token: str) -> dict:
    return {
        "x-tableau-auth": token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


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
    requests.post(url, headers=_headers(auth_token))
    print("Signed out.")


# ---------------------------------------------------------------------------
# Data retrieval
# ---------------------------------------------------------------------------

def get_all_datasources(server_url: str, site_id: str, auth_token: str) -> list[dict]:
    """
    Page through all published datasources on the site and return a list of
    raw datasource dicts from the REST API.
    """
    url = f"{server_url}/api/{API_VERSION}/sites/{site_id}/datasources"
    page_size = 100
    page_number = 1
    all_datasources: list[dict] = []

    while True:
        params = {
            "pageSize": page_size,
            "pageNumber": page_number,
        }
        response = requests.get(url, headers=_headers(auth_token), params=params)
        response.raise_for_status()
        data = response.json()

        datasources = data.get("datasources", {}).get("datasource", [])
        if isinstance(datasources, dict):
            # Single result returned as object instead of list
            datasources = [datasources]

        all_datasources.extend(datasources)

        pagination = data.get("pagination", {})
        total = int(pagination.get("totalAvailable", 0))
        fetched = page_number * page_size

        if fetched >= total:
            break
        page_number += 1

    return all_datasources


def get_datasource_connections(
    server_url: str, site_id: str, auth_token: str, datasource_id: str
) -> list[dict]:
    """
    Return the list of connection objects for a given datasource.
    Each connection contains serverAddress, username, connectionType, etc.
    """
    url = f"{server_url}/api/{API_VERSION}/sites/{site_id}/datasources/{datasource_id}/connections"
    response = requests.get(url, headers=_headers(auth_token))
    response.raise_for_status()
    data = response.json()
    connections = data.get("connections", {}).get("connection", [])
    if isinstance(connections, dict):
        connections = [connections]
    return connections


def get_project_path(
    server_url: str, site_id: str, auth_token: str, project_id: str, project_cache: dict
) -> str:
    """
    Resolve a project's full folder path (e.g. "Sales / Q1 Reports").
    All projects are fetched with pagination on the first call and cached;
    subsequent calls use the cache directly.
    """
    # "_loaded" is a sentinel that indicates every page of projects has been
    # fetched and every project ID has been added to the cache.
    if "_loaded" not in project_cache:
        all_projects: list[dict] = []
        page_size = 100
        page_number = 1

        while True:
            url = f"{server_url}/api/{API_VERSION}/sites/{site_id}/projects"
            params = {"pageSize": page_size, "pageNumber": page_number}
            response = requests.get(url, headers=_headers(auth_token), params=params)
            response.raise_for_status()
            data = response.json()

            projects = data.get("projects", {}).get("project", [])
            if isinstance(projects, dict):
                projects = [projects]
            all_projects.extend(projects)

            pagination = data.get("pagination", {})
            total = int(pagination.get("totalAvailable", 0))
            if page_number * page_size >= total:
                break
            page_number += 1

        # Build a lookup: id -> project, then resolve each full path
        by_id = {p["id"]: p for p in all_projects}

        def build_path(pid: str) -> str:
            if pid not in by_id:
                return ""
            project = by_id[pid]
            parent_id = project.get("parentProjectId")
            name = project.get("name", "")
            if parent_id:
                parent_path = build_path(parent_id)
                return f"{parent_path} / {name}" if parent_path else name
            return name

        for p in all_projects:
            project_cache[p["id"]] = build_path(p["id"])

        project_cache["_loaded"] = True

    return project_cache.get(project_id, "")


# ---------------------------------------------------------------------------
# Main extraction logic
# ---------------------------------------------------------------------------

def extract_datasources(
    server_url: str,
    site_name: str,
    token_name: str,
    pat: str,
    datasource_type_filter: str = "all",
    database_type_filter: str = "all",
    output_csv: Optional[str] = None,
) -> list[dict]:
    """
    Connect to Tableau Cloud, retrieve all published datasources, and return
    a list of records with the requested fields.

    datasource_type_filter:
        "all"     – no filter
        "extract" – only extract-based datasources
        "live"    – only live-connection datasources

    database_type_filter:
        "all"       – no filter
        "snowflake" – only Snowflake connections
        "mysql"     – only MySQL connections
        "excel-direct", "sqlserver", "postgres", etc.
        Comparison is case-insensitive.
    """
    server_url = server_url.rstrip("/")
    auth_token, site_id = sign_in(server_url, site_name, token_name, pat)

    try:
        raw_datasources = get_all_datasources(server_url, site_id, auth_token)
        print(f"Total datasources found: {len(raw_datasources)}")

        project_cache: dict = {}
        records: list[dict] = []

        for ds in raw_datasources:
            ds_id = ds.get("id", "")
            ds_name = ds.get("name", "")
            is_extract = ds.get("hasExtracts", False)  # True = extract, False = live

            # Apply type filter
            filter_lower = datasource_type_filter.lower()
            if filter_lower == "extract" and not is_extract:
                continue
            if filter_lower == "live" and is_extract:
                continue

            connection_type_label = "Extract" if is_extract else "Live"

            # Owner
            owner = ds.get("owner", {})
            owner_name = owner.get("name", owner.get("id", ""))

            # Project / folder path
            project = ds.get("project", {})
            project_id = project.get("id", "")
            folder_path = get_project_path(
                server_url, site_id, auth_token, project_id, project_cache
            ) if project_id else ""

            # Connections (server URL and username)
            connections = get_datasource_connections(server_url, site_id, auth_token, ds_id)

            # A datasource may have multiple connections; include one row per connection.
            if connections:
                for conn in connections:
                    server_address = conn.get("serverAddress", "")
                    username = conn.get("userName", "")
                    conn_type = conn.get("type", "")

                    if conn.get("useOAuthManagedKeychain"):
                        auth_method = "OAuth"
                    elif conn.get("embedPassword"):
                        auth_method = "Embedded Password"
                    else:
                        auth_method = "Prompt User"

                    # Apply database/connector type filter
                    if database_type_filter.lower() != "all" and conn_type.lower() != database_type_filter.lower():
                        continue

                    records.append({
                        "LUID": ds_id,
                        "Name": ds_name,
                        "Folder Path": folder_path,
                        "Connection Type": connection_type_label,
                        "Database Type": conn_type,
                        "Server URL": server_address,
                        "Username": username,
                        "Authentication Method": auth_method,
                        "Owner": owner_name,
                    })
            else:
                # No connection details available (embedded or unsupported)
                records.append({
                    "LUID": ds_id,
                    "Name": ds_name,
                    "Folder Path": folder_path,
                    "Connection Type": connection_type_label,
                    "Database Type": "",
                    "Server URL": "",
                    "Username": "",
                    "Authentication Method": "",
                    "Owner": owner_name,
                })

        print(f"Records after filters (type='{datasource_type_filter}', db='{database_type_filter}'): {len(records)}")

        if output_csv:
            _write_csv(records, output_csv)

        return records

    finally:
        sign_out(server_url, auth_token)


def _write_csv(records: list[dict], filepath: str) -> None:
    if not records:
        print("No records to write.")
        return
    fieldnames = list(records[0].keys())
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    print(f"Results written to: {filepath}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    results = extract_datasources(
        server_url=TABLEAU_SERVER_URL,
        site_name=SITE_NAME,
        token_name=TOKEN_NAME,
        pat=PAT,
        datasource_type_filter=DATASOURCE_TYPE_FILTER,
        database_type_filter=DATABASE_TYPE_FILTER,
        output_csv=OUTPUT_CSV,
    )

    # Pretty-print a preview of the first 5 records
    print("\n--- Preview (first 5 records) ---")
    for record in results[:5]:
        print(json.dumps(record, indent=2))
