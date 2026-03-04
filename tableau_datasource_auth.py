"""
Tableau Cloud - Published Datasources Extractor (Auth)
Identical to tableau_cloud_data_sources.py, except:
  - Limited to TEST_LIMIT datasources for testing.
  - Authentication Method is read directly from the downloaded .tds/.tdsx file
    instead of being inferred from the embedPassword/useOAuthManagedKeychain booleans.
"""

import os
import io
import zipfile
import xml.etree.ElementTree as ET
import requests
import json
import csv
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Configuration — same environment variables as the main script:
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
# Test limit — only process this many datasources
# ---------------------------------------------------------------------------
TEST_LIMIT = None

DATASOURCE_TYPE_FILTER = "all"  # "live", "extract", or "all"
DATABASE_TYPE_FILTER   = "snowflake"  # e.g. "snowflake", "redshift", "sqlserver", or "all"

OUTPUT_CSV = Path(__file__).parent / "tableau_datasources_auth_test.csv"

# ---------------------------------------------------------------------------
# Tableau REST API helpers
# ---------------------------------------------------------------------------

API_VERSION = "3.27"

# Maps the raw XML authentication attribute to a human-readable label.
AUTH_LABELS = {
    "snowflake-key-pair-jwt": "Key Pair",
    "username-password":      "Username / Password",
    "oauth":                  "OAuth",
    "serviceaccount":         "Service Account",
}


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
    """Page through all published datasources and return a list of raw dicts."""
    url = f"{server_url}/api/{API_VERSION}/sites/{site_id}/datasources"
    page_size = 100
    page_number = 1
    all_datasources: list[dict] = []

    while True:
        params = {"pageSize": page_size, "pageNumber": page_number}
        response = requests.get(url, headers=_headers(auth_token), params=params)
        response.raise_for_status()
        data = response.json()

        datasources = data.get("datasources", {}).get("datasource", [])
        if isinstance(datasources, dict):
            datasources = [datasources]
        all_datasources.extend(datasources)

        pagination = data.get("pagination", {})
        total = int(pagination.get("totalAvailable", 0))
        if page_number * page_size >= total:
            break
        page_number += 1

    return all_datasources


def get_datasource_connections(
    server_url: str, site_id: str, auth_token: str, datasource_id: str
) -> list[dict]:
    """Return connection objects for a datasource."""
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
    """Resolve a project's full folder path, fetching all pages on first call."""
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
# TDS authentication parsing
# ---------------------------------------------------------------------------

def _parse_auth_from_tds_xml(xml_content: str) -> str:
    """
    Parse a .tds XML string and return the authentication label.
    Searches all <connection> elements for an 'authentication' attribute,
    skipping federated/container nodes that don't carry auth info.
    """
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError:
        return ""

    for conn in root.iter("connection"):
        auth = conn.get("authentication", "").strip()
        # Skip blank values and high-level container connection types
        if auth and conn.get("class") not in ("federated", None.__class__):
            return AUTH_LABELS.get(auth, auth)

    return ""


def get_auth_from_tds(
    server_url: str, site_id: str, auth_token: str, datasource_id: str
) -> str:
    """
    Download the datasource file (.tds or .tdsx) and extract the authentication
    type from the embedded XML connection definition.

    Returns a human-readable label, or an empty string if it cannot be determined.
    """
    url = f"{server_url}/api/{API_VERSION}/sites/{site_id}/datasources/{datasource_id}/content"
    response = requests.get(url, headers={"x-tableau-auth": auth_token})
    response.raise_for_status()

    # A .tdsx is a ZIP archive; a .tds is plain XML.
    # Detect by magic bytes (ZIP starts with PK) rather than Content-Type,
    # which Tableau doesn't always set reliably.
    if response.content[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            tds_names = [n for n in zf.namelist() if n.endswith(".tds")]
            if not tds_names:
                return ""
            xml_content = zf.read(tds_names[0]).decode("utf-8", errors="replace")
    else:
        xml_content = response.text

    return _parse_auth_from_tds_xml(xml_content)


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
    test_limit: Optional[int] = None,
    output_csv: Optional[str] = None,
) -> list[dict]:
    server_url = server_url.rstrip("/")
    auth_token, site_id = sign_in(server_url, site_name, token_name, pat)

    try:
        raw_datasources = get_all_datasources(server_url, site_id, auth_token)
        print(f"Total datasources found: {len(raw_datasources)}")

        if test_limit:
            raw_datasources = raw_datasources[:test_limit]
            print(f"Test limit applied — processing first {test_limit} datasources.")

        project_cache: dict = {}
        records: list[dict] = []

        for i, ds in enumerate(raw_datasources, 1):
            ds_id   = ds.get("id", "")
            ds_name = ds.get("name", "")
            is_extract = ds.get("hasExtracts", False)

            filter_lower = datasource_type_filter.lower()
            if filter_lower == "extract" and not is_extract:
                continue
            if filter_lower == "live" and is_extract:
                continue

            connection_type_label = "Extract" if is_extract else "Live"

            owner = ds.get("owner", {})
            owner_name = owner.get("name", owner.get("id", ""))

            project = ds.get("project", {})
            project_id = project.get("id", "")
            folder_path = get_project_path(
                server_url, site_id, auth_token, project_id, project_cache
            ) if project_id else ""

            # Download TDS and parse authentication type from XML
            print(f"  [{i}/{len(raw_datasources)}] Fetching TDS for: {ds_name}")
            try:
                tds_auth = get_auth_from_tds(server_url, site_id, auth_token, ds_id)
            except Exception as e:
                print(f"    Warning: could not read TDS for {ds_name}: {e}")
                tds_auth = ""

            connections = get_datasource_connections(server_url, site_id, auth_token, ds_id)

            if connections:
                for conn in connections:
                    conn_type      = conn.get("type", "")
                    server_address = conn.get("serverAddress", "")
                    username       = conn.get("userName", "")

                    if database_type_filter.lower() != "all" and conn_type.lower() != database_type_filter.lower():
                        continue

                    # Use TDS-parsed auth if available; fall back to boolean flags.
                    if tds_auth:
                        auth_method = tds_auth
                    elif conn.get("useOAuthManagedKeychain"):
                        auth_method = "OAuth"
                    elif conn.get("embedPassword"):
                        auth_method = "Embedded Password"
                    else:
                        auth_method = "Prompt User"

                    records.append({
                        "LUID":                ds_id,
                        "Name":                ds_name,
                        "Folder Path":         folder_path,
                        "Connection Type":     connection_type_label,
                        "Database Type":       conn_type,
                        "Server URL":          server_address,
                        "Username":            username,
                        "Authentication Method": auth_method,
                        "Owner":               owner_name,
                    })
            else:
                records.append({
                    "LUID":                ds_id,
                    "Name":                ds_name,
                    "Folder Path":         folder_path,
                    "Connection Type":     connection_type_label,
                    "Database Type":       "",
                    "Server URL":          "",
                    "Username":            "",
                    "Authentication Method": tds_auth,
                    "Owner":               owner_name,
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
        test_limit=TEST_LIMIT,
        output_csv=OUTPUT_CSV,
    )

    print("\n--- Preview (first 10 records) ---")
    for record in results[:10]:
        print(json.dumps(record, indent=2))
