import os
from urllib.parse import urlparse

import requests
from msal import PublicClientApplication

from config import get_env_int

TENANT_ID = os.getenv("SHAREPOINT_TENANT_ID", "519943e3-a90d-49f1-a2a4-dd32f586c05f")
CLIENT_ID = os.getenv("SHAREPOINT_CLIENT_ID", "5520a688-ca19-493f-9050-f5c356fbeaff")
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
GRAPH_SCOPES = ["https://graph.microsoft.com/.default"]
GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
DEFAULT_TOP = get_env_int("SHAREPOINT_DEFAULT_TOP", 100)

APP = PublicClientApplication(client_id=CLIENT_ID, authority=AUTHORITY)


class SharePointApiError(Exception):
    """Raised when a SharePoint Graph request cannot be completed."""


def get_access_token():
    """Get an access token via MSAL cache or interactive browser login."""
    accounts = APP.get_accounts()
    if accounts:
        result = APP.acquire_token_silent(GRAPH_SCOPES, account=accounts[0])
        if result and "access_token" in result:
            return result["access_token"]

    print("Opening browser for sign-in...")
    result = APP.acquire_token_interactive(scopes=GRAPH_SCOPES)

    if "access_token" in result:
        return result["access_token"]

    error_message = result.get("error_description", result)
    raise SharePointApiError(f"Authentication failed: {error_message}")


def build_headers(access_token):
    """Build request headers for Microsoft Graph API calls."""
    return {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }


def parse_site_url(site_url):
    """Extract the SharePoint host and path from a site URL."""
    parsed = urlparse(site_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or not parsed.path:
        raise SharePointApiError(f"Invalid SharePoint site URL: {site_url}")

    site_path = parsed.path.rstrip("/")
    if not site_path:
        raise SharePointApiError(f"Invalid SharePoint site URL: {site_url}")

    return parsed.netloc, site_path


def graph_get(endpoint, headers, params=None):
    """Issue a GET request to Microsoft Graph and raise a readable error on failure."""
    response = requests.get(
        f"{GRAPH_BASE_URL}{endpoint}",
        headers=headers,
        params=params,
        timeout=30,
    )

    if response.status_code != 200:
        raise SharePointApiError(
            f"Graph request failed ({response.status_code}) for {endpoint}: {response.text}"
        )

    return response.json()


def graph_get_absolute(url, headers):
    """Follow an absolute Graph paging URL."""
    response = requests.get(url, headers=headers, timeout=30)

    if response.status_code != 200:
        raise SharePointApiError(
            f"Graph request failed ({response.status_code}) for {url}: {response.text}"
        )

    return response.json()


def get_site_id(site_url, headers):
    """Resolve a SharePoint site URL to its Microsoft Graph site ID."""
    site_host, site_path = parse_site_url(site_url)
    site = graph_get(f"/sites/{site_host}:{site_path}", headers=headers)
    site_id = site.get("id")
    if not site_id:
        raise SharePointApiError(f"Site ID missing from Graph response for {site_url}")
    return site_id


def get_list_items(site_url, list_name, headers, top=DEFAULT_TOP, site_id=None, fetch_all=False):
    """Fetch list items for a SharePoint list."""
    resolved_site_id = site_id or get_site_id(site_url, headers)
    params = {"expand": "fields"}
    if top:
        params["$top"] = top

    data = graph_get(
        f"/sites/{resolved_site_id}/lists/{list_name}/items",
        headers=headers,
        params=params,
    )
    items = data.get("value", [])

    if not fetch_all:
        return items

    next_link = data.get("@odata.nextLink")
    while next_link:
        data = graph_get_absolute(next_link, headers)
        items.extend(data.get("value", []))
        next_link = data.get("@odata.nextLink")

    return items


def get_multiple_lists(site_to_lists_map, headers, top=DEFAULT_TOP, fetch_all=False):
    """Fetch multiple lists across multiple SharePoint sites."""
    results = {}
    site_ids = {}

    for site_url, list_names in site_to_lists_map.items():
        site_id = get_site_id(site_url, headers)
        site_ids[site_url] = site_id
        site_results = {}

        for list_name in list_names:
            site_results[list_name] = get_list_items(
                site_url,
                list_name,
                headers=headers,
                top=top,
                site_id=site_id,
                fetch_all=fetch_all,
            )

        results[site_url] = site_results

    return results, site_ids
