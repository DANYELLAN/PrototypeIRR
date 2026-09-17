import os
from pathlib import Path
from urllib.parse import quote, urlparse

import requests
from msal import ConfidentialClientApplication, PublicClientApplication, SerializableTokenCache

from config import get_env_int

TENANT_ID = os.getenv("SHAREPOINT_TENANT_ID", "519943e3-a90d-49f1-a2a4-dd32f586c05f")
CLIENT_ID = os.getenv("SHAREPOINT_CLIENT_ID", "5520a688-ca19-493f-9050-f5c356fbeaff")
CLIENT_SECRET = os.getenv("SHAREPOINT_CLIENT_SECRET", "")
GRAPH_SCOPES = ["https://graph.microsoft.com/.default"]
GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
DEFAULT_TOP = get_env_int("SHAREPOINT_DEFAULT_TOP", 100)
TOKEN_CACHE_FILE = Path(os.getenv("SHAREPOINT_TOKEN_CACHE_FILE", ".msal_token_cache.json"))
PLACEHOLDER_SECRET_VALUES = {
    "your-client-secret",
    "your-sharepoint-client-secret",
    "your-time-entry-app-client-secret",
}
PLACEHOLDER_CLIENT_ID_VALUES = {
    "your-client-id",
    "your-sharepoint-client-id",
    "your-time-entry-app-client-id",
}

APP = None
CACHE = None
APP_CACHE = {}
CACHE_BY_FILE = {}


def _authority(tenant_id):
    return f"https://login.microsoftonline.com/{tenant_id}"


def _get_cache(token_cache_file=TOKEN_CACHE_FILE):
    """Load a persisted MSAL token cache from disk when available."""
    cache_path = Path(token_cache_file)
    if cache_path not in CACHE_BY_FILE:
        cache = SerializableTokenCache()
        if cache_path.exists():
            cache.deserialize(cache_path.read_text(encoding="utf-8"))
        CACHE_BY_FILE[cache_path] = cache
    return CACHE_BY_FILE[cache_path]


def _save_cache(token_cache_file=TOKEN_CACHE_FILE):
    """Persist the MSAL token cache so future refreshes reuse the session."""
    cache_path = Path(token_cache_file)
    cache = _get_cache(cache_path)
    if cache.has_state_changed:
        cache_path.write_text(cache.serialize(), encoding="utf-8")


def get_msal_app():
    """Build the MSAL public client lazily so imports do not trigger network calls."""
    return get_public_msal_app(CLIENT_ID, TENANT_ID, TOKEN_CACHE_FILE)


def get_public_msal_app(client_id, tenant_id, token_cache_file=TOKEN_CACHE_FILE):
    """Build a cached public-client MSAL app for delegated Graph auth."""
    cache_key = ("public", tenant_id, client_id, str(token_cache_file))
    if cache_key not in APP_CACHE:
        APP_CACHE[cache_key] = PublicClientApplication(
            client_id=client_id,
            authority=_authority(tenant_id),
            token_cache=_get_cache(token_cache_file),
        )
    return APP_CACHE[cache_key]


def get_confidential_msal_app(client_id, tenant_id, client_secret):
    """Build a cached confidential-client MSAL app for background Graph auth."""
    cache_key = ("confidential", tenant_id, client_id)
    if cache_key not in APP_CACHE:
        APP_CACHE[cache_key] = ConfidentialClientApplication(
            client_id=client_id,
            authority=_authority(tenant_id),
            client_credential=client_secret,
        )
    return APP_CACHE[cache_key]


class SharePointApiError(Exception):
    """Raised when a SharePoint Graph request cannot be completed."""


def get_access_token(
    tenant_id=None,
    client_id=None,
    client_secret=None,
    token_cache_file=None,
    allow_interactive=True,
):
    """Get an access token via client credentials, token cache, or browser login."""
    selected_tenant_id = tenant_id or TENANT_ID
    selected_client_id = client_id or CLIENT_ID
    selected_client_secret = client_secret if client_secret is not None else CLIENT_SECRET
    selected_token_cache_file = Path(token_cache_file or TOKEN_CACHE_FILE)

    if selected_client_secret:
        app = get_confidential_msal_app(
            selected_client_id,
            selected_tenant_id,
            selected_client_secret,
        )
        result = app.acquire_token_for_client(scopes=GRAPH_SCOPES)
        if "access_token" in result:
            return result["access_token"]
        error_message = result.get("error_description", result)
        raise SharePointApiError(f"Authentication failed: {error_message}")

    app = get_public_msal_app(
        selected_client_id,
        selected_tenant_id,
        selected_token_cache_file,
    )
    accounts = app.get_accounts()
    if accounts:
        result = app.acquire_token_silent(GRAPH_SCOPES, account=accounts[0])
        if result and "access_token" in result:
            _save_cache(selected_token_cache_file)
            return result["access_token"]

    if not allow_interactive:
        raise SharePointApiError("Authentication failed: no cached token and interactive login is disabled.")

    print("Opening browser for sign-in...")
    result = app.acquire_token_interactive(scopes=GRAPH_SCOPES)

    if "access_token" in result:
        _save_cache(selected_token_cache_file)
        return result["access_token"]

    error_message = result.get("error_description", result)
    raise SharePointApiError(f"Authentication failed: {error_message}")


def get_access_token_from_env(prefix="SHAREPOINT", allow_interactive=True):
    """Get a Graph token from a named environment-variable prefix."""
    tenant_id = os.getenv(f"{prefix}_TENANT_ID") or TENANT_ID
    client_id = os.getenv(f"{prefix}_CLIENT_ID") or CLIENT_ID
    if client_id.strip().lower() in PLACEHOLDER_CLIENT_ID_VALUES:
        client_id = CLIENT_ID
    client_secret = os.getenv(f"{prefix}_CLIENT_SECRET", "")
    if client_secret.strip().lower() in PLACEHOLDER_SECRET_VALUES:
        client_secret = ""
    token_cache_file = os.getenv(f"{prefix}_TOKEN_CACHE_FILE") or TOKEN_CACHE_FILE
    return get_access_token(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        token_cache_file=token_cache_file,
        allow_interactive=allow_interactive,
    )


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


def graph_post(endpoint, headers, payload):
    """Issue a POST request to Microsoft Graph and raise a readable error on failure."""
    response = requests.post(
        f"{GRAPH_BASE_URL}{endpoint}",
        headers={**headers, "Content-Type": "application/json"},
        json=payload,
        timeout=30,
    )

    if response.status_code not in {200, 201}:
        raise SharePointApiError(
            f"Graph request failed ({response.status_code}) for {endpoint}: {response.text}"
        )

    return response.json()


def graph_patch(endpoint, headers, payload):
    """Issue a PATCH request to Microsoft Graph and raise a readable error on failure."""
    response = requests.patch(
        f"{GRAPH_BASE_URL}{endpoint}",
        headers={**headers, "Content-Type": "application/json"},
        json=payload,
        timeout=30,
    )

    if response.status_code not in {200, 204}:
        raise SharePointApiError(
            f"Graph request failed ({response.status_code}) for {endpoint}: {response.text}"
        )

    return response.json() if response.text else {}


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
    list_identifier = quote(str(list_name), safe="")
    params = {"expand": "fields"}
    if top:
        params["$top"] = top

    data = graph_get(
        f"/sites/{resolved_site_id}/lists/{list_identifier}/items",
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


def get_list_columns(site_url, list_name, headers, site_id=None):
    """Fetch SharePoint list column metadata so writes can use internal field names."""
    resolved_site_id = site_id or get_site_id(site_url, headers)
    list_identifier = quote(str(list_name), safe="")
    data = graph_get(
        f"/sites/{resolved_site_id}/lists/{list_identifier}/columns",
        headers=headers,
    )
    return data.get("value", [])


def create_list_item(site_url, list_name, fields, headers, site_id=None):
    """Create a SharePoint list item using Microsoft Graph."""
    resolved_site_id = site_id or get_site_id(site_url, headers)
    list_identifier = quote(str(list_name), safe="")
    return graph_post(
        f"/sites/{resolved_site_id}/lists/{list_identifier}/items",
        headers=headers,
        payload={"fields": fields},
    )


def update_list_item_fields(site_url, list_name, item_id, fields, headers, site_id=None):
    """Update SharePoint list item fields using Microsoft Graph."""
    resolved_site_id = site_id or get_site_id(site_url, headers)
    list_identifier = quote(str(list_name), safe="")
    return graph_patch(
        f"/sites/{resolved_site_id}/lists/{list_identifier}/items/{item_id}/fields",
        headers=headers,
        payload=fields,
    )


def parse_list_config(list_config):
    """Return the local list name and Graph list identifier for a sync entry."""
    if isinstance(list_config, dict):
        list_name = list_config.get("name") or list_config.get("title") or list_config.get("id")
        list_identifier = list_config.get("id") or list_name
    else:
        list_name = list_config
        list_identifier = list_config

    return str(list_name), str(list_identifier)


def get_multiple_lists(site_to_lists_map, headers, top=DEFAULT_TOP, fetch_all=False):
    """Fetch multiple lists across multiple SharePoint sites."""
    results = {}
    site_ids = {}

    for site_url, list_names in site_to_lists_map.items():
        site_id = get_site_id(site_url, headers)
        site_ids[site_url] = site_id
        site_results = {}

        for list_config in list_names:
            list_name, list_identifier = parse_list_config(list_config)
            site_results[list_name] = get_list_items(
                site_url,
                list_identifier,
                headers=headers,
                top=top,
                site_id=site_id,
                fetch_all=fetch_all,
            )

        results[site_url] = site_results

    return results, site_ids
