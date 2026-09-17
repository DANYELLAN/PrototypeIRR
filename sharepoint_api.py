import os

from config import get_env_bool
from sharepoint_client import (
    DEFAULT_TOP,
    SharePointApiError,
    build_headers,
    get_access_token,
    get_access_token_from_env,
    get_multiple_lists,
    parse_site_url,
)
from postgres_sync import (
    DatabaseSyncError,
    get_db_connection,
    initialize_database,
    sync_list_to_postgres,
)


ENABLE_POSTGRES_SYNC = get_env_bool("ENABLE_POSTGRES_SYNC", True)

SITE_LISTS = {
    "https://benoitinc.sharepoint.com/sites/QMS1061": [
        "Employees",
        "InspectionRecipes",
    ],
    "https://benoitinc.sharepoint.com/sites/AcumaticaDataStorage": [
        "Production Operations",
    ],
}

WORK_ORDER_SITE_LISTS = {
    "https://benoitinc.sharepoint.com/sites/AcumaticaDataStorage": [
        "Production Operations",
    ],
}

INSPECTION_RECIPES_SITE_URL = os.getenv(
    "INSPECTION_RECIPES_SITE_URL",
    "https://benoitinc.sharepoint.com/sites/QMS1061",
)
INSPECTION_RECIPES_LIST_NAME = os.getenv("INSPECTION_RECIPES_LIST_NAME", "InspectionRecipes")
INSPECTION_RECIPE_SITE_LISTS = {
    INSPECTION_RECIPES_SITE_URL: [
        INSPECTION_RECIPES_LIST_NAME,
    ],
}

CNC_TIME_SITE_LISTS = {
    "https://benoitinc.sharepoint.com/sites/QMS1061": [
        "Employees",
    ],
    "https://benoitinc.sharepoint.com/sites/MachinistTime": [
        "Stations",
        "Ennis Start and Stop Time Inputs",
        "Ennis Machinist Time Entry",
        "Details Type",
    ],
    "https://benoitinc.sharepoint.com/sites/BenoitMaintenance1": [
        "Assets",
        "Locations",
        "Ennis Maintenance Request",
        {
            "name": "CNC Maintenace Pre-Use/Daily Checklist",
            "id": "e5d9d01b-fb27-4abb-8d93-eb285c98c228",
        },
    ],
    "https://benoitinc.sharepoint.com/sites/BenoitIT677": [
        "tblTechnicalSupportCategories",
    ],
    "https://benoitinc.sharepoint.com/sites/AcumaticaDataStorage": [
        "Production Operations",
    ],
}

SITE_LISTS_ENV = os.getenv("SITE_LISTS")
if SITE_LISTS_ENV:
    SITE_LISTS = {
        site_url.strip(): [name.strip() for name in list_names.split(",") if name.strip()]
        for site_url, list_names in (
            entry.split("=", 1) for entry in SITE_LISTS_ENV.split(";") if "=" in entry
        )
    }


def print_item_preview(items, preview_fields=6):
    """Print a compact preview of list item fields."""
    print(f"Found {len(items)} items:")

    for index, item in enumerate(items[:3], 1):
        print(f"\n  Item {index}:")
        fields = item.get("fields", {})
        for key, value in list(fields.items())[:preview_fields]:
            print(f"    {key}: {value}")

    if len(items) > 3:
        print(f"\n  ... {len(items) - 3} more items not shown")


def sync_multiple_lists_to_postgres(connection, site_to_list_results, site_ids, app_key="irr"):
    """Persist multiple SharePoint sites and lists to PostgreSQL."""
    sync_counts = {}

    for site_url, list_results in site_to_list_results.items():
        site_host, site_path = parse_site_url(site_url)
        sync_counts[site_url] = {}

        for list_name, items in list_results.items():
            inserted_count = sync_list_to_postgres(
                connection,
                site_url,
                site_host,
                site_path,
                list_name,
                items,
                site_ids[site_url],
                app_key=app_key,
            )
            sync_counts[site_url][list_name] = inserted_count

    return sync_counts


def sync_sharepoint_lists_to_postgres(
    site_lists=None,
    top=DEFAULT_TOP,
    fetch_all=True,
    app_key="irr",
    token_env_prefix="SHAREPOINT",
    allow_interactive=True,
):
    """Fetch configured SharePoint lists and persist them to PostgreSQL."""
    selected_site_lists = site_lists or SITE_LISTS

    token = get_access_token_from_env(token_env_prefix, allow_interactive=allow_interactive)
    headers = build_headers(token)
    site_results, site_ids = get_multiple_lists(
        selected_site_lists,
        headers=headers,
        top=top,
        fetch_all=fetch_all,
    )

    connection = get_db_connection()
    try:
        initialize_database(connection)
        sync_counts = sync_multiple_lists_to_postgres(
            connection,
            site_results,
            site_ids,
            app_key=app_key,
        )
    finally:
        connection.close()

    return {
        "site_results": site_results,
        "site_ids": site_ids,
        "sync_counts": sync_counts,
    }


def sync_work_orders_to_postgres():
    """Refresh only the Production Operations work-order source list."""
    return sync_sharepoint_lists_to_postgres(
        site_lists=WORK_ORDER_SITE_LISTS,
        fetch_all=True,
        app_key="irr",
        token_env_prefix="SHAREPOINT",
    )


def sync_inspection_recipes_to_postgres():
    """Refresh the SharePoint Digital IRR recipe list used by the inspection app."""
    return sync_sharepoint_lists_to_postgres(
        site_lists=INSPECTION_RECIPE_SITE_LISTS,
        fetch_all=True,
        app_key="irr",
        token_env_prefix="SHAREPOINT",
    )


def sync_cnc_time_lists_to_postgres():
    """Refresh the SharePoint lists used by the CNC Time Entry app."""
    allow_interactive = get_env_bool("CNC_TIME_SHAREPOINT_ALLOW_INTERACTIVE", True)
    return sync_sharepoint_lists_to_postgres(
        site_lists=CNC_TIME_SITE_LISTS,
        fetch_all=True,
        app_key="time_entry",
        token_env_prefix="CNC_TIME_SHAREPOINT",
        allow_interactive=allow_interactive,
    )


def main():
    print("=" * 50)
    print("SharePoint to PostgreSQL Sync")
    print("=" * 50)

    try:
        token = get_access_token()
        headers = build_headers(token)
        print("Authentication successful!")
    except SharePointApiError as error:
        print(error)
        return

    try:
        site_results, site_ids = get_multiple_lists(
            SITE_LISTS,
            headers=headers,
            top=DEFAULT_TOP,
            fetch_all=True,
        )
    except SharePointApiError as error:
        print(f"SharePoint sync failed: {error}")
        return

    for site_url, list_results in site_results.items():
        print(f"\nSite: {site_url}")
        print("Connected to site!")
        for list_name, items in list_results.items():
            print(f"\n--- Reading items from '{list_name}' ---")
            print_item_preview(items)

    if ENABLE_POSTGRES_SYNC and site_results:
        print("\nInitializing PostgreSQL sync...")
        try:
            connection = get_db_connection()
            initialize_database(connection)
            sync_counts = sync_multiple_lists_to_postgres(connection, site_results, site_ids)
            connection.close()

            print("\nPostgreSQL sync complete!")
            for site_url, list_counts in sync_counts.items():
                print(f"\nDatabase site: {site_url}")
                for list_name, item_count in list_counts.items():
                    print(f"  {list_name}: synced {item_count} items")
        except DatabaseSyncError as error:
            print(f"Database sync failed: {error}")
        except Exception as error:
            print(f"Unexpected database sync error: {error}")

    print("\n" + "=" * 50)
    print("Run complete!")
    print("=" * 50)


if __name__ == "__main__":
    main()
