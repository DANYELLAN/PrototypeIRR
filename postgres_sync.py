import os

try:
    import psycopg2
    from psycopg2.extras import Json
except ImportError:
    psycopg2 = None
    Json = None

from config import get_env_int


CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS sharepoint_sites (
    id SERIAL PRIMARY KEY,
    site_url TEXT NOT NULL UNIQUE,
    site_host TEXT NOT NULL,
    site_path TEXT NOT NULL,
    graph_site_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sharepoint_lists (
    id SERIAL PRIMARY KEY,
    site_id INTEGER NOT NULL REFERENCES sharepoint_sites(id) ON DELETE CASCADE,
    list_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (site_id, list_name)
);

CREATE TABLE IF NOT EXISTS sharepoint_items (
    id BIGSERIAL PRIMARY KEY,
    list_id INTEGER NOT NULL REFERENCES sharepoint_lists(id) ON DELETE CASCADE,
    sharepoint_item_id TEXT NOT NULL,
    etag TEXT,
    web_url TEXT,
    created_datetime TIMESTAMPTZ,
    last_modified_datetime TIMESTAMPTZ,
    fields_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    raw_item_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (list_id, sharepoint_item_id)
);
"""

class DatabaseSyncError(Exception):
    """Raised when PostgreSQL setup or sync cannot be completed."""


def get_db_config():
    """Read PostgreSQL connection settings from environment variables."""
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": get_env_int("POSTGRES_PORT", 8084),
        "dbname": os.getenv("POSTGRES_DB", "benoitirr"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "B3n01tI4I9"),
    }


def get_db_connection():
    """Create a psycopg2 connection using environment-driven configuration."""
    if psycopg2 is None:
        raise DatabaseSyncError(
            "psycopg2 is not installed. Install it with: pip install psycopg2-binary"
        )

    try:
        return psycopg2.connect(**get_db_config())
    except Exception as error:
        raise DatabaseSyncError(f"Failed to connect to PostgreSQL: {error}") from error


def initialize_database(connection):
    """Create the baseline tables used for SharePoint sync."""
    with connection.cursor() as cursor:
        cursor.execute(CREATE_TABLES_SQL)
    connection.commit()


def upsert_site(connection, site_url, site_host, site_path, graph_site_id):
    """Insert or update a SharePoint site row and return its database ID."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO sharepoint_sites (site_url, site_host, site_path, graph_site_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (site_url) DO UPDATE
            SET site_host = EXCLUDED.site_host,
                site_path = EXCLUDED.site_path,
                graph_site_id = EXCLUDED.graph_site_id,
                updated_at = NOW()
            RETURNING id
            """,
            (site_url, site_host, site_path, graph_site_id),
        )
        site_id = cursor.fetchone()[0]

    connection.commit()
    return site_id


def upsert_list(connection, site_db_id, list_name):
    """Insert or update a SharePoint list row and return its database ID."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO sharepoint_lists (site_id, list_name)
            VALUES (%s, %s)
            ON CONFLICT (site_id, list_name) DO UPDATE
            SET updated_at = NOW()
            RETURNING id
            """,
            (site_db_id, list_name),
        )
        list_db_id = cursor.fetchone()[0]

    connection.commit()
    return list_db_id


def upsert_items(connection, list_db_id, items):
    """Upsert SharePoint items into PostgreSQL as JSONB payloads."""
    if not items:
        return 0

    with connection.cursor() as cursor:
        for item in items:
            cursor.execute(
                """
                INSERT INTO sharepoint_items (
                    list_id,
                    sharepoint_item_id,
                    etag,
                    web_url,
                    created_datetime,
                    last_modified_datetime,
                    fields_json,
                    raw_item_json,
                    synced_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (list_id, sharepoint_item_id) DO UPDATE
                SET etag = EXCLUDED.etag,
                    web_url = EXCLUDED.web_url,
                    created_datetime = EXCLUDED.created_datetime,
                    last_modified_datetime = EXCLUDED.last_modified_datetime,
                    fields_json = EXCLUDED.fields_json,
                    raw_item_json = EXCLUDED.raw_item_json,
                    synced_at = NOW()
                """,
                (
                    list_db_id,
                    str(item.get("id")),
                    item.get("eTag"),
                    item.get("webUrl"),
                    item.get("createdDateTime"),
                    item.get("lastModifiedDateTime"),
                    Json(item.get("fields", {})),
                    Json(item),
                ),
            )

    connection.commit()
    return len(items)


def sync_list_to_postgres(
    connection, site_url, site_host, site_path, list_name, items, graph_site_id
):
    """Persist one SharePoint list and its items to PostgreSQL."""
    site_db_id = upsert_site(connection, site_url, site_host, site_path, graph_site_id)
    list_db_id = upsert_list(connection, site_db_id, list_name)
    return upsert_items(connection, list_db_id, items)
