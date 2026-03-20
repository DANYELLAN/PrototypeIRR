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
