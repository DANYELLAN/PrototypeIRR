CREATE TABLE IF NOT EXISTS app_locations (
    id SERIAL PRIMARY KEY,
    location_name TEXT NOT NULL UNIQUE,
    branch TEXT,
    machine_code TEXT,
    device_name TEXT,
    device_type TEXT NOT NULL DEFAULT 'manual',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS inspector_sessions (
    id BIGSERIAL PRIMARY KEY,
    inspector_item_id BIGINT,
    inspector_adp TEXT NOT NULL,
    inspector_name TEXT NOT NULL,
    branch TEXT,
    shift TEXT NOT NULL,
    location_id INTEGER REFERENCES app_locations(id),
    location_name TEXT,
    cnc_operator_item_id BIGINT,
    cnc_operator_name TEXT,
    logged_in_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    logged_out_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS manager_credentials (
    id SERIAL PRIMARY KEY,
    employee_item_id BIGINT NOT NULL UNIQUE,
    manager_name TEXT NOT NULL,
    manager_adp TEXT,
    branch TEXT,
    pin_salt TEXT NOT NULL,
    pin_hash TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS recipe_aliases (
    id SERIAL PRIMARY KEY,
    branch TEXT,
    operation_description TEXT NOT NULL,
    recipe_name TEXT NOT NULL,
    confidence_rank INTEGER NOT NULL DEFAULT 100,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (branch, operation_description, recipe_name)
);

CREATE TABLE IF NOT EXISTS pipe_units (
    id BIGSERIAL PRIMARY KEY,
    production_number TEXT NOT NULL,
    operation_description TEXT NOT NULL,
    pipe_number TEXT NOT NULL,
    branch TEXT,
    current_status TEXT NOT NULL DEFAULT 'pending',
    latest_attempt_no INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (production_number, operation_description, pipe_number)
);

CREATE TABLE IF NOT EXISTS inspection_attempts (
    id BIGSERIAL PRIMARY KEY,
    pipe_unit_id BIGINT NOT NULL REFERENCES pipe_units(id) ON DELETE CASCADE,
    session_id BIGINT REFERENCES inspector_sessions(id),
    attempt_no INTEGER NOT NULL,
    inspector_item_id BIGINT,
    inspector_name TEXT NOT NULL,
    cnc_operator_item_id BIGINT,
    cnc_operator_name TEXT,
    recipe_name TEXT,
    recipe_item_id BIGINT,
    status TEXT NOT NULL DEFAULT 'in_progress',
    requires_manager_approval BOOLEAN NOT NULL DEFAULT FALSE,
    manager_name TEXT,
    manager_pin_verified BOOLEAN NOT NULL DEFAULT FALSE,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    notes TEXT,
    UNIQUE (pipe_unit_id, attempt_no)
);

CREATE TABLE IF NOT EXISTS inspection_measurements (
    id BIGSERIAL PRIMARY KEY,
    attempt_id BIGINT NOT NULL REFERENCES inspection_attempts(id) ON DELETE CASCADE,
    element_sequence INTEGER,
    element_description TEXT NOT NULL,
    dwg_dim TEXT,
    gauge TEXT,
    measured_value TEXT,
    pass_fail TEXT,
    inspected_this_pipe BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ncr_reports (
    id BIGSERIAL PRIMARY KEY,
    attempt_id BIGINT NOT NULL REFERENCES inspection_attempts(id) ON DELETE CASCADE,
    pipe_unit_id BIGINT NOT NULL REFERENCES pipe_units(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'open',
    disposition TEXT,
    tier_code TEXT,
    nonconformance TEXT,
    immediate_containment TEXT,
    opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    closed_at TIMESTAMPTZ
);
