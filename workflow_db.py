import json
import hashlib
import hmac
import os
from datetime import datetime, time

from psycopg2.extras import RealDictCursor

from config import get_env_bool, get_env_int
from field_mappings import (
    EMPLOYEE_ADP_FIELDS,
    EMPLOYEE_BRANCH_FIELDS,
    EMPLOYEE_DEPARTMENT_FIELDS,
    EMPLOYEE_MACHINIST_FIELDS,
    EMPLOYEE_NAME_FIELDS,
    EMPLOYEE_ROLE_FIELDS,
    EMPLOYEE_STATUS_FIELDS,
    PRODUCTION_BRANCH_FIELDS,
    PRODUCTION_NUMBER_FIELDS,
    PRODUCTION_OPERATION_DESCRIPTION_FIELDS,
    PRODUCTION_ORDER_TYPE_FIELDS,
    PRODUCTION_STATUS_FIELDS,
    RECIPE_APPROVAL_RULES_FIELDS,
    RECIPE_BRANCH_FIELDS,
    RECIPE_CONNECTION_TYPE_FIELDS,
    RECIPE_DWG_DIM_FIELDS,
    RECIPE_ELEMENT_DESCRIPTION_FIELDS,
    RECIPE_GAUGE_FIELDS,
    RECIPE_JSON_FIELDS,
    RECIPE_MIN_MAX_RULES_FIELDS,
    RECIPE_NAME_FIELDS,
    RECIPE_VERSION_FIELDS,
)
from postgres_sync import get_db_connection


WORKFLOW_SCHEMA_SQL = """
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

CREATE TABLE IF NOT EXISTS app_recipe_headers (
    id BIGSERIAL PRIMARY KEY,
    branch TEXT,
    recipe_name TEXT NOT NULL,
    connection_type TEXT NOT NULL,
    size_label TEXT,
    weight_label TEXT,
    grade_label TEXT,
    connector_type TEXT,
    drawing TEXT,
    source_report TEXT,
    recipe_version INTEGER NOT NULL DEFAULT 1,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (branch, recipe_name)
);

CREATE TABLE IF NOT EXISTS app_recipe_elements (
    id BIGSERIAL PRIMARY KEY,
    recipe_header_id BIGINT NOT NULL REFERENCES app_recipe_headers(id) ON DELETE CASCADE,
    element_sequence INTEGER NOT NULL,
    element_description TEXT NOT NULL,
    measurement_mode TEXT NOT NULL,
    dwg_dim TEXT,
    gauge TEXT,
    capture_type TEXT NOT NULL DEFAULT 'numeric',
    value_format TEXT,
    frequency TEXT NOT NULL DEFAULT 'every_pipe',
    nominal NUMERIC,
    min_value NUMERIC,
    max_value NUMERIC,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (recipe_header_id, element_sequence)
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
    inspection_scope TEXT NOT NULL DEFAULT 'standard',
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
"""

DEFAULT_LOCATIONS = (
    ("Machine 1", "QMS", "M1", "station-machine-1", "machine"),
    ("Machine 2", "QMS", "M2", "station-machine-2", "machine"),
    ("Floating Tablet", "QMS", None, "tablet", "tablet"),
)
DEFAULT_ALWAYS_INSPECT_COUNT = get_env_int("RECIPE_ALWAYS_INSPECT_COUNT", 9)
DEFAULT_ROTATING_COUNT = get_env_int("RECIPE_ROTATING_COUNT_PER_PIPE", 1)
ENABLE_TEST_WORK_ORDERS = get_env_bool("ENABLE_TEST_WORK_ORDERS", True)
MANAGER_ROLE_KEYWORDS = {"manager", "supervisor"}
ADMIN_DEPARTMENT_KEYWORDS = {"it", "information technology"}
ADMIN_ROLE_KEYWORDS = {
    "it",
    "information technology",
    "integration technologist",
    "integration technologies",
}
TEST_WORK_ORDER_TEMPLATE = {
    "item_id": "test-wo-0001",
    "production_number": "TEST-IRR-0001",
    "order_type": "EN",
    "status": "Released",
    "operation_description": 'Turn & Bore on PIN End with Size: 2 7/8", Weight: 7.90#, Connection: BTS-6 Pin as Per Benoit Print# 013 - Rev. 2',
}
DEFAULT_RECIPE_ELEMENT_OPTIONS = [
    "1st Thread Diameter",
    "Pin Nose Diameter",
    "Length to Mid Shoulder",
    "Thread Height",
    "Standoff",
    "General Appearance",
    "Outside Diameter",
    "Inside Diameter",
    "Length to End of 0.090 Dia. Ball",
    "White Lead Test",
    "2nd Thread Diameter",
    "Overall length",
    "Middle Shoulder Diameter",
    "1st Thread Start",
    "1st Thread Pullout",
    "2nd Thread Start",
    "2nd Thread Pullout",
    "Lead (6P)",
    "MRP Verification",
]
DEFAULT_GAUGE_OPTIONS = [
    "MRP",
    "Caliper",
    "Dig Depth Mic",
    "TH Gauge",
    "Standoff and Feeler Gauge",
    "Visual",
    "TMic/Caliper",
    "DigDepth Mic",
    "Prussian Blue",
    "T-Mic/Caliper",
    "Lead Gauge",
]


def initialize_workflow_schema():
    """Create app workflow tables and seed a few default locations."""
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(WORKFLOW_SCHEMA_SQL)
            cursor.execute(
                """
                ALTER TABLE inspection_attempts
                ADD COLUMN IF NOT EXISTS inspection_scope TEXT NOT NULL DEFAULT 'standard'
                """
            )
            for location_name, branch, machine_code, device_name, device_type in DEFAULT_LOCATIONS:
                cursor.execute(
                    """
                    INSERT INTO app_locations (
                        location_name, branch, machine_code, device_name, device_type
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (location_name) DO NOTHING
                    """,
                    (location_name, branch, machine_code, device_name, device_type),
                )
        connection.commit()
    finally:
        connection.close()


def determine_shift(now=None):
    """Return day or night shift using the stated plant rules."""
    current_time = (now or datetime.now()).time()
    if time(5, 0) <= current_time < time(16, 0):
        return "Day"
    return "Night"


def _fetch_all_dicts(query, params=None):
    connection = get_db_connection()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params or ())
            return list(cursor.fetchall())
    finally:
        connection.close()


def _fetch_one_dict(query, params=None):
    rows = _fetch_all_dicts(query, params)
    return rows[0] if rows else None


def _candidate_value(fields, candidates):
    lowered = {str(key).lower(): value for key, value in fields.items()}
    for candidate in candidates:
        value = lowered.get(candidate.lower())
        if value not in (None, ""):
            return value
    return None


def _normalize_identifier(value):
    """Normalize user-entered and SharePoint identifiers for exact matching."""
    if value is None:
        return ""

    text = str(value).strip()
    if not text:
        return ""

    if text.endswith(".0"):
        text = text[:-2]

    return text


def _normalize_employee_row(row):
    fields = row["fields_json"] or {}
    return {
        "item_id": row["sharepoint_item_id"],
        "name": _candidate_value(fields, EMPLOYEE_NAME_FIELDS),
        "adp_number": _normalize_identifier(_candidate_value(fields, EMPLOYEE_ADP_FIELDS)),
        "employee_status": _candidate_value(fields, EMPLOYEE_STATUS_FIELDS),
        "branch": _candidate_value(fields, EMPLOYEE_BRANCH_FIELDS),
        "department": _candidate_value(fields, EMPLOYEE_DEPARTMENT_FIELDS),
        "role_title": _candidate_value(fields, EMPLOYEE_ROLE_FIELDS),
        "machinist": str(
            _candidate_value(fields, EMPLOYEE_MACHINIST_FIELDS) or ""
        ).strip().lower()
        in {"1", "true", "yes", "y"},
    }


def _normalize_production_order_row(row):
    fields = row["fields_json"] or {}
    return {
        "item_id": row["sharepoint_item_id"],
        "production_number": str(_candidate_value(fields, PRODUCTION_NUMBER_FIELDS) or ""),
        "order_type": _candidate_value(fields, PRODUCTION_ORDER_TYPE_FIELDS),
        "status": _candidate_value(fields, PRODUCTION_STATUS_FIELDS),
        "operation_description": _candidate_value(fields, PRODUCTION_OPERATION_DESCRIPTION_FIELDS),
        "branch": _candidate_value(fields, PRODUCTION_BRANCH_FIELDS),
    }


def _normalize_recipe_row(row):
    fields = row["fields_json"] or {}
    recipe_json = _parse_json_value(_candidate_value(fields, RECIPE_JSON_FIELDS))
    min_max_rules = _parse_json_value(_candidate_value(fields, RECIPE_MIN_MAX_RULES_FIELDS))
    approval_rules = _parse_json_value(_candidate_value(fields, RECIPE_APPROVAL_RULES_FIELDS))
    return {
        "item_id": row["sharepoint_item_id"],
        "recipe_name": _candidate_value(fields, RECIPE_NAME_FIELDS),
        "connection_type": _candidate_value(fields, RECIPE_CONNECTION_TYPE_FIELDS),
        "recipe_version": _candidate_value(fields, RECIPE_VERSION_FIELDS),
        "recipe_json": recipe_json,
        "min_max_rules": min_max_rules if isinstance(min_max_rules, list) else [],
        "approval_rules": approval_rules if isinstance(approval_rules, list) else [],
        "element_description": _candidate_value(fields, RECIPE_ELEMENT_DESCRIPTION_FIELDS),
        "dwg_dim": _candidate_value(fields, RECIPE_DWG_DIM_FIELDS),
        "gauge": _candidate_value(fields, RECIPE_GAUGE_FIELDS),
        "branch": _candidate_value(fields, RECIPE_BRANCH_FIELDS),
    }


def _recipe_definition_from_local(header_row, element_rows):
    elements = []
    always_items = []
    rotating_items = []

    for row in element_rows:
        sequence = row["element_sequence"]
        frequency = row["frequency"] or "every_pipe"
        if frequency == "rotating":
            rotating_items.append(sequence)
        else:
            always_items.append(sequence)
        elements.append(
            {
                "item_id": f"local-recipe-{header_row['id']}",
                "element_sequence": sequence,
                "element_description": row["element_description"],
                "dwg_dim": row["dwg_dim"],
                "gauge": row["gauge"],
                "capture_type": row["capture_type"] or "numeric",
                "value_format": row["value_format"],
                "frequency": frequency,
                "nominal": float(row["nominal"]) if row["nominal"] is not None else None,
                "min": float(row["min_value"]) if row["min_value"] is not None else None,
                "max": float(row["max_value"]) if row["max_value"] is not None else None,
                "notes": row["notes"],
            }
        )

    sampling_plan = {
        "alwaysMeasureItems": always_items,
        "rotatingAuditItems": rotating_items,
    }
    if rotating_items:
        cycle_map = {str(index + 1): item for index, item in enumerate(rotating_items)}
        sampling_plan["cycleMap"] = cycle_map
        sampling_plan["rule"] = (
            f"Measure items {', '.join(map(str, always_items))} on every pipe. "
            f"Measure one additional rotating item from {', '.join(map(str, rotating_items))} on each pipe."
            if always_items
            else f"Measure one rotating item from {', '.join(map(str, rotating_items))} on each pipe."
        )

    return {
        "recipe_name": header_row["recipe_name"],
        "connection_type": header_row["connection_type"],
        "recipe_version": header_row["recipe_version"],
        "drawing": header_row["drawing"],
        "source_report": header_row["source_report"],
        "sampling_plan": sampling_plan,
        "approval_rules": [
            {"condition": "numeric_out_of_spec", "requiresApproval": True},
            {"condition": "visual_fail", "requiresApproval": True},
        ],
        "elements": elements,
    }


def _parse_json_value(value):
    if value in (None, ""):
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return None


def _coerce_bigint(value):
    if value is None:
        return None
    text = str(value).strip()
    return int(text) if text.isdigit() else None


def _hash_pin(pin, salt):
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        str(pin).encode("utf-8"),
        salt.encode("utf-8"),
        120000,
    )
    return digest.hex()


def _count_decimal_places(value):
    text = str(value)
    if "." not in text:
        return 0
    return len(text.split(".")[1].rstrip("0")) or 0


def get_locations():
    """Return active locations for the login screen."""
    return _fetch_all_dicts(
        """
        SELECT id, location_name, branch, machine_code, device_type
        FROM app_locations
        WHERE is_active = TRUE
        ORDER BY location_name
        """
    )


def get_employee_by_adp(adp_number):
    """Lookup an employee from the synced Employees SharePoint list."""
    normalized_adp = _normalize_identifier(adp_number)
    if not normalized_adp:
        return None

    rows = _fetch_all_dicts(
        """
        SELECT si.sharepoint_item_id, si.fields_json
        FROM sharepoint_items si
        JOIN sharepoint_lists sl ON sl.id = si.list_id
        WHERE sl.list_name = 'Employees'
        ORDER BY si.id
        """
    )

    for row in rows:
        employee = _normalize_employee_row(row)
        if employee["employee_status"] not in {None, "", "Active"}:
            continue
        if employee["adp_number"] == normalized_adp:
            return employee
    return None


def get_cnc_operators(branch):
    """Return machinists for the same branch as the inspector."""
    rows = _fetch_all_dicts(
        """
        SELECT si.sharepoint_item_id, si.fields_json
        FROM sharepoint_items si
        JOIN sharepoint_lists sl ON sl.id = si.list_id
        WHERE sl.list_name = 'Employees'
        ORDER BY si.id
        """
    )

    operators = []
    for row in rows:
        employee = _normalize_employee_row(row)
        if employee["employee_status"] not in {None, "", "Active"}:
            continue
        if employee["machinist"] and employee["name"]:
            if not branch or employee["branch"] == branch:
                operators.append(employee)

    operators.sort(key=lambda item: item["name"])
    return operators


def get_manager_candidates(branch=None):
    """Return active employees likely eligible for manager approval."""
    rows = _fetch_all_dicts(
        """
        SELECT si.sharepoint_item_id, si.fields_json
        FROM sharepoint_items si
        JOIN sharepoint_lists sl ON sl.id = si.list_id
        WHERE sl.list_name = 'Employees'
        ORDER BY si.id
        """
    )

    managers = []
    for row in rows:
        employee = _normalize_employee_row(row)
        if employee["employee_status"] not in {None, "", "Active"}:
            continue
        if not employee["name"]:
            continue
        if branch and employee["branch"] not in {None, "", branch}:
            continue

        role_title = (employee.get("role_title") or "").strip().lower()
        if any(keyword in role_title for keyword in MANAGER_ROLE_KEYWORDS):
            managers.append(employee)

    managers.sort(key=lambda item: item["name"])
    return managers


def is_manager_or_supervisor(employee):
    """Return True when the synced employee role indicates admin-tool access."""
    if not employee:
        return False
    if employee.get("employee_status") not in {None, "", "Active"}:
        return False

    role_title = (employee.get("role_title") or "").strip().lower()
    return any(keyword in role_title for keyword in MANAGER_ROLE_KEYWORDS)


def is_admin_user(employee):
    """Return True when the user should have admin-tool access."""
    if is_manager_or_supervisor(employee):
        return True

    if not employee:
        return False
    if employee.get("employee_status") not in {None, "", "Active"}:
        return False

    department = (employee.get("department") or "").strip().lower()
    role_title = (employee.get("role_title") or "").strip().lower()
    return (
        department in ADMIN_DEPARTMENT_KEYWORDS
        or any(keyword in role_title for keyword in ADMIN_ROLE_KEYWORDS)
    )


def set_manager_pin(manager_employee, pin):
    """Create or update a manager PIN using a salted PBKDF2 hash."""
    normalized_pin = str(pin).strip()
    if not normalized_pin:
        raise ValueError("PIN cannot be blank.")

    employee_item_id = _coerce_bigint(manager_employee["item_id"])
    if employee_item_id is None:
        raise ValueError("Manager employee item ID is invalid.")

    salt = os.urandom(16).hex()
    pin_hash = _hash_pin(normalized_pin, salt)

    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO manager_credentials (
                    employee_item_id, manager_name, manager_adp, branch,
                    pin_salt, pin_hash, is_active
                )
                VALUES (%s, %s, %s, %s, %s, %s, TRUE)
                ON CONFLICT (employee_item_id) DO UPDATE
                SET manager_name = EXCLUDED.manager_name,
                    manager_adp = EXCLUDED.manager_adp,
                    branch = EXCLUDED.branch,
                    pin_salt = EXCLUDED.pin_salt,
                    pin_hash = EXCLUDED.pin_hash,
                    is_active = TRUE,
                    updated_at = NOW()
                """,
                (
                    employee_item_id,
                    manager_employee["name"],
                    manager_employee.get("adp_number"),
                    manager_employee.get("branch"),
                    salt,
                    pin_hash,
                ),
            )
        connection.commit()
    finally:
        connection.close()


def verify_manager_pin(manager_item_id, pin):
    """Validate a manager PIN against the stored credential hash."""
    employee_item_id = _coerce_bigint(manager_item_id)
    if employee_item_id is None or not str(pin).strip():
        return False

    record = _fetch_one_dict(
        """
        SELECT pin_salt, pin_hash
        FROM manager_credentials
        WHERE employee_item_id = %s
          AND is_active = TRUE
        """,
        (employee_item_id,),
    )
    if not record:
        return False

    computed_hash = _hash_pin(str(pin).strip(), record["pin_salt"])
    return hmac.compare_digest(computed_hash, record["pin_hash"])


def has_manager_pin(manager_item_id):
    """Check whether a manager has a configured credential."""
    employee_item_id = _coerce_bigint(manager_item_id)
    if employee_item_id is None:
        return False

    record = _fetch_one_dict(
        """
        SELECT 1 AS has_pin
        FROM manager_credentials
        WHERE employee_item_id = %s
          AND is_active = TRUE
        """,
        (employee_item_id,),
    )
    return bool(record)


def _get_test_work_orders(branch=None):
    """Return synthetic non-production work orders for safe app testing."""
    if not ENABLE_TEST_WORK_ORDERS:
        return []

    test_order = dict(TEST_WORK_ORDER_TEMPLATE)
    test_order["branch"] = branch or ""
    return [test_order]


def get_open_work_orders(branch=None):
    """Return active EN work orders from synced production operations."""
    rows = _fetch_all_dicts(
        """
        SELECT si.sharepoint_item_id, si.fields_json
        FROM sharepoint_items si
        JOIN sharepoint_lists sl ON sl.id = si.list_id
        WHERE sl.list_name = 'Production Operations'
        ORDER BY si.id
        """
    )

    orders = []
    for row in rows:
        order = _normalize_production_order_row(row)
        if (
            order["production_number"]
            and order["order_type"] == "EN"
            and order["status"] in {"Released", "In Process"}
            and order["operation_description"]
        ):
            if not branch or order["branch"] in {None, "", branch}:
                orders.append(order)

    orders.extend(_get_test_work_orders(branch))
    return orders


def get_connection_types(production_number, branch=None):
    """Return unique operation descriptions for a production number."""
    matches = [
        order
        for order in get_open_work_orders(branch)
        if order["production_number"] == str(production_number).strip()
    ]

    seen = set()
    results = []
    for item in matches:
        operation_description = item["operation_description"]
        if operation_description not in seen:
            seen.add(operation_description)
            results.append(item)
    return results


def get_recipe_builder_options(branch=None):
    """Return dropdown options for the admin recipe builder."""
    recipe_rows = _fetch_all_dicts(
        """
        SELECT si.sharepoint_item_id, si.fields_json
        FROM sharepoint_items si
        JOIN sharepoint_lists sl ON sl.id = si.list_id
        WHERE sl.list_name = 'InspectionRecipes'
        ORDER BY si.id
        """
    )

    element_options = set(DEFAULT_RECIPE_ELEMENT_OPTIONS)
    gauge_options = set(DEFAULT_GAUGE_OPTIONS)

    for row in recipe_rows:
        recipe = _normalize_recipe_row(row)
        if branch and recipe["branch"] not in {None, "", branch}:
            continue
        recipe_json = recipe.get("recipe_json")
        if isinstance(recipe_json, dict) and isinstance(recipe_json.get("elements"), list):
            for element in recipe_json["elements"]:
                if not isinstance(element, dict):
                    continue
                if element.get("element"):
                    element_options.add(str(element["element"]).strip())
                if element.get("gauge"):
                    gauge_options.add(str(element["gauge"]).strip())
        else:
            if recipe.get("element_description"):
                element_options.add(str(recipe["element_description"]).strip())
            if recipe.get("gauge"):
                gauge_options.add(str(recipe["gauge"]).strip())

    return {
        "element_options": sorted(item for item in element_options if item),
        "gauge_options": sorted(item for item in gauge_options if item),
        "measurement_modes": [
            {"value": "nominal_tolerance", "label": "Nominal +/- Tolerance"},
            {"value": "range", "label": "Range"},
            {"value": "deviation", "label": "+/- From Zero"},
            {"value": "visual", "label": "Visual / SOP"},
        ],
        "frequency_options": [
            {"value": "every_pipe", "label": "Every Pipe"},
            {"value": "rotating", "label": "Rotating"},
        ],
    }


def list_local_recipes(branch=None):
    """Return locally built recipes available in the admin tool."""
    clauses = ["is_active = TRUE"]
    params = []
    if branch:
        clauses.append("(branch = %s OR branch IS NULL OR branch = '')")
        params.append(branch)

    return _fetch_all_dicts(
        f"""
        SELECT id, branch, recipe_name, connection_type, drawing, source_report,
               recipe_version, created_by, created_at, updated_at
        FROM app_recipe_headers
        WHERE {' AND '.join(clauses)}
        ORDER BY updated_at DESC, recipe_name ASC
        """,
        tuple(params),
    )


def get_local_recipe_by_id(recipe_header_id):
    """Return one locally managed recipe with its element rows for editing."""
    header = _fetch_one_dict(
        """
        SELECT id, branch, recipe_name, connection_type, size_label, weight_label,
               grade_label, connector_type, drawing, source_report, recipe_version,
               created_by, created_at, updated_at
        FROM app_recipe_headers
        WHERE id = %s
          AND is_active = TRUE
        """,
        (recipe_header_id,),
    )
    if not header:
        return None

    rows = _fetch_all_dicts(
        """
        SELECT id, element_sequence, element_description, measurement_mode, dwg_dim, gauge,
               capture_type, value_format, frequency, nominal, min_value, max_value, notes
        FROM app_recipe_elements
        WHERE recipe_header_id = %s
        ORDER BY element_sequence ASC
        """,
        (recipe_header_id,),
    )
    header["rows"] = rows
    return header


def find_recipe_candidates(operation_description, branch=None):
    """Suggest likely recipes using aliases first and recipe-name token overlap second."""
    alias_matches = _fetch_all_dicts(
        """
        SELECT recipe_name, confidence_rank
        FROM recipe_aliases
        WHERE is_active = TRUE
          AND operation_description = %s
          AND (branch = %s OR branch IS NULL OR branch = '')
        ORDER BY confidence_rank ASC, recipe_name ASC
        """,
        (operation_description, branch),
    )
    if alias_matches:
        return [{"recipe_name": row["recipe_name"], "match_type": "alias"} for row in alias_matches]

    local_recipe_rows = _fetch_all_dicts(
        """
        SELECT recipe_name, connection_type, branch
        FROM app_recipe_headers
        WHERE is_active = TRUE
        ORDER BY updated_at DESC, recipe_name ASC
        """
    )

    recipe_rows = _fetch_all_dicts(
        """
        SELECT si.sharepoint_item_id, si.fields_json
        FROM sharepoint_items si
        JOIN sharepoint_lists sl ON sl.id = si.list_id
        WHERE sl.list_name = 'InspectionRecipes'
        ORDER BY si.id
        """
    )

    tokens = {
        token.lower()
        for token in str(operation_description).replace(",", " ").replace('"', " ").split()
        if len(token) > 2
    }
    scored = []
    seen = set()

    for recipe in local_recipe_rows:
        recipe_name = recipe["recipe_name"]
        if not recipe_name or recipe_name in seen:
            continue
        if branch and recipe["branch"] not in {None, "", branch}:
            continue
        comparable_text = " ".join(
            value
            for value in [recipe_name, recipe.get("connection_type")]
            if value
        )
        recipe_tokens = {
            token.lower() for token in comparable_text.replace(",", " ").replace('"', " ").split()
        }
        score = len(tokens.intersection(recipe_tokens))
        if score:
            scored.append((score, recipe_name))
            seen.add(recipe_name)

    for row in recipe_rows:
        recipe = _normalize_recipe_row(row)
        recipe_name = recipe["recipe_name"]
        if not recipe_name or recipe_name in seen:
            continue
        if branch and recipe["branch"] not in {None, "", branch}:
            continue

        comparable_text = " ".join(
            value
            for value in [recipe_name, recipe.get("connection_type")]
            if value
        )
        recipe_tokens = {
            token.lower() for token in comparable_text.replace(",", " ").replace('"', " ").split()
        }
        score = len(tokens.intersection(recipe_tokens))
        if score:
            scored.append((score, recipe_name))
            seen.add(recipe_name)

    scored.sort(key=lambda item: (-item[0], item[1]))
    return [
        {"recipe_name": recipe_name, "match_type": "token", "score": score}
        for score, recipe_name in scored[:5]
    ]


def get_recipe_elements(recipe_name, branch=None):
    """Return all synced recipe elements for the selected recipe."""
    local_header = _fetch_one_dict(
        """
        SELECT id, branch, recipe_name, connection_type, drawing, source_report, recipe_version
        FROM app_recipe_headers
        WHERE recipe_name = %s
          AND is_active = TRUE
          AND (%s IS NULL OR branch = %s OR branch IS NULL OR branch = '')
        ORDER BY updated_at DESC NULLS LAST, id DESC
        LIMIT 1
        """,
        (recipe_name, branch, branch),
    )
    if local_header:
        local_elements = _fetch_all_dicts(
            """
            SELECT element_sequence, element_description, measurement_mode, dwg_dim, gauge,
                   capture_type, value_format, frequency, nominal, min_value, max_value, notes
            FROM app_recipe_elements
            WHERE recipe_header_id = %s
            ORDER BY element_sequence ASC
            """,
            (local_header["id"],),
        )
        return _recipe_definition_from_local(local_header, local_elements)

    rows = _fetch_all_dicts(
        """
        SELECT si.sharepoint_item_id, si.fields_json
        FROM sharepoint_items si
        JOIN sharepoint_lists sl ON sl.id = si.list_id
        WHERE sl.list_name = 'InspectionRecipes'
        ORDER BY si.id
        """
    )

    elements = []
    for row in rows:
        recipe = _normalize_recipe_row(row)
        if recipe["recipe_name"] != recipe_name:
            continue
        if branch and recipe["branch"] not in {None, "", branch}:
            continue

        recipe_json = recipe.get("recipe_json")
        if isinstance(recipe_json, dict) and isinstance(recipe_json.get("elements"), list):
            min_max_by_item = {
                rule.get("item"): rule
                for rule in recipe.get("min_max_rules", [])
                if isinstance(rule, dict) and rule.get("item") is not None
            }
            elements = []
            for index, element in enumerate(recipe_json["elements"], start=1):
                if not isinstance(element, dict):
                    continue
                item_number = element.get("item", index)
                min_max_rule = min_max_by_item.get(item_number, {})
                elements.append(
                    {
                        "item_id": recipe["item_id"],
                        "element_sequence": item_number,
                        "element_description": element.get("element"),
                        "dwg_dim": element.get("specText"),
                        "gauge": element.get("gauge"),
                        "capture_type": element.get("captureType", "numeric"),
                        "value_format": element.get("valueFormat"),
                        "frequency": element.get("frequency"),
                        "nominal": element.get("nominal"),
                        "min": element.get("min", min_max_rule.get("min")),
                        "max": element.get("max", min_max_rule.get("max")),
                        "notes": element.get("notes"),
                    }
                )
            return {
                "recipe_name": recipe["recipe_name"],
                "connection_type": recipe.get("connection_type"),
                "recipe_version": recipe.get("recipe_version"),
                "drawing": recipe_json.get("drawing"),
                "source_report": recipe_json.get("sourceReport"),
                "sampling_plan": recipe_json.get("samplingPlan", {}),
                "approval_rules": recipe.get("approval_rules", []),
                "elements": elements,
            }

        if recipe["element_description"]:
            elements.append(recipe)

    if elements:
        for index, element in enumerate(elements, start=1):
            element["element_sequence"] = index
        return {
            "recipe_name": recipe_name,
            "connection_type": None,
            "recipe_version": None,
            "drawing": None,
            "source_report": None,
            "sampling_plan": {},
            "approval_rules": [],
            "elements": elements,
        }
    return None


def create_local_recipe(recipe_payload):
    """Create a locally managed recipe and its ordered element rows."""
    processed_payload = _prepare_local_recipe_payload(recipe_payload)

    connection = get_db_connection()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                INSERT INTO app_recipe_headers (
                    branch, recipe_name, connection_type, size_label, weight_label,
                    grade_label, connector_type, drawing, source_report, created_by
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    processed_payload["branch"] or None,
                    processed_payload["recipe_name"],
                    processed_payload["connection_type"],
                    processed_payload["size_label"] or None,
                    processed_payload["weight_label"] or None,
                    processed_payload["grade_label"] or None,
                    processed_payload["connector_type"] or None,
                    processed_payload["drawing"] or None,
                    processed_payload["source_report"] or None,
                    processed_payload["created_by"] or None,
                ),
            )
            header_id = cursor.fetchone()["id"]

            for row in processed_payload["processed_rows"]:
                cursor.execute(
                    """
                    INSERT INTO app_recipe_elements (
                        recipe_header_id, element_sequence, element_description, measurement_mode,
                        dwg_dim, gauge, capture_type, value_format, frequency,
                        nominal, min_value, max_value, notes
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        header_id,
                        row["element_sequence"],
                        row["element_description"],
                        row["measurement_mode"],
                        row["dwg_dim"],
                        row["gauge"],
                        row["capture_type"],
                        row["value_format"],
                        row["frequency"],
                        row["nominal"],
                        row["min_value"],
                        row["max_value"],
                        row["notes"],
                    ),
                )
        connection.commit()
        return get_recipe_elements(processed_payload["recipe_name"], processed_payload["branch"] or None)
    finally:
        connection.close()


def _prepare_local_recipe_payload(recipe_payload):
    """Validate and normalize local recipe inputs for create/update operations."""
    branch = str(recipe_payload.get("branch") or "").strip()
    size_label = str(recipe_payload.get("size_label") or "").strip()
    weight_label = str(recipe_payload.get("weight_label") or "").strip()
    grade_label = str(recipe_payload.get("grade_label") or "").strip()
    connector_type = str(recipe_payload.get("connector_type") or "").strip()
    drawing = str(recipe_payload.get("drawing") or "").strip()
    source_report = str(recipe_payload.get("source_report") or "").strip()
    created_by = str(recipe_payload.get("created_by") or "").strip()
    rows = recipe_payload.get("rows") or []

    connection_parts = [part for part in [size_label, weight_label, grade_label, connector_type] if part]
    connection_type = " ".join(connection_parts).strip()
    if not connection_type:
        raise ValueError("Size, weight, grade, and connector type are required to build the recipe name.")

    recipe_name = f"{connection_type} Recipe"
    active_rows = [row for row in rows if str(row.get("element_description") or "").strip()]
    if not active_rows:
        raise ValueError("Add at least one recipe element before saving.")

    processed_rows = []
    for index, row in enumerate(active_rows, start=1):
        element_description = str(row.get("element_description") or "").strip()
        measurement_mode = str(row.get("measurement_mode") or "").strip()
        gauge = str(row.get("gauge") or "").strip()
        frequency = str(row.get("frequency") or "every_pipe").strip() or "every_pipe"
        notes = str(row.get("notes") or "").strip() or None

        if not element_description:
            continue
        if measurement_mode not in {"nominal_tolerance", "range", "deviation", "visual"}:
            raise ValueError(f"Element {index} is missing a valid measurement mode.")
        if not gauge and measurement_mode != "visual":
            raise ValueError(f"Element {index} is missing a gauge.")

        capture_type = "boolean" if measurement_mode == "visual" else "numeric"
        nominal = None
        min_value = None
        max_value = None
        value_format = None
        dwg_dim = None

        if measurement_mode == "nominal_tolerance":
            nominal = float(str(row.get("nominal_value") or "").strip())
            tolerance_digits = str(row.get("tolerance_digits") or "").strip()
            tolerance_decimal_places = int(str(row.get("tolerance_decimal_places") or "3").strip())
            if not tolerance_digits:
                raise ValueError(f"Element {index} needs tolerance digits.")
            tolerance_value = int(tolerance_digits) / (10 ** tolerance_decimal_places)
            min_value = nominal - tolerance_value
            max_value = nominal + tolerance_value
            value_format = "decimal"
            dwg_dim = f"{nominal:.{tolerance_decimal_places}f} +/- .{'0' * max(tolerance_decimal_places - len(tolerance_digits), 0)}{tolerance_digits}"
        elif measurement_mode == "range":
            min_value = float(str(row.get("range_min") or "").strip())
            max_value = float(str(row.get("range_max") or "").strip())
            value_format = "decimal"
            decimals = max(_count_decimal_places(min_value), _count_decimal_places(max_value))
            dwg_dim = f"{min_value:.{decimals}f} - {max_value:.{decimals}f}"
        elif measurement_mode == "deviation":
            tolerance_digits = str(row.get("tolerance_digits") or "").strip()
            tolerance_decimal_places = int(str(row.get("tolerance_decimal_places") or "3").strip())
            if not tolerance_digits:
                raise ValueError(f"Element {index} needs tolerance digits.")
            nominal = 0.0
            tolerance_value = int(tolerance_digits) / (10 ** tolerance_decimal_places)
            min_value = -tolerance_value
            max_value = tolerance_value
            value_format = "deviation"
            dwg_dim = f"+/- .{'0' * max(tolerance_decimal_places - len(tolerance_digits), 0)}{tolerance_digits}"
        else:
            value_format = "yes/no"
            dwg_dim = str(row.get("visual_spec") or "Visual").strip()

        processed_rows.append(
            {
                "element_sequence": index,
                "element_description": element_description,
                "measurement_mode": measurement_mode,
                "dwg_dim": dwg_dim,
                "gauge": gauge if gauge else "Visual",
                "capture_type": capture_type,
                "value_format": value_format,
                "frequency": "rotating" if frequency == "rotating" else "every_pipe",
                "nominal": nominal,
                "min_value": min_value,
                "max_value": max_value,
                "notes": notes,
            }
        )

    return {
        "branch": branch,
        "size_label": size_label,
        "weight_label": weight_label,
        "grade_label": grade_label,
        "connector_type": connector_type,
        "drawing": drawing,
        "source_report": source_report,
        "created_by": created_by,
        "connection_type": connection_type,
        "recipe_name": recipe_name,
        "processed_rows": processed_rows,
    }


def update_local_recipe(recipe_header_id, recipe_payload):
    """Update an existing locally managed recipe and replace its element rows."""
    processed_payload = _prepare_local_recipe_payload(recipe_payload)
    connection = get_db_connection()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT id
                FROM app_recipe_headers
                WHERE id = %s
                  AND is_active = TRUE
                """,
                (recipe_header_id,),
            )
            existing = cursor.fetchone()
            if not existing:
                raise ValueError("That local recipe could not be found.")

            cursor.execute(
                """
                UPDATE app_recipe_headers
                SET branch = %s,
                    recipe_name = %s,
                    connection_type = %s,
                    size_label = %s,
                    weight_label = %s,
                    grade_label = %s,
                    connector_type = %s,
                    drawing = %s,
                    source_report = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (
                    processed_payload["branch"] or None,
                    processed_payload["recipe_name"],
                    processed_payload["connection_type"],
                    processed_payload["size_label"] or None,
                    processed_payload["weight_label"] or None,
                    processed_payload["grade_label"] or None,
                    processed_payload["connector_type"] or None,
                    processed_payload["drawing"] or None,
                    processed_payload["source_report"] or None,
                    recipe_header_id,
                ),
            )
            cursor.execute(
                "DELETE FROM app_recipe_elements WHERE recipe_header_id = %s",
                (recipe_header_id,),
            )

            for row in processed_payload["processed_rows"]:
                cursor.execute(
                    """
                    INSERT INTO app_recipe_elements (
                        recipe_header_id, element_sequence, element_description, measurement_mode,
                        dwg_dim, gauge, capture_type, value_format, frequency,
                        nominal, min_value, max_value, notes
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        recipe_header_id,
                        row["element_sequence"],
                        row["element_description"],
                        row["measurement_mode"],
                        row["dwg_dim"],
                        row["gauge"],
                        row["capture_type"],
                        row["value_format"],
                        row["frequency"],
                        row["nominal"],
                        row["min_value"],
                        row["max_value"],
                        row["notes"],
                    ),
                )
        connection.commit()
        return get_recipe_elements(processed_payload["recipe_name"], processed_payload["branch"] or None)
    finally:
        connection.close()


def build_inspection_plan(
    recipe_definition,
    attempt_no,
    inspection_scope="standard",
    rotation_index=None,
    every_pipe_count=DEFAULT_ALWAYS_INSPECT_COUNT,
    rotating_count=DEFAULT_ROTATING_COUNT,
):
    """Return the recipe elements due for this attempt."""
    recipe_elements = recipe_definition.get("elements", []) if recipe_definition else []
    if not recipe_elements:
        return []

    if str(inspection_scope).strip().lower() == "full":
        due_elements = []
        for element in recipe_elements:
            planned = dict(element)
            planned["inspection_frequency"] = "full"
            planned["inspected_this_pipe"] = True
            due_elements.append(planned)
        return due_elements

    sampling_plan = recipe_definition.get("sampling_plan", {}) if recipe_definition else {}
    always_items = sampling_plan.get("alwaysMeasureItems") or []
    rotating_items = sampling_plan.get("rotatingAuditItems") or []
    cycle_map = sampling_plan.get("cycleMap") or {}
    sequence_index = (
        int(rotation_index)
        if rotation_index is not None and str(rotation_index).strip().isdigit()
        else int(attempt_no)
    )
    elements_by_sequence = {
        element.get("element_sequence"): element for element in recipe_elements
    }

    if always_items or rotating_items:
        due_elements = []
        for item_no in always_items:
            element = elements_by_sequence.get(item_no)
            if element:
                planned = dict(element)
                planned["inspection_frequency"] = "every_pipe"
                planned["inspected_this_pipe"] = True
                due_elements.append(planned)

        cycle_key = str(((sequence_index - 1) % max(len(rotating_items), 1)) + 1)
        rotating_selection = cycle_map.get(cycle_key)
        if rotating_selection is None and rotating_items:
            rotating_selection = rotating_items[(sequence_index - 1) % len(rotating_items)]
        if rotating_selection is not None:
            selections = rotating_selection if isinstance(rotating_selection, list) else [rotating_selection]
            for item_no in selections:
                element = elements_by_sequence.get(item_no)
                if element:
                    planned = dict(element)
                    planned["inspection_frequency"] = "rotating"
                    planned["inspected_this_pipe"] = True
                    due_elements.append(planned)
        return due_elements

    always_count = min(max(every_pipe_count, 0), len(recipe_elements))
    due_elements = []

    for element in recipe_elements[:always_count]:
        planned = dict(element)
        planned["inspection_frequency"] = "every_pipe"
        planned["inspected_this_pipe"] = True
        due_elements.append(planned)

    rotating_elements = recipe_elements[always_count:]
    if rotating_elements and rotating_count > 0:
        start_index = ((sequence_index - 1) * rotating_count) % len(rotating_elements)
        for offset in range(rotating_count):
            rotating_element = rotating_elements[(start_index + offset) % len(rotating_elements)]
            planned = dict(rotating_element)
            planned["inspection_frequency"] = "rotating"
            planned["inspected_this_pipe"] = True
            due_elements.append(planned)

    return due_elements


def evaluate_measurements(measurements, approval_rules=None):
    """Auto-evaluate entered measurements against recipe rules."""
    evaluated = []
    requires_approval = False
    failed_count = 0

    for measurement in measurements:
        updated = dict(measurement)
        capture_type = updated.get("capture_type")
        value = updated.get("measured_value")
        auto_pass = True

        if capture_type == "numeric":
            try:
                numeric_value = float(str(value).strip())
                updated["measured_value"] = numeric_value
                minimum = updated.get("min")
                maximum = updated.get("max")
                if minimum is not None and numeric_value < float(minimum):
                    auto_pass = False
                if maximum is not None and numeric_value > float(maximum):
                    auto_pass = False
            except (TypeError, ValueError):
                auto_pass = False
        elif capture_type == "boolean":
            normalized = str(value).strip().lower()
            auto_pass = normalized in {"yes", "y", "pass", "true", "1"}
        else:
            auto_pass = str(value).strip() != ""

        updated["pass_fail"] = "Pass" if auto_pass else "Fail"
        if updated["pass_fail"] == "Fail":
            failed_count += 1
        evaluated.append(updated)

    for rule in approval_rules or []:
        if not isinstance(rule, dict):
            continue
        condition = rule.get("condition")
        if condition == "numeric_out_of_spec" and failed_count:
            requires_approval = bool(rule.get("requiresApproval"))
        if condition == "visual_fail":
            if any(
                item.get("capture_type") == "boolean" and item.get("pass_fail") == "Fail"
                for item in evaluated
            ):
                requires_approval = bool(rule.get("requiresApproval"))

    return {
        "measurements": evaluated,
        "requires_approval": requires_approval,
        "has_failures": failed_count > 0,
    }


def get_pipe_unit(production_number, operation_description, pipe_number):
    """Return an existing pipe unit if this pipe has been seen before."""
    return _fetch_one_dict(
        """
        SELECT id, production_number, operation_description, pipe_number, branch,
               current_status, latest_attempt_no
        FROM pipe_units
        WHERE production_number = %s
          AND operation_description = %s
          AND pipe_number = %s
        """,
        (str(production_number).strip(), operation_description, str(pipe_number).strip()),
    )


def get_pipe_unit_by_id(pipe_unit_id):
    """Return one pipe unit by its primary key."""
    return _fetch_one_dict(
        """
        SELECT id, production_number, operation_description, pipe_number, branch,
               current_status, latest_attempt_no, created_at, updated_at
        FROM pipe_units
        WHERE id = %s
        """,
        (pipe_unit_id,),
    )


def get_pipe_attempt_history(pipe_unit_id):
    """Return prior attempts for a pipe unit."""
    return _fetch_all_dicts(
        """
        SELECT ia.id,
               ia.attempt_no,
               ia.inspection_scope,
               ia.status,
               ia.requires_manager_approval,
               ia.manager_name,
               ia.inspector_name,
               ia.cnc_operator_name,
               ia.recipe_name,
               ia.started_at,
               ia.completed_at,
               ia.notes,
               sess.shift,
               sess.location_name,
               sess.logged_in_at,
               sess.logged_out_at
        FROM inspection_attempts ia
        LEFT JOIN inspector_sessions sess ON sess.id = ia.session_id
        WHERE ia.pipe_unit_id = %s
        ORDER BY attempt_no DESC
        """,
        (pipe_unit_id,),
    )


def get_attempt_measurements(attempt_id):
    """Return saved measurements for one attempt."""
    return _fetch_all_dicts(
        """
        SELECT element_sequence, element_description, dwg_dim, gauge,
               measured_value, pass_fail, inspected_this_pipe, created_at
        FROM inspection_measurements
        WHERE attempt_id = %s
        ORDER BY element_sequence ASC, id ASC
        """,
        (attempt_id,),
    )


def search_pipe_units(branch=None, production_number=None, pipe_number=None, status=None, inspection_scope=None):
    """Search pipe units with optional filters."""
    clauses = ["1 = 1"]
    params = []

    if branch:
        clauses.append("branch = %s")
        params.append(branch)
    if production_number:
        clauses.append("production_number = %s")
        params.append(str(production_number).strip())
    if pipe_number:
        clauses.append("pipe_number = %s")
        params.append(str(pipe_number).strip())
    if status:
        clauses.append("current_status = %s")
        params.append(status)
    if inspection_scope:
        clauses.append("latest_attempt.inspection_scope = %s")
        params.append(inspection_scope)

    return _fetch_all_dicts(
        f"""
        SELECT pu.id, pu.production_number, pu.operation_description, pu.pipe_number,
               pu.branch, pu.current_status, pu.latest_attempt_no, pu.created_at, pu.updated_at,
               latest_attempt.inspection_scope AS latest_inspection_scope
        FROM pipe_units pu
        LEFT JOIN inspection_attempts latest_attempt
          ON latest_attempt.pipe_unit_id = pu.id
         AND latest_attempt.attempt_no = pu.latest_attempt_no
        WHERE {' AND '.join(clauses)}
        ORDER BY updated_at DESC, production_number ASC, pipe_number ASC
        """,
        tuple(params),
    )


def get_ncr_reports(branch=None, status=None):
    """Return NCR records with related pipe context."""
    clauses = ["1 = 1"]
    params = []

    if branch:
        clauses.append("pu.branch = %s")
        params.append(branch)
    if status:
        clauses.append("nr.status = %s")
        params.append(status)

    return _fetch_all_dicts(
        f"""
        SELECT nr.id, nr.attempt_id, nr.pipe_unit_id, nr.status, nr.disposition,
               nr.tier_code, nr.nonconformance, nr.immediate_containment,
               nr.opened_at, nr.closed_at,
               pu.production_number, pu.operation_description, pu.pipe_number, pu.branch
        FROM ncr_reports nr
        JOIN pipe_units pu ON pu.id = nr.pipe_unit_id
        WHERE {' AND '.join(clauses)}
        ORDER BY nr.status ASC, nr.opened_at DESC
        """,
        tuple(params),
    )


def update_ncr_report(
    ncr_id,
    status,
    disposition=None,
    tier_code=None,
    nonconformance=None,
    immediate_containment=None,
):
    """Update an NCR record from the queue UI."""
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE ncr_reports
                SET status = %s,
                    disposition = COALESCE(%s, disposition),
                    tier_code = COALESCE(%s, tier_code),
                    nonconformance = COALESCE(%s, nonconformance),
                    immediate_containment = COALESCE(%s, immediate_containment),
                    closed_at = CASE WHEN %s = 'closed' THEN NOW() ELSE NULL END
                WHERE id = %s
                """,
                (
                    status,
                    disposition,
                    tier_code,
                    nonconformance,
                    immediate_containment,
                    status,
                    ncr_id,
                ),
            )
        connection.commit()
    finally:
        connection.close()


def delete_pipe_unit(pipe_unit_id):
    """Delete a pipe unit and all cascading attempts/measurements/NCRs."""
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM pipe_units WHERE id = %s", (pipe_unit_id,))
        connection.commit()
    finally:
        connection.close()


def reset_in_progress_pipe_unit(pipe_unit_id):
    """Remove the current in-progress attempt and restore the last resolved state."""
    connection = get_db_connection()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT id, current_status, latest_attempt_no
                FROM pipe_units
                WHERE id = %s
                """,
                (pipe_unit_id,),
            )
            pipe_unit = cursor.fetchone()
            if not pipe_unit:
                raise ValueError("Pipe inspection was not found.")
            if pipe_unit["current_status"] != "in_progress":
                raise ValueError("Only in-progress pipe inspections can be reset.")

            cursor.execute(
                """
                SELECT id, attempt_no
                FROM inspection_attempts
                WHERE pipe_unit_id = %s
                  AND attempt_no = %s
                ORDER BY id DESC
                LIMIT 1
                """,
                (pipe_unit_id, pipe_unit["latest_attempt_no"]),
            )
            current_attempt = cursor.fetchone()
            if current_attempt:
                cursor.execute(
                    "DELETE FROM inspection_attempts WHERE id = %s",
                    (current_attempt["id"],),
                )

            cursor.execute(
                """
                SELECT attempt_no, status
                FROM inspection_attempts
                WHERE pipe_unit_id = %s
                ORDER BY attempt_no DESC, id DESC
                LIMIT 1
                """,
                (pipe_unit_id,),
            )
            previous_attempt = cursor.fetchone()

            if not previous_attempt:
                cursor.execute("DELETE FROM pipe_units WHERE id = %s", (pipe_unit_id,))
                connection.commit()
                return {"deleted_pipe_unit": True, "restored_status": None}

            status_map = {
                "passed": "completed",
                "approved": "completed",
                "rework": "rework",
                "scrapped": "scrapped",
            }
            restored_status = status_map.get(previous_attempt["status"], "pending")
            cursor.execute(
                """
                UPDATE pipe_units
                SET latest_attempt_no = %s,
                    current_status = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (previous_attempt["attempt_no"], restored_status, pipe_unit_id),
            )

        connection.commit()
        return {"deleted_pipe_unit": False, "restored_status": restored_status}
    finally:
        connection.close()


def update_pipe_unit(pipe_unit_id, production_number, operation_description, pipe_number):
    """Update the key display fields for a pipe unit while preserving history."""
    connection = get_db_connection()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT id
                FROM pipe_units
                WHERE id = %s
                """,
                (pipe_unit_id,),
            )
            existing = cursor.fetchone()
            if not existing:
                raise ValueError("Pipe inspection was not found.")

            normalized_production = str(production_number or "").strip()
            normalized_operation = str(operation_description or "").strip()
            normalized_pipe = str(pipe_number or "").strip()

            if not normalized_production or not normalized_operation or not normalized_pipe:
                raise ValueError("Production number, connection description, and pipe number are required.")

            cursor.execute(
                """
                SELECT id
                FROM pipe_units
                WHERE production_number = %s
                  AND operation_description = %s
                  AND pipe_number = %s
                  AND id <> %s
                """,
                (normalized_production, normalized_operation, normalized_pipe, pipe_unit_id),
            )
            conflicting = cursor.fetchone()
            if conflicting:
                raise ValueError("Another pipe inspection already exists with that work order, connection, and pipe number.")

            cursor.execute(
                """
                UPDATE pipe_units
                SET production_number = %s,
                    operation_description = %s,
                    pipe_number = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (normalized_production, normalized_operation, normalized_pipe, pipe_unit_id),
            )
        connection.commit()
        return get_pipe_unit_by_id(pipe_unit_id)
    finally:
        connection.close()


def create_inspection_attempt(
    production_number,
    operation_description,
    pipe_number,
    branch,
    session_id,
    inspector,
    cnc_operator,
    recipe_name,
    recipe_elements,
    inspection_scope="standard",
):
    """Create a new inspection attempt and return plan context."""
    normalized_pipe_number = str(pipe_number).strip()
    rotation_index = int(normalized_pipe_number) if normalized_pipe_number.isdigit() else None
    connection = get_db_connection()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT id, latest_attempt_no, current_status
                FROM pipe_units
                WHERE production_number = %s
                  AND operation_description = %s
                  AND pipe_number = %s
                """,
                (str(production_number).strip(), operation_description, str(pipe_number).strip()),
            )
            pipe_unit = cursor.fetchone()
            resumed_attempt = False

            if pipe_unit:
                pipe_unit_id = pipe_unit["id"]
                previous_status = pipe_unit["current_status"]
                if previous_status == "in_progress":
                    cursor.execute(
                        """
                        SELECT id, attempt_no, inspection_scope, started_at
                        FROM inspection_attempts
                        WHERE pipe_unit_id = %s
                          AND attempt_no = %s
                        ORDER BY id DESC
                        LIMIT 1
                        """,
                        (pipe_unit_id, pipe_unit["latest_attempt_no"]),
                    )
                    existing_attempt = cursor.fetchone()
                    if existing_attempt:
                        attempt_no = existing_attempt["attempt_no"]
                        normalized_scope = (
                            "full"
                            if str(existing_attempt.get("inspection_scope") or inspection_scope).strip().lower() == "full"
                            else "standard"
                        )
                        inspection_plan = build_inspection_plan(
                            recipe_elements,
                            attempt_no,
                            inspection_scope=normalized_scope,
                            rotation_index=rotation_index,
                        )
                        connection.commit()
                        return {
                            "attempt_id": existing_attempt["id"],
                            "pipe_unit_id": pipe_unit_id,
                            "attempt_no": attempt_no,
                            "inspection_scope": normalized_scope,
                            "is_rework": False,
                            "previous_status": previous_status,
                            "inspection_plan": inspection_plan,
                            "approval_rules": recipe_elements.get("approval_rules", []) if recipe_elements else [],
                            "resumed_attempt": True,
                        }

                if previous_status in {"completed", "rework"}:
                    attempt_no = pipe_unit["latest_attempt_no"] + 1
                    is_rework = True
                else:
                    attempt_no = max(int(pipe_unit["latest_attempt_no"] or 0), 0) + 1
                    is_rework = False
                cursor.execute(
                    """
                    UPDATE pipe_units
                    SET latest_attempt_no = %s,
                        current_status = 'in_progress',
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (attempt_no, pipe_unit_id),
                )
            else:
                attempt_no = 1
                is_rework = False
                previous_status = None
                cursor.execute(
                    """
                    INSERT INTO pipe_units (
                        production_number, operation_description, pipe_number, branch,
                        current_status, latest_attempt_no, updated_at
                    )
                    VALUES (%s, %s, %s, %s, 'in_progress', %s, NOW())
                    RETURNING id
                    """,
                    (
                        str(production_number).strip(),
                        operation_description,
                        str(pipe_number).strip(),
                        branch,
                        attempt_no,
                    ),
                )
                pipe_unit_id = cursor.fetchone()["id"]

            normalized_scope = (
                "full" if str(inspection_scope).strip().lower() == "full" else "standard"
            )
            inspection_plan = build_inspection_plan(
                recipe_elements,
                attempt_no,
                inspection_scope=normalized_scope,
                rotation_index=rotation_index,
            )
            cursor.execute(
                """
                INSERT INTO inspection_attempts (
                    pipe_unit_id,
                    session_id,
                    attempt_no,
                    inspector_item_id,
                    inspector_name,
                    cnc_operator_item_id,
                    cnc_operator_name,
                    recipe_name,
                    recipe_item_id,
                    inspection_scope,
                    status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'in_progress')
                RETURNING id, started_at
                """,
                (
                    pipe_unit_id,
                    session_id,
                    attempt_no,
                    _coerce_bigint(inspector["item_id"]),
                    inspector["name"],
                    _coerce_bigint(cnc_operator["item_id"]) if cnc_operator else None,
                    cnc_operator["name"] if cnc_operator else None,
                    recipe_name,
                    _coerce_bigint(inspection_plan[0]["item_id"]) if inspection_plan else None,
                    normalized_scope,
                ),
            )
            attempt = cursor.fetchone()

        connection.commit()
        return {
            "attempt_id": attempt["id"],
            "pipe_unit_id": pipe_unit_id,
            "attempt_no": attempt_no,
            "inspection_scope": normalized_scope,
            "is_rework": is_rework,
            "previous_status": previous_status,
            "inspection_plan": inspection_plan,
            "approval_rules": recipe_elements.get("approval_rules", []) if recipe_elements else [],
            "resumed_attempt": resumed_attempt,
        }
    finally:
        connection.close()


def complete_inspection_attempt(
    attempt_id,
    pipe_unit_id,
    measurements,
    disposition,
    notes="",
    manager_name="",
    manager_reason="",
    ncr_data=None,
):
    """Persist measurements and close an attempt with its resulting pipe status."""
    requires_manager_approval = disposition == "manager_approved"
    manager_pin_verified = requires_manager_approval
    pipe_status_map = {
        "pass": "completed",
        "manager_approved": "completed",
        "rework": "rework",
        "scrapped": "scrapped",
    }
    attempt_status_map = {
        "pass": "passed",
        "manager_approved": "approved",
        "rework": "rework",
        "scrapped": "scrapped",
    }
    pipe_status = pipe_status_map[disposition]
    attempt_status = attempt_status_map[disposition]
    combined_notes = (notes or "").strip()
    approval_reason_text = (manager_reason or "").strip()
    if requires_manager_approval and approval_reason_text:
        combined_notes = (
            f"Approval reason: {approval_reason_text}"
            if not combined_notes
            else f"Approval reason: {approval_reason_text}\n\n{combined_notes}"
        )

    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM inspection_measurements
                WHERE attempt_id = %s
                """,
                (attempt_id,),
            )

            any_failures = False
            for measurement in measurements:
                pass_fail = measurement.get("pass_fail", "")
                if pass_fail == "Fail":
                    any_failures = True
                cursor.execute(
                    """
                    INSERT INTO inspection_measurements (
                        attempt_id,
                        element_sequence,
                        element_description,
                        dwg_dim,
                        gauge,
                        measured_value,
                        pass_fail,
                        inspected_this_pipe
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        attempt_id,
                        measurement.get("element_sequence"),
                        measurement.get("element_description"),
                        measurement.get("dwg_dim"),
                        measurement.get("gauge"),
                        measurement.get("measured_value"),
                        pass_fail,
                        measurement.get("inspected_this_pipe", True),
                    ),
                )

            cursor.execute(
                """
                UPDATE inspection_attempts
                SET status = %s,
                    requires_manager_approval = %s,
                    manager_name = %s,
                    manager_pin_verified = %s,
                    completed_at = NOW(),
                    notes = %s
                WHERE id = %s
                """,
                (
                    attempt_status,
                    requires_manager_approval,
                    manager_name or None,
                    manager_pin_verified,
                    combined_notes or None,
                    attempt_id,
                ),
            )

            cursor.execute(
                """
                UPDATE pipe_units
                SET current_status = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (pipe_status, pipe_unit_id),
            )

            if any_failures:
                if pipe_status == "completed":
                    cursor.execute(
                        """
                        INSERT INTO ncr_reports (
                            attempt_id, pipe_unit_id, status, disposition,
                            tier_code, nonconformance, immediate_containment, closed_at
                        )
                        VALUES (%s, %s, 'closed', %s, %s, %s, %s, NOW())
                        """,
                        (
                            attempt_id,
                            pipe_unit_id,
                            pipe_status,
                            (ncr_data or {}).get("tier_code"),
                            (ncr_data or {}).get("nonconformance"),
                            (ncr_data or {}).get("immediate_containment"),
                        ),
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO ncr_reports (
                            attempt_id, pipe_unit_id, status, disposition,
                            tier_code, nonconformance, immediate_containment
                        )
                        VALUES (%s, %s, 'open', %s, %s, %s, %s)
                        """,
                        (
                            attempt_id,
                            pipe_unit_id,
                            pipe_status,
                            (ncr_data or {}).get("tier_code"),
                            (ncr_data or {}).get("nonconformance"),
                            (ncr_data or {}).get("immediate_containment"),
                        ),
                    )

            if pipe_status in {"completed", "scrapped"}:
                cursor.execute(
                    """
                    UPDATE ncr_reports
                    SET status = 'closed',
                        disposition = %s,
                        closed_at = NOW()
                    WHERE pipe_unit_id = %s
                      AND status = 'open'
                    """,
                    (pipe_status, pipe_unit_id),
                )

        connection.commit()
        return {
            "attempt_status": attempt_status,
            "pipe_status": pipe_status,
            "manager_pin_verified": manager_pin_verified,
        }
    finally:
        connection.close()


def create_inspector_session(inspector, shift, location, cnc_operator):
    """Store the login/session context for the current inspector."""
    connection = get_db_connection()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                INSERT INTO inspector_sessions (
                    inspector_item_id,
                    inspector_adp,
                    inspector_name,
                    branch,
                    shift,
                    location_id,
                    location_name,
                    cnc_operator_item_id,
                    cnc_operator_name
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, logged_in_at, location_id, location_name,
                          cnc_operator_item_id, cnc_operator_name
                """,
                (
                    int(inspector["item_id"]) if str(inspector["item_id"]).isdigit() else None,
                    inspector["adp_number"],
                    inspector["name"],
                    inspector["branch"],
                    shift,
                    location.get("id") if location else None,
                    location.get("location_name") if location else None,
                    int(cnc_operator["item_id"]) if cnc_operator and str(cnc_operator["item_id"]).isdigit() else None,
                    cnc_operator.get("name") if cnc_operator else None,
                ),
            )
            row = cursor.fetchone()
        connection.commit()
        return dict(row)
    finally:
        connection.close()


def close_inspector_session(session_id):
    """Mark a session as logged out."""
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE inspector_sessions SET logged_out_at = NOW() WHERE id = %s",
                (session_id,),
            )
        connection.commit()
    finally:
        connection.close()
