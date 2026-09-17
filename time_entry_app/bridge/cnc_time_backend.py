import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = APP_ROOT.parent
for candidate in (str(APP_ROOT), str(PROJECT_ROOT)):
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

LOCAL_MACHINE_CONFIG = APP_ROOT / "config" / "machines.json"

import requests

from local_store import (
    approve_approval_record,
    discard_pending_record,
    ensure_db,
    finish_break_event,
    get_approval_record,
    get_active_break_event,
    get_machine_options,
    list_approval_records,
    list_pending_records,
    mark_approval_reviewed,
    mark_record_synced,
    patch_approval_record_fields,
    patch_approval_record_payload,
    patch_pending_record_fields,
    queue_approval,
    queue_record,
    save_session,
    start_break_event,
    upsert_machine_options,
)
from postgres_sync import get_db_connection, initialize_database, sync_list_to_postgres
from sharepoint_api import sync_cnc_time_lists_to_postgres
from sharepoint_client import (
    GRAPH_BASE_URL,
    SharePointApiError,
    build_headers,
    get_access_token_from_env,
    get_list_items,
    get_site_id,
)


MACHINIST_TIME_SITE = "https://benoitinc.sharepoint.com/sites/MachinistTime"
MAINTENANCE_SITE = "https://benoitinc.sharepoint.com/sites/BenoitMaintenance1"
EMPLOYEE_SITE = "https://benoitinc.sharepoint.com/sites/QMS1061"
IT_SITE = "https://benoitinc.sharepoint.com/sites/BenoitIT677"
ACUMATICA_SITE = "https://benoitinc.sharepoint.com/sites/AcumaticaDataStorage"

LISTS = {
    "employees": {"site": EMPLOYEE_SITE, "id": "3574d10f-5582-45d6-829e-fff73cf29635", "name": "Employees"},
    "stations": {"site": MACHINIST_TIME_SITE, "id": "16cf4cdf-71f0-4538-b12e-a9df741e7e49", "name": "Stations"},
    "startstop": {
        "site": MACHINIST_TIME_SITE,
        "id": "c38ea91b-6793-4b90-809c-3362c6b3d0bb",
        "name": "Ennis Start and Stop Time Inputs",
    },
    "timeentry": {
        "site": MACHINIST_TIME_SITE,
        "id": "07d68d05-cb88-4571-b199-c47af3f27ac0",
        "name": "Ennis Machinist Time Entry",
    },
    "detail_types": {
        "site": MACHINIST_TIME_SITE,
        "id": "de31c397-12b7-4ab9-8dd3-6c94a033c4f4",
        "name": "Details Type",
    },
    "maintenance_assets": {
        "site": MAINTENANCE_SITE,
        "id": "58699b59-e003-41d6-96ff-fe16bda550dd",
        "name": "Assets",
    },
    "maintenance_locations": {
        "site": MAINTENANCE_SITE,
        "id": "8b649cb2-f8b3-41bf-8747-af08b79d3ef3",
        "name": "Locations",
    },
    "maintenance_requests": {
        "site": MAINTENANCE_SITE,
        "id": "9bdf2505-e63b-48d8-b55c-475a9ab296d1",
        "name": "Ennis Maintenance Request",
    },
    "checklist": {
        "site": MAINTENANCE_SITE,
        "id": "e5d9d01b-fb27-4abb-8d93-eb285c98c228",
        "name": "CNC Maintenace Pre-Use/Daily Checklist",
    },
    "tech_categories": {
        "site": IT_SITE,
        "id": "74ee8aa3-7227-4b0c-9dde-44d901fe062f",
        "name": "tblTechnicalSupportCategories",
    },
    "work_orders": {
        "site": ACUMATICA_SITE,
        "id": "Production Operations",
        "name": "Production Operations",
    },
}

SHIFT_OPTIONS = [
    {"id": 40, "title": "Day Shift"},
    {"id": 41, "title": "Night Shift"},
]

DEFAULT_DETAIL_TYPES = [
    {"item_id": 1, "title": "Set-Up", "option_step": 2, "type_ii": "", "branch": "Ennis"},
    {"item_id": 2, "title": "Machining", "option_step": 2, "type_ii": "", "branch": "Ennis"},
    {"item_id": 3, "title": "Downtime", "option_step": 2, "type_ii": "Downtime", "branch": "Ennis"},
    {"item_id": 4, "title": "Turn & Bore", "option_step": 2, "type_ii": "", "branch": "Ennis"},
    {"item_id": 5, "title": "Change Over", "option_step": 2, "type_ii": "", "branch": "Ennis"},
]

STATION_DETAIL_TYPES = [
    {"item_id": 901, "title": "Inspection", "option_step": 2, "type_ii": "", "branch": "Ennis"},
    {"item_id": 902, "title": "Sandblast", "option_step": 2, "type_ii": "", "branch": "Ennis"},
    {"item_id": 903, "title": "Drift", "option_step": 2, "type_ii": "", "branch": "Ennis"},
    {"item_id": 904, "title": "Stenciling", "option_step": 2, "type_ii": "", "branch": "Ennis"},
]

DOWNTIME_REASONS = [
    {"code": "M1", "category": "MACHINE", "label": "DT- M1 (MACHINE)"},
    {"code": "M2", "category": "MACHINE", "label": "DT- M2 (MACHINE)"},
    {"code": "M3", "category": "MACHINE", "label": "DT- M3 (MACHINE)"},
    {"code": "M4", "category": "MACHINE", "label": "DT- M4 (MACHINE)"},
    {"code": "M5", "category": "MACHINE", "label": "DT- M5 (MACHINE)"},
    {"code": "M6", "category": "MACHINE", "label": "DT- M6 (MACHINE)"},
    {"code": "INS 1", "category": "INSPECTION", "label": "DT- INS 1 (INSPECTION)"},
    {"code": "INS 2", "category": "INSPECTION", "label": "DT- INS 2 (INSPECTION)"},
    {"code": "INS 3", "category": "INSPECTION", "label": "DT- INS 3 (INSPECTION)"},
    {"code": "INS 4", "category": "INSPECTION", "label": "DT- INS 4 (INSPECTION)"},
    {"code": "SB1", "category": "SANDBLAST", "label": "DT- SB1 (SANDBLAST)"},
    {"code": "SB2", "category": "SANDBLAST", "label": "DT- SB2 (SANDBLAST)"},
    {"code": "SB3", "category": "SANDBLAST", "label": "DT- SB3 (SANDBLAST)"},
    {"code": "SB4", "category": "SANDBLAST", "label": "DT- SB4 (SANDBLAST)"},
    {"code": "SW", "category": "SWAGE", "label": "DT- SW (SWAGE)"},
    {"code": "QA", "category": "QUALITY", "label": "DT- QA (QUALITY)"},
    {"code": "OPS", "category": "OPERATIONS", "label": "DT- OPS (OPERATIONS)"},
    {"code": "AIR", "category": "AIR COMPRESSOR", "label": "DT- AIR (AIR COMPRESSOR)"},
    {"code": "WOP", "category": "WAITING ON PIPE", "label": "DT- WOP (WAITING ON PIPE)"},
    {"code": "TT", "category": "TURN AROUND TABLE", "label": "DT- TT (TURN AROUND TABLE)"},
]

ACUMATICA_DEFAULTS = {
    "branch": "ENNIS",
    "uom": "JOINT",
    "warehouse": "EN-FG SSOT",
    "location": "CUST REC",
    "qty_scrapped": 0,
    "labor_rate": 123.3000,
}

OUTBOX_FILE = Path(__file__).resolve().parents[1] / "data" / "cnc_time_outbox.jsonl"
SITE_ID_CACHE = {}
LIST_CACHE = {}
CACHE_TTL_SECONDS = 30
POSTGRES_READ_ENABLED = os.getenv("CNC_TIME_POSTGRES_READ_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
APP_KEY = "time_entry"
SHARED_REFERENCE_FALLBACK_APP_KEY = "irr"
SHARED_REFERENCE_LIST_KEYS = {"employees", "work_orders"}
SHAREPOINT_WRITES_ENABLED = os.getenv("CNC_TIME_SHAREPOINT_WRITES_ENABLED", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
ALLOW_DIRECT_SHAREPOINT_READS = os.getenv("CNC_TIME_ALLOW_DIRECT_SHAREPOINT_READS", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
EXPORTER_DEPARTMENT_KEYWORDS = tuple(
    item.strip().lower()
    for item in os.getenv("CNC_TIME_EXPORT_DEPARTMENT_KEYWORDS", "Logistics 1,shipping,receiving,shipping and receiving").split(",")
    if item.strip()
)
EXPORTER_ADP_NUMBERS = {
    (item.strip()[:-2] if item.strip().endswith(".0") else item.strip()).lstrip("0") or item.strip()
    for item in os.getenv("CNC_TIME_EXPORT_ADP_NUMBERS", "").replace(";", ",").split(",")
    if item.strip()
}
APPROVER_DEPARTMENT_KEYWORDS = tuple(
    item.strip().lower()
    for item in os.getenv("CNC_TIME_APPROVER_DEPARTMENT_KEYWORDS", "supervisor,manager,lead,production manager").split(",")
    if item.strip()
)
APPROVER_ADP_NUMBERS = {
    (item.strip()[:-2] if item.strip().endswith(".0") else item.strip()).lstrip("0") or item.strip()
    for item in os.getenv("CNC_TIME_APPROVER_ADP_NUMBERS", "").replace(";", ",").split(",")
    if item.strip()
}

WRITE_RECORD_TARGETS = {
    "startstop": {"list_key": "startstop", "mode": "create"},
    "misc_time": {"list_key": "timeentry", "mode": "create"},
    "manual_time": {"list_key": "timeentry", "mode": "create"},
    "daily_checklist": {"list_key": "checklist", "mode": "create"},
    "maintenance_request": {"list_key": "maintenance_requests", "mode": "create"},
    "stop_time_entry": {"list_key": "timeentry", "mode": "create"},
    "pause_lunch": {"list_key": "startstop", "mode": "update"},
    "resume_lunch": {"list_key": "startstop", "mode": "update"},
    "complete_startstop": {"list_key": "startstop", "mode": "update"},
    "active_time_correction": {"list_key": "startstop", "mode": "update"},
    "timeentry_correction": {"list_key": "timeentry", "mode": "update"},
    "startstop_delete": {"list_key": "startstop", "mode": "delete"},
    "timeentry_delete": {"list_key": "timeentry", "mode": "delete"},
}


class SharePointWritesDisabled(Exception):
    """Raised when the app is running in local-only write mode."""


def _utc_now():
    return datetime.now(timezone.utc)


def _iso_now():
    return _utc_now().isoformat()


def _get_time_entry_access_token():
    return get_access_token_from_env("CNC_TIME_SHAREPOINT", allow_interactive=False)


def _site_id(site_url, headers):
    cached = SITE_ID_CACHE.get(site_url)
    if cached:
        return cached
    resolved = get_site_id(site_url, headers)
    SITE_ID_CACHE[site_url] = resolved
    return resolved


def _request(method, endpoint, headers, payload=None):
    response = requests.request(
        method,
        f"{GRAPH_BASE_URL}{endpoint}",
        headers=headers,
        json=payload,
        timeout=30,
    )
    if response.status_code not in {200, 201, 204}:
        raise SharePointApiError(
            f"Graph request failed ({response.status_code}) for {endpoint}: {response.text}"
        )
    if response.status_code == 204 or not response.text.strip():
        return {}
    return response.json()


def _read_synced_list(list_key):
    """Read a SharePoint list from the IRR-style PostgreSQL sync table."""
    if not POSTGRES_READ_ENABLED:
        return []

    config = LISTS[list_key]
    connection = get_db_connection()
    try:
        initialize_database(connection)
        with connection.cursor() as cursor:
            rows = _fetch_synced_list_rows(cursor, config["name"], APP_KEY)
            if not rows and list_key in SHARED_REFERENCE_LIST_KEYS:
                rows = _fetch_synced_list_rows(
                    cursor,
                    config["name"],
                    SHARED_REFERENCE_FALLBACK_APP_KEY,
                )
    finally:
        connection.close()

    items = []
    for sharepoint_item_id, etag, web_url, fields_json, raw_item_json in rows:
        raw_item = raw_item_json if isinstance(raw_item_json, dict) else {}
        item = dict(raw_item)
        item["id"] = str(sharepoint_item_id)
        item["eTag"] = item.get("eTag") or etag
        item["webUrl"] = item.get("webUrl") or web_url
        item["fields"] = fields_json if isinstance(fields_json, dict) else {}
        items.append(item)
    return items


def _fetch_synced_list_rows(cursor, list_name, app_key):
    cursor.execute(
        """
        SELECT si.sharepoint_item_id, si.etag, si.web_url, si.fields_json, si.raw_item_json
        FROM sharepoint_items si
        JOIN sharepoint_lists sl ON sl.id = si.list_id
        WHERE sl.list_name = %s
          AND sl.app_key = %s
        ORDER BY si.id
        """,
        (list_name, app_key),
    )
    return cursor.fetchall()


def _cache_fetched_list(list_key, items, graph_site_id=None):
    """Persist a directly fetched SharePoint list so later page loads use Postgres."""
    if not POSTGRES_READ_ENABLED or not items:
        return

    config = LISTS[list_key]
    connection = get_db_connection()
    try:
        initialize_database(connection)
        sync_list_to_postgres(
            connection,
            config["site"],
            *parse_site_parts(config["site"]),
            config["name"],
            items,
            graph_site_id or _site_id(config["site"], build_headers(_get_time_entry_access_token())),
            app_key=APP_KEY,
        )
    finally:
        connection.close()


def _patch_synced_item_fields(list_key, item_id, fields):
    """Patch the cached JSON fields for a synced SharePoint item."""
    if not POSTGRES_READ_ENABLED or not item_id or not fields:
        return

    config = LISTS[list_key]
    connection = get_db_connection()
    try:
        initialize_database(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE sharepoint_items si
                SET fields_json = fields_json || %s::jsonb,
                    raw_item_json = jsonb_set(
                        COALESCE(raw_item_json, '{}'::jsonb),
                        '{fields}',
                        COALESCE(raw_item_json->'fields', '{}'::jsonb) || %s::jsonb,
                        true
                    ),
                    synced_at = NOW()
                FROM sharepoint_lists sl
                WHERE sl.id = si.list_id
                  AND sl.list_name = %s
                  AND sl.app_key = %s
                  AND si.sharepoint_item_id = %s
                """,
                (json.dumps(fields), json.dumps(fields), config["name"], APP_KEY, str(item_id)),
            )
        connection.commit()
    finally:
        connection.close()


def parse_site_parts(site_url):
    from sharepoint_client import parse_site_url

    return parse_site_url(site_url)


def _read_list(list_key, fetch_all=True, top=500):
    cache_key = (list_key, fetch_all, top)
    now = datetime.now(timezone.utc)
    cached = LIST_CACHE.get(cache_key)
    if cached is not None:
        cached_time = cached.get("time")
        if cached_time and (now - cached_time).total_seconds() < CACHE_TTL_SECONDS:
            return cached["items"]

    try:
        postgres_items = _read_synced_list(list_key)
        if postgres_items:
            LIST_CACHE[cache_key] = {"items": postgres_items, "time": now}
            return postgres_items
    except Exception:
        pass

    if not ALLOW_DIRECT_SHAREPOINT_READS:
        LIST_CACHE[cache_key] = {"items": [], "time": now}
        return []

    token = _get_time_entry_access_token()
    headers = build_headers(token)
    config = LISTS[list_key]
    site_id = _site_id(config["site"], headers)
    items = get_list_items(
        config["site"],
        config["id"],
        headers=headers,
        top=top,
        site_id=site_id,
        fetch_all=fetch_all,
    )
    try:
        _cache_fetched_list(list_key, items, graph_site_id=site_id)
    except Exception:
        pass
    LIST_CACHE[cache_key] = {"items": items, "time": now}
    return items


def sync_reference_data_from_sharepoint():
    """Refresh CNC SharePoint source lists into PostgreSQL."""
    result = sync_cnc_time_lists_to_postgres()
    LIST_CACHE.clear()
    sync_counts = result.get("sync_counts", {})
    return {"synced": True, "sync_counts": sync_counts}


def sync_pending_writes_to_sharepoint(limit=50):
    """Replay locally queued app writes to SharePoint when connectivity returns."""
    ensure_db()
    records = list_pending_records(limit=limit)
    if not SHAREPOINT_WRITES_ENABLED:
        return {
            "synced": 0,
            "failed": 0,
            "skipped": len(records),
            "dry_run": True,
            "message": "SharePoint writes are disabled.",
        }

    synced = 0
    failed = 0
    skipped = 0
    errors = []

    for record in records:
        target = WRITE_RECORD_TARGETS.get(record.get("record_type"))
        if not target:
            skipped += 1
            continue

        try:
            payload = json.loads(record.get("payload") or "{}")
            fields = payload.get("fields") or {}
            if target["mode"] != "delete" and not fields:
                skipped += 1
                continue

            if target["mode"] == "delete":
                item_id = payload.get("item_id") or payload.get("entry_id")
                if not item_id:
                    skipped += 1
                    continue
                _delete_item(target["list_key"], item_id)
            elif target["mode"] == "update":
                item_id = payload.get("item_id") or payload.get("entry_id")
                if not item_id:
                    skipped += 1
                    continue
                _update_item(target["list_key"], item_id, fields)
            else:
                _create_item(target["list_key"], fields)

            mark_record_synced(record["id"])
            synced += 1
        except Exception as error:
            failed += 1
            if len(errors) < 5:
                errors.append(str(error))

    return {"synced": synced, "failed": failed, "skipped": skipped, "errors": errors}


def sync_background_jobs():
    """Run the periodic CNC sync jobs used by the Node app scheduler."""
    pending_result = sync_pending_writes_to_sharepoint()
    reference_result = sync_reference_data_from_sharepoint()
    return {
        "pending_writes": pending_result,
        "reference_data": reference_result,
    }


def _create_item(list_key, fields):
    if not SHAREPOINT_WRITES_ENABLED:
        raise SharePointWritesDisabled("SharePoint writes are disabled.")

    token = _get_time_entry_access_token()
    headers = build_headers(token)
    headers["Content-Type"] = "application/json"
    config = LISTS[list_key]
    site_id = _site_id(config["site"], headers)
    item = _request(
        "POST",
        f"/sites/{site_id}/lists/{config['id']}/items",
        headers,
        {"fields": fields},
    )
    try:
        _cache_fetched_list(list_key, [item], graph_site_id=site_id)
    except Exception:
        pass
    return item


def _update_item(list_key, item_id, fields):
    if not SHAREPOINT_WRITES_ENABLED:
        raise SharePointWritesDisabled("SharePoint writes are disabled.")

    token = _get_time_entry_access_token()
    headers = build_headers(token)
    headers["Content-Type"] = "application/json"
    config = LISTS[list_key]
    site_id = _site_id(config["site"], headers)
    _request(
        "PATCH",
        f"/sites/{site_id}/lists/{config['id']}/items/{item_id}/fields",
        headers,
        fields,
    )
    try:
        _patch_synced_item_fields(list_key, item_id, fields)
    except Exception:
        pass


def _delete_item(list_key, item_id):
    if not SHAREPOINT_WRITES_ENABLED:
        raise SharePointWritesDisabled("SharePoint writes are disabled.")

    token = _get_time_entry_access_token()
    headers = build_headers(token)
    config = LISTS[list_key]
    site_id = _site_id(config["site"], headers)
    _request(
        "DELETE",
        f"/sites/{site_id}/lists/{config['id']}/items/{item_id}",
        headers,
    )
    try:
        _delete_synced_item(list_key, item_id)
    except Exception:
        pass


def _delete_synced_item(list_key, item_id):
    if not POSTGRES_READ_ENABLED or not item_id:
        return

    config = LISTS[list_key]
    connection = get_db_connection()
    try:
        initialize_database(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM sharepoint_items si
                USING sharepoint_lists sl
                WHERE sl.id = si.list_id
                  AND sl.list_name = %s
                  AND sl.app_key = %s
                  AND si.sharepoint_item_id = %s
                """,
                (config["name"], APP_KEY, str(item_id)),
            )
        connection.commit()
    finally:
        connection.close()


def _value(fields, *keys):
    for key in keys:
        if key in fields and fields[key] not in (None, ""):
            return fields[key]
    return None


def _joined_values(fields, *keys):
    values = []
    for key in keys:
        value = fields.get(key)
        if value not in (None, ""):
            values.append(str(value).strip())
    return " ".join(value for value in values if value)


def _as_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1", "yes"}


def _as_number(value, default=0):
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_identifier(value):
    text = str(value or "").strip()
    if text.endswith(".0"):
        text = text[:-2]
    normalized = text.lstrip("0")
    return normalized or text


def _shift_label(shift_id):
    for option in SHIFT_OPTIONS:
        if str(option["id"]) == str(shift_id):
            return option["title"]
    return str(shift_id or "")


def _hhmm_from_minutes(minutes_value):
    total_minutes = max(int(round(_as_number(minutes_value))), 0)
    hours = total_minutes // 60
    minutes = total_minutes % 60
    return f"{hours:02d}:{minutes:02d}"


def _normalize_employee(item):
    fields = item.get("fields", {})
    full_name = _value(fields, "Full_x0020_Name", "FullName", "Title")
    department = _value(
        fields,
        "DepartmentName",
        "Department",
        "HomeDepartment",
        "field_20",
        "Dept",
        "EmployeeDepartment",
    )
    title = _value(
        fields,
        "PositionTitle",
        "ADPJobTitleDesc",
        "JobTitle",
        "Title_x0020_Position",
        "Position",
        "field_6",
        "field_21",
    )
    return {
        "item_id": item.get("id"),
        "emp_id": str(_value(fields, "ADPEmpNumber", "field_0", "EmployeeID", "EmpID") or "").strip(),
        "full_name": str(full_name or "").strip(),
        "first_name": str(_value(fields, "First_x0020_Name", "field_2") or "").strip(),
        "last_name": str(_value(fields, "Last_x0020_Name", "field_4") or "").strip(),
        "branch": str(_value(fields, "Branches", "field_22", "Branch") or "").strip(),
        "status": str(_value(fields, "Status", "field_19") or "").strip(),
        "machinist": _as_bool(_value(fields, "Machinist", "machinist", "IsMachinist")),
        "department": str(department or "").strip(),
        "job_title": str(title or "").strip(),
        "role_text": _joined_values(
            fields,
            "DepartmentName",
            "HomeDepartment",
            "PositionTitle",
            "ADPJobTitleDesc",
            "field_6",
            "JobTitleCode2",
        ),
    }


def _is_active_ennis_employee(employee):
    return employee["status"].lower() == "active" and employee["branch"].lower() == "ennis"


def _employee_roles(employee):
    roles = []
    normalized_emp_id = _normalize_identifier(employee.get("emp_id"))
    if employee.get("machinist"):
        roles.append("operator")
    searchable = " ".join(
        str(employee.get(key) or "").lower()
        for key in ("department", "job_title", "role_text", "full_name")
    )
    if normalized_emp_id in EXPORTER_ADP_NUMBERS or any(keyword in searchable for keyword in EXPORTER_DEPARTMENT_KEYWORDS):
        roles.append("exporter")
    if normalized_emp_id in APPROVER_ADP_NUMBERS or any(keyword in searchable for keyword in APPROVER_DEPARTMENT_KEYWORDS):
        roles.append("approver")
        if "operator" not in roles:
            roles.insert(0, "operator")
    return roles


def lookup_employee_by_adp(adp_number, employee_items=None, required_role=None):
    items = employee_items if employee_items is not None else _read_list("employees")
    normalized_adp_number = _normalize_identifier(adp_number)
    for item in items:
        candidate = _normalize_employee(item)
        if _normalize_identifier(candidate["emp_id"]) != normalized_adp_number or not _is_active_ennis_employee(candidate):
            continue
        candidate["roles"] = _employee_roles(candidate)
        if required_role and required_role not in candidate["roles"]:
            continue
        if not required_role and not candidate["roles"]:
            continue
        return candidate
    return None


def get_employee_lookup(adp_number):
    employee = lookup_employee_by_adp(adp_number)
    if not employee:
        return None
    return {
        "emp_id": employee["emp_id"],
        "full_name": employee["full_name"],
        "first_name": employee["first_name"],
        "last_name": employee["last_name"],
        "roles": employee.get("roles", []),
        "department": employee.get("department", ""),
    }


def _normalize_station(item):
    fields = item.get("fields", {})
    return {
        "item_id": item.get("id"),
        "machine_no": str(_value(fields, "Title", "MachineNo") or "").strip(),
        "email": str(_value(fields, "EmailTest") or "").strip().lower(),
        "location": str(_value(fields, "Location") or "").strip(),
    }


def _normalize_detail_type(item):
    fields = item.get("fields", {})
    branch = _value(fields, "BranchValue", "Branch", "Branch0")
    if isinstance(branch, dict):
        branch = branch.get("Value") or branch.get("value")
    return {
        "item_id": int(_as_number(_value(fields, "ID", "Id"), 0)),
        "title": str(_value(fields, "Title") or "").strip(),
        "option_step": int(_as_number(_value(fields, "OptionStep"), 0)),
        "type_ii": str(_value(fields, "TypeII") or "").strip(),
        "branch": str(branch or "").strip(),
    }


def _detail_types():
    details = [
        item
        for item in (_normalize_detail_type(entry) for entry in _read_list("detail_types"))
        if item["branch"].lower() == "ennis"
    ]
    if not details:
        details = list(DEFAULT_DETAIL_TYPES)

    existing_titles = {item["title"].strip().lower() for item in details}
    for detail in STATION_DETAIL_TYPES:
        if detail["title"].lower() not in existing_titles:
            details.append(dict(detail))
            existing_titles.add(detail["title"].lower())
    return details


def _downtime_reason_code(value):
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("Downtime reason is required.")

    normalized = raw.upper().removeprefix("DT-").strip()
    if "(" in normalized:
        normalized = normalized.split("(", 1)[0].strip()
    normalized = " ".join(normalized.split())

    for reason in DOWNTIME_REASONS:
        if normalized in {
            reason["code"].upper(),
            reason["label"].upper(),
            f"DT- {reason['code']}".upper(),
        }:
            return reason["code"]

    raise ValueError("The selected downtime reason is not valid.")


def _detail_code(detail_type, downtime_reason=None):
    detail = str(detail_type or "").strip()
    if detail.lower() in {"downtime", "dt"}:
        return "DT", _downtime_reason_code(downtime_reason) if str(downtime_reason or "").strip() else ""
    return detail, ""


def _time_entry_comment(detail_code, reason_code="", comments=None, fallback=""):
    note = str(comments or "").strip()
    if detail_code == "DT":
        if reason_code and note:
            return f"{reason_code} - {note}"
        return reason_code or note
    return note or str(fallback or "").strip()


def _break_counts_against_production(break_event):
    break_type = str((break_event or {}).get("break_type") or "Lunch").strip()
    return break_type in {"Lunch", "No Relief"}


def _get_operation_for_employee(employee, production_number, operation_id, error_message):
    context = get_dashboard_context(employee["emp_id"], "")
    operation = next(
        (
            item
            for item in context["operations"]
            if item["production_number"] == production_number and item["operation_id"] == operation_id
        ),
        None,
    )
    if not operation:
        raise ValueError(error_message)
    return operation


def _get_operation(production_number, operation_id, error_message):
    operation = next(
        (
            item
            for item in (_normalize_work_order(entry) for entry in _read_list("work_orders", fetch_all=True))
            if item["production_number"] == production_number and item["operation_id"] == operation_id
        ),
        None,
    )
    if not operation:
        raise ValueError(error_message)
    return operation


def _operation_fields(operation, production_number, operation_id):
    return {
        "Title": production_number,
        "ProductionNo": production_number,
        "OrderType": operation["order_type"] or "EN",
        "InventoryID": operation["inventory_id"],
        "Description": operation["description"],
        "OperationDescription": operation["operation_description"],
        "OperationID": operation_id,
    }


def _is_cnc_machine(machine_no):
    text = str(machine_no or "").strip().lower()
    if not text:
        return False
    if text.startswith("cnc "):
        text = text.removeprefix("cnc ").strip()
    return text.isdigit()


def _is_cnc_machinist_time(employee, machine_no):
    return bool((employee or {}).get("machinist")) and _is_cnc_machine(machine_no)


def _attach_acumatica_payload(payload, employee, fields, reason_code=""):
    payload.pop("acumatica_labor_transaction", None)
    payload.pop("acumatica_labor_transactions", None)
    if not _is_cnc_machinist_time(employee, fields.get("MachineNo")):
        return payload

    detail_code = str(fields.get("DetailsType") or "").strip()
    if detail_code == "DT" and reason_code:
        transactions = _acumatica_downtime_transactions(fields, reason_code)
        payload["acumatica_labor_transaction"] = transactions[0]
        payload["acumatica_labor_transactions"] = transactions
    else:
        payload["acumatica_labor_transaction"] = _acumatica_labor_transaction(fields, reason_code=reason_code)
    return payload


def _acumatica_labor_transaction(fields, reason_code="", labor_rate=None):
    total_minutes = int(_as_number(fields.get("TotalMinutes"), 0))
    quantity = _as_number(fields.get("Quantity"), 0)
    rate = _as_number(labor_rate, ACUMATICA_DEFAULTS["labor_rate"])
    labor_hours = round(total_minutes / 60, 4)
    return {
        "tran_description": str(fields.get("TranDescription") or fields.get("DetailsType") or "").strip(),
        "detail_type": str(fields.get("DetailsType") or "").strip(),
        "employee_id": str(fields.get("EmployeeID") or "").strip(),
        "machine_no": str(fields.get("MachineNo") or "").strip(),
        "labor_type": str(fields.get("LaborType") or "Direct").strip(),
        "order_type": str(fields.get("OrderType") or "EN").strip(),
        "production_number": str(fields.get("ProductionNo") or fields.get("Title") or "").strip(),
        "operation_id": str(fields.get("OperationID") or "").strip(),
        "inventory_id": str(fields.get("InventoryID") or "").strip(),
        "branch": ACUMATICA_DEFAULTS["branch"],
        "shift": str(fields.get("Shift") or "").strip(),
        "labor_time": _hhmm_from_minutes(total_minutes),
        "labor_minutes": total_minutes,
        "labor_hours": labor_hours,
        "labor_rate": rate,
        "labor_amount": round(labor_hours * rate, 2),
        "quantity": quantity,
        "uom": ACUMATICA_DEFAULTS["uom"],
        "warehouse": ACUMATICA_DEFAULTS["warehouse"],
        "location": ACUMATICA_DEFAULTS["location"],
        "qty_scrapped": ACUMATICA_DEFAULTS["qty_scrapped"],
        "reason_code": reason_code or "",
    }


def _acumatica_downtime_transactions(fields, reason_code):
    positive = _acumatica_labor_transaction(fields, reason_code=reason_code)
    negative = dict(positive)
    negative["labor_time"] = "-00:01"
    negative["labor_minutes"] = -1
    negative["labor_hours"] = round(-1 / 60, 4)
    negative["labor_amount"] = round(negative["labor_hours"] * negative["labor_rate"], 2)
    negative["quantity"] = -1
    negative["qty_scrapped"] = 0
    return [positive, negative]


def _normalize_work_order(item):
    fields = item.get("fields", {})
    return {
        "production_number": str(_value(fields, "ProductionNumber", "ProductionNo", "Title") or "").strip(),
        "inventory_id": str(_value(fields, "InventoryID", "Inventory_x0020_ID") or "").strip(),
        "description": str(_value(fields, "Description") or "").strip(),
        "operation_id": str(_value(fields, "OperationID", "Operation_x0020_ID") or "").strip(),
        "operation_description": str(
            _value(fields, "OperationDescription", "Operation_x0020_Description") or ""
        ).strip(),
        "status": str(_value(fields, "Status") or "").strip(),
        "order_type": str(_value(fields, "OrderType", "Order_x0020_Type") or "").strip(),
        "labor_input": str(_value(fields, "LaborInput", "Labor_x0020_Input") or "").strip(),
    }


def _normalize_startstop(item):
    fields = item.get("fields", {})
    return {
        "entry_list": "startstop",
        "id": int(_as_number(fields.get("ID"), 0)),
        "sp_id": item.get("id"),
        "status": str(_value(fields, "Status") or "").strip(),
        "labor_date": fields.get("LaborDate"),
        "production_number": str(_value(fields, "ProductionNo", "Title") or "").strip(),
        "inventory_id": str(_value(fields, "InventoryID") or "").strip(),
        "description": str(_value(fields, "Description") or "").strip(),
        "operation_id": str(_value(fields, "OperationID") or "").strip(),
        "operation_description": str(_value(fields, "OperationDescription") or "").strip(),
        "machine_no": str(_value(fields, "MachineNo") or "").strip(),
        "details_type": str(_value(fields, "DetailsType") or "").strip(),
        "tran_description": str(_value(fields, "TranDescription") or "").strip(),
        "emp_id": str(_value(fields, "EmpID", "EmployeeID") or "").strip(),
        "employee_id": str(_value(fields, "EmployeeID") or "").strip(),
        "operators_name": str(_value(fields, "OperatorsName") or "").strip(),
        "shift": str(_value(fields, "Shift") or "").strip(),
        "start": fields.get("Start"),
        "lunch_start": fields.get("LunchStart"),
        "lunch_stop": fields.get("LunchStop"),
        "end": fields.get("End"),
        "break_minutes": _as_number(fields.get("BreakMinutes"), 0),
        "total_minutes": _as_number(fields.get("TotalMinutes"), 0),
        "total": str(_value(fields, "Total") or "").strip(),
        "quantity": _as_number(fields.get("Quantity"), 0),
        "average": _as_number(fields.get("Average"), 0),
        "submitted": _as_bool(fields.get("Submitted")),
    }


def _normalize_timeentry(item):
    fields = item.get("fields", {})
    return {
        "entry_list": "timeentry",
        "id": int(_as_number(fields.get("ID"), 0)),
        "sp_id": item.get("id"),
        "status": str(_value(fields, "Status") or "").strip(),
        "labor_date": fields.get("LaborDate"),
        "production_number": str(_value(fields, "ProductionNo", "Title") or "").strip(),
        "inventory_id": str(_value(fields, "InventoryID") or "").strip(),
        "description": str(_value(fields, "Description") or "").strip(),
        "operation_id": str(_value(fields, "OperationID") or "").strip(),
        "operation_description": str(_value(fields, "OperationDescription") or "").strip(),
        "machine_no": str(_value(fields, "MachineNo") or "").strip(),
        "details_type": str(_value(fields, "DetailsType") or "").strip(),
        "details_type_ii": str(_value(fields, "DetailsTypeII") or "").strip(),
        "tran_description": str(_value(fields, "TranDescription") or "").strip(),
        "emp_id": str(_value(fields, "EmpID", "EmployeeID") or "").strip(),
        "employee_id": str(_value(fields, "EmployeeID") or "").strip(),
        "operators_name": str(_value(fields, "OperatorsName") or "").strip(),
        "shift": str(_value(fields, "Shift") or "").strip(),
        "start": fields.get("Start"),
        "lunch_start": fields.get("LunchStart"),
        "lunch_stop": fields.get("LunchStop"),
        "end": fields.get("End"),
        "break_minutes": _as_number(fields.get("BreakMinutes"), 0),
        "total_minutes": _as_number(fields.get("TotalMinutes"), 0),
        "total": str(_value(fields, "Total") or "").strip(),
        "quantity": _as_number(fields.get("Quantity"), 0),
        "average": _as_number(fields.get("Average"), 0),
        "start_stop_id": _as_number(fields.get("StartStopID"), 0),
    }


def _pending_payload(record):
    try:
        return json.loads(record.get("payload") or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}


def _approval_payload(record):
    try:
        return json.loads(record.get("payload") or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}


def _approval_context_entry(record):
    payload = _approval_payload(record)
    fields = payload.get("fields") or {}
    entry = _normalize_timeentry({"id": str(record["id"]), "fields": fields})
    entry.update(
        {
            "approval_id": record["id"],
            "record_type": record.get("record_type"),
            "submitted_at": record.get("submitted_at"),
            "reviewed_at": record.get("reviewed_at"),
            "reviewed_by_emp_id": record.get("reviewed_by_emp_id"),
            "reviewed_by_name": record.get("reviewed_by_name"),
            "review_note": record.get("review_note"),
            "approval_status": record.get("approval_status"),
            "employee_name": record.get("employee_name") or entry.get("operators_name"),
            "emp_id": record.get("emp_id") or entry.get("emp_id"),
            "machine_no": record.get("machine_no") or entry.get("machine_no"),
            "has_acumatica_payload": bool(
                payload.get("acumatica_labor_transaction") or payload.get("acumatica_labor_transactions")
            ),
        }
    )
    if entry.get("approval_status") == "pending":
        entry["status"] = "Pending Approval"
    return entry


def _refresh_approval_acumatica_payload(payload, fields):
    detail_code = str(fields.get("DetailsType") or "").strip()
    reason_code = str(fields.get("DetailsTypeII") or "").strip()
    had_acumatica_payload = bool(
        payload.get("acumatica_labor_transaction") or payload.get("acumatica_labor_transactions")
    )
    employee = payload.get("employee") or {
        "machinist": bool(
            payload.get("is_cnc_machinist_time")
            or _is_cnc_machine(fields.get("MachineNo"))
            or (had_acumatica_payload and _is_cnc_machine(fields.get("MachineNo")))
        )
    }
    return _attach_acumatica_payload(payload, employee, fields, reason_code if detail_code == "DT" else "")


def _local_record_key(value):
    text = str(value or "").strip()
    return text.removeprefix("local:")


def _approval_record_key(value):
    text = str(value or "").strip()
    return text.removeprefix("approval:")


def _local_startstop_entries(emp_id=None):
    starts = {}
    for record in reversed(list_pending_records(limit=500)):
        payload = _pending_payload(record)
        fields = payload.get("fields") or {}
        record_type = record.get("record_type")

        if record_type == "startstop":
            local_id = str(record["id"])
            item = _normalize_startstop({"id": local_id, "fields": fields})
            item["id"] = record["id"]
            item["sp_id"] = f"local:{record['id']}"
            starts[local_id] = item
            continue

        if record_type in {"pause_lunch", "resume_lunch", "complete_startstop", "active_time_correction"}:
            target_id = _local_record_key(payload.get("item_id") or payload.get("entry_id"))
            if target_id in starts:
                updated = dict(starts[target_id])
                updated_fields = dict(fields)
                if "LunchStart" in updated_fields:
                    updated["lunch_start"] = updated_fields["LunchStart"]
                if "LunchStop" in updated_fields:
                    updated["lunch_stop"] = updated_fields["LunchStop"]
                if "Status" in updated_fields:
                    updated["status"] = str(updated_fields["Status"] or "").strip()
                if "End" in updated_fields:
                    updated["end"] = updated_fields["End"]
                if "BreakMinutes" in updated_fields:
                    updated["break_minutes"] = _as_number(updated_fields["BreakMinutes"], updated["break_minutes"])
                if "TotalMinutes" in updated_fields:
                    updated["total_minutes"] = _as_number(updated_fields["TotalMinutes"], updated["total_minutes"])
                if "Total" in updated_fields:
                    updated["total"] = str(updated_fields["Total"] or "")
                if "Quantity" in updated_fields:
                    updated["quantity"] = _as_number(updated_fields["Quantity"], updated["quantity"])
                if "Average" in updated_fields:
                    updated["average"] = _as_number(updated_fields["Average"], updated["average"])
                if "Title" in updated_fields or "ProductionNo" in updated_fields:
                    updated["production_number"] = str(
                        updated_fields.get("ProductionNo") or updated_fields.get("Title") or updated["production_number"]
                    )
                if "InventoryID" in updated_fields:
                    updated["inventory_id"] = str(updated_fields["InventoryID"] or "")
                if "Description" in updated_fields:
                    updated["description"] = str(updated_fields["Description"] or "")
                if "OperationID" in updated_fields:
                    updated["operation_id"] = str(updated_fields["OperationID"] or "")
                if "OperationDescription" in updated_fields:
                    updated["operation_description"] = str(updated_fields["OperationDescription"] or "")
                if "DetailsType" in updated_fields:
                    updated["details_type"] = str(updated_fields["DetailsType"] or "")
                if "TranDescription" in updated_fields:
                    updated["tran_description"] = str(updated_fields["TranDescription"] or "")
                starts[target_id] = updated

    entries = list(starts.values())
    if emp_id is not None:
        entries = [item for item in entries if _normalize_identifier(item.get("emp_id")) == _normalize_identifier(emp_id)]
    return entries


def _local_timeentry_entries(emp_id=None):
    entries_by_id = {}
    corrections = []
    for record in list_pending_records(limit=500):
        record_type = record.get("record_type")
        payload = _pending_payload(record)
        fields = payload.get("fields") or {}

        if record_type == "timeentry_correction":
            corrections.append((payload, fields))
            continue

        if record_type not in {"stop_time_entry", "misc_time", "manual_time"}:
            continue
        if not fields:
            continue
        item = _normalize_timeentry({"id": str(record["id"]), "fields": fields})
        item["id"] = record["id"]
        item["sp_id"] = f"local:{record['id']}"
        entries_by_id[str(record["id"])] = item

    for record in list_approval_records(status="pending", limit=500):
        payload = _approval_payload(record)
        fields = payload.get("fields") or {}
        if not fields:
            continue
        item = _normalize_timeentry({"id": str(record["id"]), "fields": fields})
        item["id"] = record["id"]
        item["sp_id"] = f"approval:{record['id']}"
        item["status"] = "Pending Approval"
        entries_by_id[f"approval:{record['id']}"] = item

    for payload, fields in corrections:
        target_id = _local_record_key(payload.get("item_id") or payload.get("entry_id"))
        if target_id in entries_by_id:
            updated = dict(entries_by_id[target_id])
            if "Quantity" in fields:
                updated["quantity"] = _as_number(fields["Quantity"], updated["quantity"])
            if "BreakMinutes" in fields:
                updated["break_minutes"] = _as_number(fields["BreakMinutes"], updated["break_minutes"])
            if "TotalMinutes" in fields:
                updated["total_minutes"] = _as_number(fields["TotalMinutes"], updated["total_minutes"])
            if "Total" in fields:
                updated["total"] = str(fields["Total"] or "")
            if "Average" in fields:
                updated["average"] = _as_number(fields["Average"], updated["average"])
            if "Title" in fields or "ProductionNo" in fields:
                updated["production_number"] = str(fields.get("ProductionNo") or fields.get("Title") or updated["production_number"])
            if "InventoryID" in fields:
                updated["inventory_id"] = str(fields["InventoryID"] or "")
            if "Description" in fields:
                updated["description"] = str(fields["Description"] or "")
            if "OperationID" in fields:
                updated["operation_id"] = str(fields["OperationID"] or "")
            if "OperationDescription" in fields:
                updated["operation_description"] = str(fields["OperationDescription"] or "")
            if "DetailsType" in fields:
                updated["details_type"] = str(fields["DetailsType"] or "")
            if "DetailsTypeII" in fields:
                updated["details_type_ii"] = str(fields["DetailsTypeII"] or "")
            if "TranDescription" in fields:
                updated["tran_description"] = str(fields["TranDescription"] or "")
            entries_by_id[target_id] = updated

    entries = list(entries_by_id.values())

    if emp_id is not None:
        entries = [item for item in entries if _normalize_identifier(item.get("emp_id")) == _normalize_identifier(emp_id)]
    return entries


def _normalize_location(item):
    fields = item.get("fields", {})
    return {
        "id": int(_as_number(_value(fields, "LocationID"), 0)),
        "title": str(_value(fields, "Title") or "").strip(),
    }


def _normalize_asset(item):
    fields = item.get("fields", {})
    return {
        "id": int(_as_number(item.get("id"), 0)),
        "title": str(_value(fields, "Title") or "").strip(),
        "location_id": int(_as_number(_value(fields, "LocationId"), 0)),
        "asset_id": int(_as_number(_value(fields, "AssetId"), 0)),
        "description": str(_value(fields, "Description") or "").strip(),
    }


def _normalize_tech_category(item):
    fields = item.get("fields", {})
    return {
        "id": int(_as_number(_value(fields, "CategoryID"), 0)),
        "title": str(_value(fields, "TechnicalCategory") or "").strip(),
        "description": str(_value(fields, "Description") or "").strip(),
    }


def _queue_notification(kind, payload):
    OUTBOX_FILE.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "kind": kind,
        "queued_at": _iso_now(),
        "payload": payload,
    }
    with OUTBOX_FILE.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record) + "\n")
    return record


def _load_local_machine_config():
    if not LOCAL_MACHINE_CONFIG.exists():
        return []
    try:
        with LOCAL_MACHINE_CONFIG.open("r", encoding="utf-8") as handle:
            entries = json.load(handle) or []
    except (json.JSONDecodeError, OSError):
        return []

    normalized = []
    for entry in entries:
        machine_no = str(entry.get("machine_no") or "").strip()
        email = str(entry.get("email") or "").strip().lower()
        if machine_no and email:
            normalized.append({
                "machine_no": machine_no,
                "email": email,
                "label": str(entry.get("label") or f"Machine {machine_no}").strip(),
            })
    return normalized


def get_sign_in_context():
    local_machines = _load_local_machine_config()
    if local_machines:
        return {
            "shift_options": SHIFT_OPTIONS,
            "machine_options": local_machines,
            "station_emails": sorted({item["email"] for item in local_machines}),
        }

    try:
        stations = [_normalize_station(item) for item in _read_list("stations")]
    except Exception:
        stations = []

    local_machine_map = {str(item["machine_no"]).strip(): item for item in local_machines}
    machine_options = []
    for station in sorted(stations, key=lambda item: str(item["machine_no"] or "")):
        machine_no = str(station.get("machine_no") or "").strip()
        if not machine_no or not station.get("email"):
            continue
        if local_machine_map and machine_no not in local_machine_map:
            continue
        machine_options.append({
            "machine_no": machine_no,
            "email": str(station["email"]).strip().lower(),
            "label": local_machine_map.get(machine_no, {}).get("label") or f"Machine {machine_no}",
        })

    if not machine_options:
        machine_options = _load_local_machine_config()

    if not machine_options:
        machine_options = get_machine_options()

    if machine_options:
        upsert_machine_options(machine_options)

    return {
        "shift_options": SHIFT_OPTIONS,
        "machine_options": machine_options,
        "station_emails": sorted({item["email"] for item in stations if item["email"]}),
    }


def sign_in(adp_number, user_email, shift_id, machine_no=None):
    employee = lookup_employee_by_adp(adp_number)
    if not employee:
        raise ValueError("No active Ennis employee with Time Entry access was found for that ADP number.")

    station = None
    email_key = str(user_email or "").strip().lower()
    machine_key = str(machine_no or "").strip()
    try:
        station_items = list(_read_list("stations"))
    except Exception:
        station_items = []

    for item in station_items:
        candidate = _normalize_station(item)
        if machine_key and str(candidate["machine_no"] or "").strip() == machine_key:
            station = candidate
            break
        if not station and candidate["email"] == email_key:
            station = candidate

    if not station:
        local_machine_map = {str(item["machine_no"]): item for item in get_machine_options()}
        if machine_key and machine_key in local_machine_map:
            local_match = local_machine_map[machine_key]
            station = {
                "item_id": None,
                "machine_no": machine_key,
                "email": str(local_match.get("email") or "").strip().lower(),
                "location": "",
            }
        elif email_key:
            for machine in get_machine_options():
                if str(machine.get("email") or "").strip().lower() == email_key:
                    station = {
                        "item_id": None,
                        "machine_no": str(machine.get("machine_no") or "").strip(),
                        "email": email_key,
                        "location": "",
                    }
                    break

    if not station:
        raise ValueError(f"No station was found for Machine {machine_key or 'selection'}.")

    email_key = str(station["email"] or "").strip().lower()
    ensure_db()
    save_session(
        adp_number=adp_number,
        employee_name=employee["full_name"],
        machine_no=str(station["machine_no"] or "").strip(),
        shift_id=int(_as_number(shift_id, 40)),
        user_email=email_key,
    )
    return {
        "employee": employee,
        "roles": employee.get("roles", []),
        "user_email": email_key,
        "shift_id": int(_as_number(shift_id, 40)),
        "shift_title": _shift_label(shift_id),
        "machine_no": str(station["machine_no"] or "").strip(),
        "station": station,
    }


def get_dashboard_context(emp_id, user_email):
    details = _detail_types()
    work_orders = [
        item
        for item in (_normalize_work_order(entry) for entry in _read_list("work_orders", fetch_all=True))
        if item["order_type"] in {"EN", "RD"} and item["status"] in {"In Process", "Released", "Planned"}
    ]
    startstop_entries = [
        item
        for item in (_normalize_startstop(entry) for entry in _read_list("startstop", fetch_all=True))
        if item["emp_id"] == str(emp_id)
    ]
    startstop_entries.extend(_local_startstop_entries(emp_id))
    final_entries = [
        item
        for item in (_normalize_timeentry(entry) for entry in _read_list("timeentry", fetch_all=True))
        if item["emp_id"] == str(emp_id)
    ]
    final_entries.extend(_local_timeentry_entries(emp_id))
    tech_categories = [
        item for item in (_normalize_tech_category(entry) for entry in _read_list("tech_categories")) if item["id"] in {1, 2, 9}
    ]
    maintenance_locations = [_normalize_location(entry) for entry in _read_list("maintenance_locations")]
    maintenance_assets = [_normalize_asset(entry) for entry in _read_list("maintenance_assets")]
    station = None
    for item in _read_list("stations"):
        candidate = _normalize_station(item)
        if candidate["email"] == str(user_email or "").strip().lower():
            station = candidate
            break

    active_entry = next(
        (
            item
            for item in sorted(startstop_entries, key=lambda row: row["id"], reverse=True)
            if item["status"] in {"In Progress", "Paused"}
        ),
        None,
    )
    visible_startstop_entries = [
        item
        for item in startstop_entries
        if item["status"] in {"In Progress", "Paused"}
    ]
    recent_entries = sorted(
        final_entries + visible_startstop_entries,
        key=lambda row: (str(row.get("labor_date") or ""), int(row.get("id") or 0)),
        reverse=True,
    )[:40]

    grouped_workorders = {}
    for item in work_orders:
        grouped_workorders.setdefault(item["production_number"], []).append(item)

    return {
        "machine_no": station["machine_no"] if station else "",
        "active_entry": active_entry,
        "active_break": get_active_break_event(active_entry["sp_id"]) if active_entry else None,
        "recent_entries": recent_entries,
        "details_step_one": [item for item in details if item["option_step"] == 1],
        "details_step_two": [item for item in details if item["option_step"] == 2],
        "downtime_reasons": DOWNTIME_REASONS,
        "work_orders": sorted(grouped_workorders.keys()),
        "operations": work_orders,
        "tech_categories": tech_categories,
        "maintenance_locations": maintenance_locations,
        "maintenance_assets": maintenance_assets,
    }


def get_time_export_rows(start_date=None, end_date=None):
    start_text = str(start_date or "").strip()
    end_text = str(end_date or "").strip()
    rows = [_normalize_timeentry(entry) for entry in _read_list("timeentry", fetch_all=True)]
    rows.extend(_local_timeentry_entries())
    filtered = []
    for row in rows:
        labor_date = str(row.get("labor_date") or "")[:10]
        if start_text and labor_date < start_text:
            continue
        if end_text and labor_date > end_text:
            continue
        filtered.append({
            "labor_date": labor_date,
            "status": row.get("status", ""),
            "production_number": row.get("production_number", ""),
            "operation_id": row.get("operation_id", ""),
            "operation_description": row.get("operation_description", ""),
            "detail": row.get("details_type", ""),
            "dt_reason": row.get("details_type_ii", ""),
            "comments": row.get("tran_description", ""),
            "employee_id": row.get("employee_id") or row.get("emp_id", ""),
            "operator": row.get("operators_name", ""),
            "machine_no": row.get("machine_no", ""),
            "shift": row.get("shift", ""),
            "start": row.get("start", ""),
            "end": row.get("end", ""),
            "break_minutes": row.get("break_minutes", 0),
            "total": row.get("total", ""),
            "total_minutes": row.get("total_minutes", 0),
            "quantity": row.get("quantity", 0),
            "average": row.get("average", 0),
        })
    return sorted(
        filtered,
        key=lambda item: (str(item.get("labor_date") or ""), str(item.get("machine_no") or ""), str(item.get("start") or "")),
        reverse=True,
    )


def get_admin_dashboard_context():
    details = _detail_types()
    work_orders = [
        item
        for item in (_normalize_work_order(entry) for entry in _read_list("work_orders", fetch_all=True))
        if item["order_type"] in {"EN", "RD"} and item["status"] in {"In Process", "Released", "Planned"}
    ]
    grouped_workorders = {}
    for item in work_orders:
        grouped_workorders.setdefault(item["production_number"], []).append(item)
    pending = [_approval_context_entry(record) for record in list_approval_records(status="pending", limit=500)]
    reviewed = [
        _approval_context_entry(record)
        for record in list_approval_records(status=None, limit=50)
        if record.get("approval_status") != "pending"
    ]
    return {
        "pending_approvals": pending,
        "reviewed_approvals": reviewed[:25],
        "pending_count": len(pending),
        "details_step_two": [item for item in details if item["option_step"] == 2],
        "downtime_reasons": DOWNTIME_REASONS,
        "work_orders": sorted(grouped_workorders.keys()),
        "operations": work_orders,
        "shift_options": SHIFT_OPTIONS,
        "machine_options": get_machine_options(),
    }


def approve_time_entry(approval_id, reviewer=None, note=None):
    record = get_approval_record(int(_as_number(approval_id, 0)))
    if not record or record.get("approval_status") != "pending":
        raise ValueError("The selected approval record could not be found.")
    payload = _approval_payload(record)
    fields = payload.get("fields") or {}
    if not fields:
        raise ValueError("The selected approval record has no time entry fields.")

    approval_result = approve_approval_record(record["id"], reviewer=reviewer, note=note)
    if not approval_result:
        raise ValueError("The selected approval record was already reviewed.")
    queued_id = approval_result["queued_id"]

    try:
        _create_item("timeentry", fields)
        mark_record_synced(queued_id)
        return {"approved": True, "synced": True}
    except Exception:
        return {"approved": True, "queued": True, "offline_only": True}


def update_approval_time_entry(approval_id, fields_update=None):
    record = get_approval_record(int(_as_number(approval_id, 0)))
    if not record or record.get("approval_status") != "pending":
        raise ValueError("The selected approval record could not be found.")
    payload = _approval_payload(record)
    fields = dict(payload.get("fields") or {})
    if not fields:
        raise ValueError("The selected approval record has no time entry fields.")

    update = fields_update or {}
    production_number = str(update.get("production_number") or fields.get("ProductionNo") or fields.get("Title") or "").strip()
    operation_id = str(update.get("operation_id") or fields.get("OperationID") or "").strip()
    if production_number and operation_id:
        operation = _get_operation(
            production_number,
            operation_id,
            "The selected work order operation could not be found.",
        )
        fields.update(_operation_fields(operation, production_number, operation_id))

    detail_type = update.get("detail_type")
    downtime_reason = update.get("downtime_reason")
    detail_code = str(fields.get("DetailsType") or "").strip()
    reason_code = str(fields.get("DetailsTypeII") or "").strip()
    if detail_type is not None:
        detail_code, reason_code = _detail_code(detail_type, downtime_reason)
        fields["DetailsType"] = detail_code
        fields["DetailsTypeII"] = reason_code if detail_code == "DT" else ""
    elif downtime_reason is not None and str(downtime_reason or "").strip():
        detail_code = "DT"
        reason_code = _downtime_reason_code(downtime_reason)
        fields["DetailsType"] = detail_code
        fields["DetailsTypeII"] = reason_code

    direct_text_fields = {
        "labor_date": "LaborDate",
        "status": "Status",
        "operator_name": "OperatorsName",
        "shift": "Shift",
        "machine_no": "MachineNo",
        "start": "Start",
        "lunch_start": "LunchStart",
        "lunch_stop": "LunchStop",
        "end": "End",
    }
    for source_key, target_key in direct_text_fields.items():
        if source_key in update:
            fields[target_key] = str(update.get(source_key) or "").strip()

    if "emp_id" in update:
        emp_id = str(update.get("emp_id") or "").strip()
        fields["EmpID"] = int(_as_number(emp_id, 0)) if emp_id.isdigit() else emp_id
    if "employee_id" in update:
        fields["EmployeeID"] = str(update.get("employee_id") or "").strip()
    elif "emp_id" in update:
        fields["EmployeeID"] = str(update.get("emp_id") or "").strip()

    if "quantity" in update:
        fields["Quantity"] = _nonnegative_number(update.get("quantity"), fields.get("Quantity", 0))
    if "break_minutes" in update:
        fields["BreakMinutes"] = _nonnegative_number(update.get("break_minutes"), fields.get("BreakMinutes", 0))

    if "total_hours" in update or "total_minutes_remainder" in update:
        hours = _nonnegative_number(update.get("total_hours"), 0)
        minutes = _nonnegative_number(update.get("total_minutes_remainder"), 0)
        if minutes >= 60:
            raise ValueError("Total minutes must be less than 60.")
        total_minutes = int(hours * 60 + minutes)
        fields["TotalMinutes"] = total_minutes
        fields["Total"] = _hhmm_from_minutes(total_minutes)

    qty = _as_number(fields.get("Quantity"), 0)
    total_minutes = _as_number(fields.get("TotalMinutes"), 0)
    fields["Average"] = _recalculate_average(qty, total_minutes)

    if "comments" in update:
        fields["TranDescription"] = _time_entry_comment(
            str(fields.get("DetailsType") or "").strip(),
            str(fields.get("DetailsTypeII") or "").strip(),
            update.get("comments"),
            fields.get("OperationDescription") or fields.get("Description") or "",
        )

    payload["fields"] = fields
    _refresh_approval_acumatica_payload(payload, fields)
    if not patch_approval_record_payload(record["id"], payload):
        raise ValueError("The selected approval record was already reviewed.")
    return {"updated": True, "pending_approval": True}


def reject_time_entry(approval_id, reviewer=None, note=None):
    record = get_approval_record(int(_as_number(approval_id, 0)))
    if not record or record.get("approval_status") != "pending":
        raise ValueError("The selected approval record could not be found.")
    if not mark_approval_reviewed(record["id"], "rejected", reviewer=reviewer, note=note):
        raise ValueError("The selected approval record was already reviewed.")
    return {"rejected": True}


def start_time_entry(employee, shift_id, machine_no, production_number, operation_id, detail_type):
    context = get_dashboard_context(employee["emp_id"], "")
    if context.get("active_entry"):
        raise ValueError("An active time entry is already running. Stop or edit the active entry before starting another.")

    operation = next(
        (
            item
            for item in context["operations"]
            if item["production_number"] == production_number and item["operation_id"] == operation_id
        ),
        None,
    )
    if not operation:
        raise ValueError("The selected work order operation could not be found.")
    detail_code, _ = _detail_code(detail_type)

    fields = {
        "Title": production_number,
        "Submitted": False,
        "LaborType": "Direct",
        "LaborDate": datetime.now().date().isoformat(),
        "Status": "In Progress",
        "MachineNo": machine_no or "",
        "OrderType": operation["order_type"] or "EN",
        "ProductionNo": production_number,
        "InventoryID": operation["inventory_id"],
        "Description": operation["description"],
        "OperationDescription": operation["operation_description"],
        "EmpID": int(_as_number(employee["emp_id"], 0)),
        "EmployeeID": str(employee["emp_id"]).zfill(6),
        "OperatorsName": employee["full_name"],
        "Shift": str(shift_id),
        "OperationID": operation_id,
        "DetailsType": detail_code,
        "Quantity": 0,
        "Start": _iso_now(),
        "BreakMinutes": 0,
        "TotalMinutes": 0,
        "Total": "00:00",
        "Average": 0,
        "Year": datetime.now().year,
    }
    ensure_db()
    queued_id = queue_record(
        "startstop",
        {"employee": employee, "shift_id": shift_id, "machine_no": machine_no, "production_number": production_number, "operation_id": operation_id, "detail_type": detail_code, "fields": fields},
        machine_no=machine_no,
        emp_id=employee.get("emp_id"),
        employee_name=employee.get("full_name"),
        source="offline-first",
    )
    try:
        item = _create_item("startstop", fields)
        mark_record_synced(queued_id)
        return _normalize_startstop(item)
    except Exception:
        return {"offline_only": True, "queued": True, "fields": fields}


def _get_startstop_by_id(entry_id):
    normalized = [_normalize_startstop(item) for item in _read_list("startstop", fetch_all=True)]
    normalized.extend(_local_startstop_entries())
    for item in normalized:
        if str(item["sp_id"]) == str(entry_id) or str(item["id"]) == str(entry_id):
            return item
    raise ValueError("The requested active time entry could not be found.")


def _get_timeentry_by_id(entry_id):
    normalized = [_normalize_timeentry(item) for item in _read_list("timeentry", fetch_all=True)]
    normalized.extend(_local_timeentry_entries())
    for item in normalized:
        if str(item["sp_id"]) == str(entry_id) or str(item["id"]) == str(entry_id):
            return item
    raise ValueError("The requested submitted time entry could not be found.")


def _nonnegative_number(value, default=None):
    if value is None or str(value).strip() == "":
        return default
    number = _as_number(value, default if default is not None else 0)
    if number < 0:
        raise ValueError("Correction values cannot be negative.")
    return number


def _worked_minutes_from_entry(entry, break_minutes):
    start_value = entry.get("start")
    end_value = entry.get("end")
    if not start_value or not end_value:
        return None
    start_dt = datetime.fromisoformat(str(start_value).replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(str(end_value).replace("Z", "+00:00"))
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=timezone.utc)
    elapsed = max(int((end_dt - start_dt).total_seconds() // 60), 0)
    return max(elapsed - int(break_minutes), 0)


def _recalculate_average(quantity, total_minutes):
    return round(quantity / (total_minutes / 60), 2) if total_minutes and total_minutes > 0 else 0


def edit_active_time_entry(
    entry_id,
    production_number=None,
    operation_id=None,
    detail_type=None,
    downtime_reason=None,
    quantity=None,
    break_minutes=None,
    comments=None,
):
    entry = _get_startstop_by_id(entry_id)
    fields = {}
    qty = _nonnegative_number(quantity, None)
    corrected_break_minutes = _nonnegative_number(break_minutes, None)
    selected_production = str(production_number or "").strip()
    selected_operation = str(operation_id or "").strip()
    selected_detail = str(detail_type or "").strip()
    downtime_reason_was_sent = downtime_reason is not None

    if selected_production and selected_operation:
        if selected_production != entry.get("production_number") or selected_operation != entry.get("operation_id"):
            operation = _get_operation(
                selected_production,
                selected_operation,
                "The selected work order operation could not be found.",
            )
            fields.update(_operation_fields(operation, selected_production, selected_operation))
    if selected_detail and (selected_detail != entry.get("details_type") or str(downtime_reason or "").strip()):
        detail_code, reason_code = _detail_code(selected_detail, downtime_reason)
        fields["DetailsType"] = detail_code
        if detail_code == "DT":
            fields["TranDescription"] = _time_entry_comment(detail_code, reason_code, comments)
    if qty is not None:
        fields["Quantity"] = qty
    if corrected_break_minutes is not None:
        fields["BreakMinutes"] = corrected_break_minutes
    note = str(comments or "").strip()
    if note and "TranDescription" not in fields:
        fields["TranDescription"] = note
    if not fields:
        raise ValueError("Enter a correction to save.")

    item_id = entry["sp_id"]
    if str(item_id).startswith("approval:"):
        approval_id = int(_as_number(_approval_record_key(item_id), 0))
        if not patch_approval_record_fields(approval_id, fields):
            raise ValueError("The pending approval record could not be updated.")
        return {"corrected": True, "pending_approval": True}

    if str(item_id).startswith("local:"):
        local_id = int(_as_number(_local_record_key(item_id), 0))
        patch_pending_record_fields(local_id, fields)
        return {"corrected": True, "offline_only": True, "updated_pending_record": True}

    queued_id = queue_record(
        "active_time_correction",
        {"entry_id": item_id, "item_id": item_id, "fields": fields},
        machine_no=entry.get("machine_no"),
        emp_id=entry.get("emp_id"),
        employee_name=entry.get("operators_name"),
        source="offline-first",
    )
    try:
        _update_item("startstop", item_id, fields)
        mark_record_synced(queued_id)
        return {"corrected": True}
    except Exception:
        try:
            _patch_synced_item_fields("startstop", item_id, fields)
        except Exception:
            pass
        return {"corrected": False, "queued": True, "offline_only": True}


def correct_time_entry(
    entry_id,
    production_number=None,
    operation_id=None,
    detail_type=None,
    downtime_reason=None,
    quantity=None,
    break_minutes=None,
    total_hours=None,
    total_minutes_remainder=None,
    comments=None,
):
    entry = _get_timeentry_by_id(entry_id)
    fields = {}
    qty = _nonnegative_number(quantity, entry.get("quantity"))
    corrected_break_minutes = _nonnegative_number(break_minutes, entry.get("break_minutes"))
    selected_production = str(production_number or "").strip()
    selected_operation = str(operation_id or "").strip()
    selected_detail = str(detail_type or "").strip()
    downtime_reason_was_sent = downtime_reason is not None

    if selected_production and selected_operation:
        if selected_production != entry.get("production_number") or selected_operation != entry.get("operation_id"):
            operation = _get_operation(
                selected_production,
                selected_operation,
                "The selected work order operation could not be found.",
            )
            fields.update(_operation_fields(operation, selected_production, selected_operation))

    detail_code = entry.get("details_type") or ""
    reason_code = entry.get("details_type_ii") or ""
    if selected_detail:
        detail_code, reason_code = _detail_code(selected_detail, downtime_reason)
        fields["DetailsType"] = detail_code
        fields["DetailsTypeII"] = reason_code if detail_code == "DT" else ""
    elif str(downtime_reason or "").strip():
        detail_code = "DT"
        reason_code = _downtime_reason_code(downtime_reason)
        fields["DetailsType"] = detail_code
        fields["DetailsTypeII"] = reason_code

    if quantity is not None and str(quantity).strip() != "":
        fields["Quantity"] = qty
    if break_minutes is not None and str(break_minutes).strip() != "":
        fields["BreakMinutes"] = corrected_break_minutes
        total_minutes = _worked_minutes_from_entry(entry, corrected_break_minutes)
        if total_minutes is not None:
            fields["TotalMinutes"] = total_minutes
            fields["Total"] = _hhmm_from_minutes(total_minutes)
    else:
        total_minutes = entry.get("total_minutes")

    if total_hours is not None and str(total_hours).strip() != "":
        hours = _nonnegative_number(total_hours, 0)
        minutes = _nonnegative_number(total_minutes_remainder, 0)
        if minutes >= 60:
            raise ValueError("Total minutes must be less than 60.")
        total_minutes = int(hours * 60 + minutes)
        fields["TotalMinutes"] = total_minutes
        fields["Total"] = _hhmm_from_minutes(total_minutes)

    if "Quantity" in fields or "TotalMinutes" in fields:
        fields["Average"] = _recalculate_average(qty, _as_number(fields.get("TotalMinutes"), total_minutes))
    if selected_detail or downtime_reason_was_sent or comments is not None:
        comment_value = _time_entry_comment(
            detail_code,
            reason_code,
            comments,
            fields.get("OperationDescription") or entry.get("operation_description"),
        )
        fields["TranDescription"] = comment_value
    if not fields:
        raise ValueError("Enter a correction to save.")

    item_id = entry["sp_id"]
    if str(item_id).startswith("approval:"):
        approval_id = int(_as_number(_approval_record_key(item_id), 0))
        if not patch_approval_record_fields(approval_id, fields):
            raise ValueError("The pending approval record could not be updated.")
        return {"corrected": True, "pending_approval": True}

    if str(item_id).startswith("local:"):
        local_id = int(_as_number(_local_record_key(item_id), 0))
        patch_pending_record_fields(local_id, fields)
        return {"corrected": True, "offline_only": True, "updated_pending_record": True}

    queued_id = queue_record(
        "timeentry_correction",
        {"entry_id": item_id, "item_id": item_id, "fields": fields},
        machine_no=entry.get("machine_no"),
        emp_id=entry.get("emp_id"),
        employee_name=entry.get("operators_name"),
        source="offline-first",
    )
    try:
        _patch_synced_item_fields("timeentry", item_id, fields)
    except Exception:
        pass
    try:
        _update_item("timeentry", item_id, fields)
        mark_record_synced(queued_id)
        return {"corrected": True}
    except Exception:
        return {"corrected": False, "queued": True, "offline_only": True}


def delete_time_entry(entry_id, entry_list="timeentry"):
    list_key = str(entry_list or "timeentry").strip()
    if list_key not in {"timeentry", "startstop"}:
        raise ValueError("The selected entry type cannot be deleted.")

    item_id = str(entry_id or "").strip()
    if not item_id:
        raise ValueError("The requested time entry could not be found.")

    if item_id.startswith("local:"):
        local_id = int(_as_number(_local_record_key(item_id), 0))
        discard_pending_record(local_id)
        return {"deleted": True, "offline_only": True, "removed_pending_record": True}

    if item_id.startswith("approval:"):
        approval_id = int(_as_number(_approval_record_key(item_id), 0))
        if not mark_approval_reviewed(approval_id, "rejected", note="Deleted by operator before approval."):
            raise ValueError("The pending approval record could not be deleted.")
        return {"deleted": True, "pending_approval": True}

    entry = _get_timeentry_by_id(item_id) if list_key == "timeentry" else _get_startstop_by_id(item_id)
    queued_id = queue_record(
        f"{list_key}_delete",
        {"entry_id": item_id, "item_id": item_id},
        machine_no=entry.get("machine_no"),
        emp_id=entry.get("emp_id"),
        employee_name=entry.get("operators_name"),
        source="offline-first",
    )
    try:
        _delete_synced_item(list_key, item_id)
    except Exception:
        pass
    try:
        _delete_item(list_key, item_id)
        mark_record_synced(queued_id)
        return {"deleted": True}
    except Exception:
        return {"deleted": False, "queued": True, "offline_only": True}


def pause_for_lunch(entry_id, break_type=None, comments=None):
    entry = _get_startstop_by_id(entry_id)
    if entry.get("status") == "Paused":
        raise ValueError("This entry is already paused.")
    selected_break_type = str(break_type or "Lunch").strip()
    if selected_break_type not in {"Lunch", "With Relief", "No Relief"}:
        raise ValueError("Select a valid break type.")
    break_start = _iso_now()
    fields = {"LunchStart": break_start, "LunchStop": None, "Status": "Paused"}
    item_id = entry["sp_id"]
    start_break_event(
        item_id,
        selected_break_type,
        comments,
        machine_no=entry.get("machine_no"),
        emp_id=entry.get("emp_id"),
        employee_name=entry.get("operators_name"),
        started_at=break_start,
    )
    queued_id = queue_record(
        "pause_lunch",
        {
            "entry_id": item_id,
            "item_id": item_id,
            "fields": fields,
            "break_type": selected_break_type,
            "break_comment": str(comments or "").strip(),
        },
        machine_no=entry.get("machine_no"),
        emp_id=entry.get("emp_id"),
        employee_name=entry.get("operators_name"),
        source="offline-first",
    )
    try:
        _update_item("startstop", item_id, fields)
        mark_record_synced(queued_id)
        return {"paused": True}
    except Exception:
        return {"paused": False, "queued": True, "offline_only": True}


def resume_from_lunch(entry_id):
    entry = _get_startstop_by_id(entry_id)
    lunch_start = entry["lunch_start"]
    if not lunch_start:
        raise ValueError("Lunch start time is missing for this entry.")
    lunch_start_dt = datetime.fromisoformat(str(lunch_start).replace("Z", "+00:00"))
    now_dt = _utc_now()
    current_break_minutes = max(int((now_dt - lunch_start_dt).total_seconds() // 60), 0)
    item_id = entry["sp_id"]
    active_break = get_active_break_event(item_id)
    deducted_minutes = current_break_minutes if _break_counts_against_production(active_break) else 0
    break_minutes = entry["break_minutes"] + deducted_minutes
    fields = {"LunchStop": now_dt.isoformat(), "Status": "In Progress", "BreakMinutes": break_minutes}
    finish_break_event(item_id, ended_at=now_dt.isoformat(), duration_minutes=current_break_minutes)
    queued_id = queue_record(
        "resume_lunch",
        {"entry_id": item_id, "item_id": item_id, "fields": fields},
        machine_no=entry.get("machine_no"),
        emp_id=entry.get("emp_id"),
        employee_name=entry.get("operators_name"),
        source="offline-first",
    )
    try:
        _update_item("startstop", item_id, fields)
        mark_record_synced(queued_id)
        return {"resumed": True}
    except Exception:
        return {"resumed": False, "queued": True, "offline_only": True}


def stop_time_entry(entry_id, quantity=None):
    entry = _get_startstop_by_id(entry_id)
    start_dt = datetime.fromisoformat(str(entry["start"]).replace("Z", "+00:00"))
    end_dt = _utc_now()
    break_minutes = entry["break_minutes"]
    if entry["lunch_start"] and not entry["lunch_stop"]:
        lunch_start_dt = datetime.fromisoformat(str(entry["lunch_start"]).replace("Z", "+00:00"))
        active_break = get_active_break_event(entry["sp_id"])
        current_break_minutes = max(int((end_dt - lunch_start_dt).total_seconds() // 60), 0)
        if _break_counts_against_production(active_break):
            break_minutes += current_break_minutes
        finish_break_event(entry["sp_id"], ended_at=end_dt.isoformat(), duration_minutes=current_break_minutes)
    worked_minutes = max(int((end_dt - start_dt).total_seconds() // 60) - int(break_minutes), 0)
    qty = _as_number(quantity, 1)
    average = round(qty / (worked_minutes / 60), 2) if worked_minutes > 0 else 0

    updated_fields = {
        "End": end_dt.isoformat(),
        "Status": "Submitted",
        "Submitted": True,
        "BreakMinutes": break_minutes,
        "TotalMinutes": worked_minutes,
        "Total": _hhmm_from_minutes(worked_minutes),
        "Quantity": qty,
        "Average": average,
    }
    item_id = entry["sp_id"]
    complete_queued_id = queue_record(
        "complete_startstop",
        {"entry_id": item_id, "item_id": item_id, "fields": updated_fields},
        machine_no=entry.get("machine_no"),
        emp_id=entry.get("emp_id"),
        employee_name=entry.get("operators_name"),
        source="offline-first",
    )
    try:
        _update_item("startstop", item_id, updated_fields)
        mark_record_synced(complete_queued_id)
    except Exception:
        pass

    final_fields = {
        "Title": entry["production_number"],
        "LaborDate": entry["labor_date"],
        "Status": "Submitted",
        "OrderType": "EN",
        "ProductionNo": entry["production_number"],
        "OperatorsName": entry["operators_name"],
        "Shift": entry["shift"],
        "MachineNo": entry["machine_no"],
        "DetailsType": entry["details_type"],
        "EmpID": int(_as_number(entry["emp_id"], 0)),
        "EmployeeID": entry["employee_id"],
        "OperationID": entry["operation_id"],
        "Quantity": qty,
        "BreakMinutes": break_minutes,
        "TotalMinutes": worked_minutes,
        "Total": _hhmm_from_minutes(worked_minutes),
        "Average": average,
        "InventoryID": entry["inventory_id"],
        "Description": entry["description"],
        "OperationDescription": entry["operation_description"],
        "LaborType": "Direct",
        "Start": entry["start"],
        "LunchStart": entry["lunch_start"],
        "LunchStop": entry["lunch_stop"],
        "End": end_dt.isoformat(),
        "TranDescription": entry.get("tran_description") or entry["operation_description"] or entry["description"],
        "Year": datetime.now().year,
        "StartStopID": int(_as_number(entry["id"], 0)),
    }
    employee_for_acumatica = lookup_employee_by_adp(entry.get("emp_id")) or {}
    approval_payload = {"entry_id": entry_id, "fields": final_fields, "employee": employee_for_acumatica}
    _attach_acumatica_payload(approval_payload, employee_for_acumatica, final_fields)
    ensure_db()
    approval_id = queue_approval(
        "stop_time_entry",
        approval_payload,
        machine_no=entry.get("machine_no"),
        emp_id=entry.get("emp_id"),
        employee_name=entry.get("operators_name"),
    )
    return {"submitted": False, "pending_approval": True, "approval_id": approval_id, "worked_minutes": worked_minutes}


def submit_misc_time(
    employee,
    shift_id,
    machine_no,
    detail_type_ii,
    production_number,
    operation_id,
    hours,
    minutes,
    comments,
):
    operation = _get_operation_for_employee(
        employee,
        production_number,
        operation_id,
        "The selected downtime operation could not be found.",
    )
    total_minutes = int(_as_number(hours, 0) * 60 + _as_number(minutes, 0))
    if total_minutes <= 0:
        raise ValueError("Misc time must be greater than zero.")
    total = _hhmm_from_minutes(total_minutes)
    downtime_reason = _downtime_reason_code(detail_type_ii)
    comments_text = str(comments or "").strip()
    tran_description = downtime_reason if not comments_text else f"{downtime_reason} - {comments_text}"
    fields = {
        "Title": employee["full_name"],
        "LaborDate": datetime.now().date().isoformat(),
        "Status": "Submitted",
        "LaborType": "Direct",
        "OrderType": operation["order_type"] or "EN",
        "ProductionNo": production_number,
        "InventoryID": operation["inventory_id"],
        "Description": operation["description"],
        "OperationDescription": operation["operation_description"],
        "OperationID": operation_id,
        "Shift": str(shift_id),
        "EmpID": int(_as_number(employee["emp_id"], 0)),
        "EmployeeID": str(employee["emp_id"]).zfill(6),
        "OperatorsName": employee["full_name"],
        "MachineNo": machine_no or "",
        "DetailsType": "DT",
        "DetailsTypeII": downtime_reason,
        "Quantity": 1,
        "Total": total,
        "TotalMinutes": total_minutes,
        "TranDescription": tran_description,
        "Year": datetime.now().year,
    }
    approval_payload = {"employee": employee, "fields": fields}
    _attach_acumatica_payload(approval_payload, employee, fields, downtime_reason)
    ensure_db()
    approval_id = queue_approval(
        "misc_time",
        approval_payload,
        machine_no=machine_no,
        emp_id=employee.get("emp_id"),
        employee_name=employee.get("full_name"),
    )
    return {"submitted": False, "pending_approval": True, "approval_id": approval_id}


def submit_manual_time(
    employee,
    shift_id,
    machine_no,
    production_number,
    operation_id,
    detail_type,
    quantity,
    hours,
    minutes,
    comments,
):
    operation = _get_operation_for_employee(
        employee,
        production_number,
        operation_id,
        "The selected manual-time operation could not be found.",
    )
    total_minutes = int(_as_number(hours, 0) * 60 + _as_number(minutes, 0))
    qty = _as_number(quantity, 0)
    if total_minutes <= 0:
        raise ValueError("Manual time must be greater than zero.")
    detail_code, _ = _detail_code(detail_type)
    average = round(qty / (total_minutes / 60), 2) if total_minutes > 0 else 0
    fields = {
        "Title": production_number,
        "Status": "Submitted",
        "LaborDate": datetime.now().date().isoformat(),
        "LaborType": "Direct",
        "OrderType": operation["order_type"] or "EN",
        "OperatorsName": employee["full_name"],
        "EmpID": int(_as_number(employee["emp_id"], 0)),
        "EmployeeID": str(employee["emp_id"]).zfill(6),
        "Shift": str(shift_id),
        "ProductionNo": production_number,
        "InventoryID": operation["inventory_id"],
        "Description": operation["description"],
        "OperationDescription": operation["operation_description"],
        "DetailsType": detail_code,
        "OperationID": operation_id,
        "MachineNo": machine_no or "",
        "Total": _hhmm_from_minutes(total_minutes),
        "TotalMinutes": total_minutes,
        "Quantity": qty,
        "Average": average,
        "TranDescription": comments or operation["operation_description"] or operation["description"],
        "Year": datetime.now().year,
    }
    approval_payload = {"employee": employee, "fields": fields}
    _attach_acumatica_payload(approval_payload, employee, fields)
    ensure_db()
    approval_id = queue_approval(
        "manual_time",
        approval_payload,
        machine_no=machine_no,
        emp_id=employee.get("emp_id"),
        employee_name=employee.get("full_name"),
    )
    return {"submitted": False, "pending_approval": True, "approval_id": approval_id}


def submit_daily_checklist(employee, shift_id, machine_no, initials, notes, checks):
    fields = {
        "Title": employee["full_name"],
        "CNCMachine": machine_no or "",
        "Date": _iso_now(),
        "Shift": _shift_label(shift_id),
        "Mon": bool(checks.get("lub_unit")),
        "Tues": bool(checks.get("oil_air_lub")),
        "Wed": bool(checks.get("machine_chamber")),
        "Thurs": bool(checks.get("chuck")),
        "Fri": bool(checks.get("obs_window")),
        "PneumaticDevice_x002d_look_x002f": bool(checks.get("pneu_device")),
        "ChipConveyors_x002d_clean": bool(checks.get("chip_conveyor")),
        "CoolantUnit_x002d_look_x002f_che": bool(checks.get("coolant_unit")),
        "OilChiller_x002d_look_x002f_clea": bool(checks.get("oil_chiller")),
        "HydraulicUnit_x002d_look_x002f_c": bool(checks.get("hydraulic_unit")),
        "OilSkimmer": bool(checks.get("oil_skimmer")),
        "Notes": notes or "",
        "OperatorInitials": initials or "",
        "CompletedBy": employee["full_name"],
        "MachineNumber": machine_no or "",
    }
    ensure_db()
    queued_id = queue_record(
        "daily_checklist",
        {"employee": employee, "fields": fields},
        machine_no=machine_no,
        emp_id=employee.get("emp_id"),
        employee_name=employee.get("full_name"),
        source="offline-first",
    )
    try:
        _create_item("checklist", fields)
        mark_record_synced(queued_id)
        return {"submitted": True}
    except Exception:
        return {"submitted": False, "queued": True, "offline_only": True}


def submit_maintenance_request(employee, requester_id, requester_name, title, description, priority, location_id, asset_id, user_email):
    locations = [_normalize_location(item) for item in _read_list("maintenance_locations")]
    assets = [_normalize_asset(item) for item in _read_list("maintenance_assets")]
    location = next((item for item in locations if str(item["id"]) == str(location_id)), None)
    asset = next((item for item in assets if str(item["asset_id"]) == str(asset_id) or str(item["id"]) == str(asset_id)), None)

    fields = {
        "Title": title,
        "Description": description,
        "Priority": priority,
        "Status": "Open",
        "Requester_x0020_ID": str(requester_id or employee["emp_id"]),
        "Requester": requester_name or employee["full_name"],
        "TabletEmail": user_email or "",
        "Location": location["title"] if location else "",
        "Asset": asset["title"] if asset else "",
    }
    if location:
        fields["Location_x0020_ID"] = location["id"]
    if asset:
        fields["Asset_x0020_ID"] = asset["asset_id"] or asset["id"]

    ensure_db()
    queued_id = queue_record(
        "maintenance_request",
        {"employee": employee, "fields": fields},
        machine_no=None,
        emp_id=employee.get("emp_id"),
        employee_name=employee.get("full_name"),
        source="offline-first",
    )
    try:
        _create_item("maintenance_requests", fields)
        mark_record_synced(queued_id)
        return {"submitted": True}
    except Exception:
        return {"submitted": False, "queued": True, "offline_only": True}


def send_it_request(user_email, user_name, machine_no, category, issue):
    payload = {
        "user_email": user_email,
        "user_name": user_name,
        "machine_no": machine_no,
        "category": category,
        "issue": issue,
    }
    webhook_url = os.getenv("CNC_TIME_IT_WEBHOOK_URL", "").strip()
    if webhook_url:
        response = requests.post(webhook_url, json=payload, timeout=20)
        if response.status_code not in {200, 201, 202}:
            raise ValueError("The IT webhook rejected the request.")
        return {"delivered": True}
    _queue_notification("it_support", payload)
    return {"delivered": False, "queued": True}


def send_supervisor_message(user_name, machine_no, comment):
    payload = {
        "user_name": user_name,
        "machine_no": machine_no,
        "comment": comment,
    }
    webhook_url = os.getenv("CNC_TIME_SUPERVISOR_WEBHOOK_URL", "").strip()
    if webhook_url:
        response = requests.post(webhook_url, json=payload, timeout=20)
        if response.status_code not in {200, 201, 202}:
            raise ValueError("The supervisor webhook rejected the request.")
        return {"delivered": True}
    _queue_notification("supervisor_message", payload)
    return {"delivered": False, "queued": True}
