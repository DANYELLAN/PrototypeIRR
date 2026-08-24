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
    ensure_db,
    get_machine_options,
    list_pending_records,
    mark_record_synced,
    queue_record,
    save_session,
    upsert_machine_options,
)
from postgres_sync import get_db_connection, initialize_database, sync_list_to_postgres
from sharepoint_api import sync_cnc_time_lists_to_postgres
from sharepoint_client import (
    GRAPH_BASE_URL,
    SharePointApiError,
    build_headers,
    get_access_token,
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
        "id": "a5849673-cc3e-47a3-8e1d-aec90d2374cc",
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

OUTBOX_FILE = Path(__file__).resolve().parents[1] / "data" / "cnc_time_outbox.jsonl"
SITE_ID_CACHE = {}
LIST_CACHE = {}
CACHE_TTL_SECONDS = 30
POSTGRES_READ_ENABLED = os.getenv("CNC_TIME_POSTGRES_READ_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}

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
}


def _utc_now():
    return datetime.now(timezone.utc)


def _iso_now():
    return _utc_now().isoformat()


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
            cursor.execute(
                """
                SELECT si.sharepoint_item_id, si.etag, si.web_url, si.fields_json, si.raw_item_json
                FROM sharepoint_items si
                JOIN sharepoint_lists sl ON sl.id = si.list_id
                WHERE sl.list_name = %s
                ORDER BY si.id
                """,
                (config["name"],),
            )
            rows = cursor.fetchall()
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


def _cache_fetched_list(list_key, items):
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
            _site_id(config["site"], build_headers(get_access_token())),
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
                  AND si.sharepoint_item_id = %s
                """,
                (json.dumps(fields), json.dumps(fields), config["name"], str(item_id)),
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

    token = get_access_token()
    headers = build_headers(token)
    config = LISTS[list_key]
    items = get_list_items(
        config["site"],
        config["id"],
        headers=headers,
        top=top,
        site_id=_site_id(config["site"], headers),
        fetch_all=fetch_all,
    )
    try:
        _cache_fetched_list(list_key, items)
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
            if not fields:
                skipped += 1
                continue

            if target["mode"] == "update":
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
    token = get_access_token()
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
        _cache_fetched_list(list_key, [item])
    except Exception:
        pass
    return item


def _update_item(list_key, item_id, fields):
    token = get_access_token()
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


def _value(fields, *keys):
    for key in keys:
        if key in fields and fields[key] not in (None, ""):
            return fields[key]
    return None


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
    return {
        "item_id": item.get("id"),
        "emp_id": str(_value(fields, "ADPEmpNumber", "field_0", "EmployeeID", "EmpID") or "").strip(),
        "full_name": str(full_name or "").strip(),
        "first_name": str(_value(fields, "First_x0020_Name", "field_2") or "").strip(),
        "last_name": str(_value(fields, "Last_x0020_Name", "field_4") or "").strip(),
        "branch": str(_value(fields, "Branches", "field_22", "Branch") or "").strip(),
        "status": str(_value(fields, "Status", "field_19") or "").strip(),
        "machinist": _as_bool(_value(fields, "Machinist", "machinist", "IsMachinist")),
    }


def lookup_employee_by_adp(adp_number, employee_items=None):
    items = employee_items if employee_items is not None else _read_list("employees")
    for item in items:
        candidate = _normalize_employee(item)
        if (
            str(candidate["emp_id"]).strip() == str(adp_number).strip()
            and candidate["status"].lower() == "active"
            and candidate["branch"].lower() == "ennis"
            and candidate["machinist"]
        ):
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
                "label": f"Machine {machine_no}",
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
            "label": f"Machine {machine_no}",
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
        raise ValueError("No active Ennis machinist was found for that ADP number.")

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
        "user_email": email_key,
        "shift_id": int(_as_number(shift_id, 40)),
        "shift_title": _shift_label(shift_id),
        "machine_no": str(station["machine_no"] or "").strip(),
        "station": station,
    }


def get_dashboard_context(emp_id, user_email):
    details = [
        item
        for item in (_normalize_detail_type(entry) for entry in _read_list("detail_types"))
        if item["branch"].lower() == "ennis"
    ]
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
    final_entries = [
        item
        for item in (_normalize_timeentry(entry) for entry in _read_list("timeentry", fetch_all=True))
        if item["emp_id"] == str(emp_id)
    ]
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
    recent_entries = sorted(
        final_entries + startstop_entries,
        key=lambda row: (str(row.get("labor_date") or ""), int(row.get("id") or 0)),
        reverse=True,
    )[:40]

    grouped_workorders = {}
    for item in work_orders:
        grouped_workorders.setdefault(item["production_number"], []).append(item)

    return {
        "machine_no": station["machine_no"] if station else "",
        "active_entry": active_entry,
        "recent_entries": recent_entries,
        "details_step_one": [item for item in details if item["option_step"] == 1],
        "details_step_two": [item for item in details if item["option_step"] == 2],
        "work_orders": sorted(grouped_workorders.keys()),
        "operations": work_orders,
        "tech_categories": tech_categories,
        "maintenance_locations": maintenance_locations,
        "maintenance_assets": maintenance_assets,
    }


def start_time_entry(employee, shift_id, machine_no, production_number, operation_id, detail_type):
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
        raise ValueError("The selected work order operation could not be found.")

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
        "DetailsType": detail_type,
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
        {"employee": employee, "shift_id": shift_id, "machine_no": machine_no, "production_number": production_number, "operation_id": operation_id, "detail_type": detail_type, "fields": fields},
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
    for item in normalized:
        if str(item["sp_id"]) == str(entry_id) or str(item["id"]) == str(entry_id):
            return item
    raise ValueError("The requested active time entry could not be found.")


def pause_for_lunch(entry_id):
    entry = _get_startstop_by_id(entry_id)
    fields = {"LunchStart": _iso_now(), "Status": "Paused"}
    queued_id = queue_record(
        "pause_lunch",
        {"entry_id": entry_id, "item_id": entry["sp_id"], "fields": fields},
        machine_no=entry.get("machine_no"),
        emp_id=entry.get("emp_id"),
        employee_name=entry.get("operators_name"),
        source="offline-first",
    )
    try:
        _update_item("startstop", entry["sp_id"], fields)
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
    break_minutes = entry["break_minutes"] + max(int((now_dt - lunch_start_dt).total_seconds() // 60), 0)
    fields = {"LunchStop": now_dt.isoformat(), "Status": "In Progress", "BreakMinutes": break_minutes}
    queued_id = queue_record(
        "resume_lunch",
        {"entry_id": entry_id, "item_id": entry["sp_id"], "fields": fields},
        machine_no=entry.get("machine_no"),
        emp_id=entry.get("emp_id"),
        employee_name=entry.get("operators_name"),
        source="offline-first",
    )
    try:
        _update_item("startstop", entry["sp_id"], fields)
        mark_record_synced(queued_id)
        return {"resumed": True}
    except Exception:
        return {"resumed": False, "queued": True, "offline_only": True}


def stop_time_entry(entry_id):
    entry = _get_startstop_by_id(entry_id)
    start_dt = datetime.fromisoformat(str(entry["start"]).replace("Z", "+00:00"))
    end_dt = _utc_now()
    break_minutes = entry["break_minutes"]
    if entry["lunch_start"] and not entry["lunch_stop"]:
        lunch_start_dt = datetime.fromisoformat(str(entry["lunch_start"]).replace("Z", "+00:00"))
        break_minutes += max(int((end_dt - lunch_start_dt).total_seconds() // 60), 0)
    worked_minutes = max(int((end_dt - start_dt).total_seconds() // 60) - int(break_minutes), 0)
    quantity = 1
    average = round(quantity / (worked_minutes / 60), 2) if worked_minutes > 0 else 0

    updated_fields = {
        "End": end_dt.isoformat(),
        "Status": "Submitted",
        "Submitted": True,
        "BreakMinutes": break_minutes,
        "TotalMinutes": worked_minutes,
        "Total": _hhmm_from_minutes(worked_minutes),
        "Quantity": quantity,
        "Average": average,
    }
    complete_queued_id = queue_record(
        "complete_startstop",
        {"entry_id": entry_id, "item_id": entry["sp_id"], "fields": updated_fields},
        machine_no=entry.get("machine_no"),
        emp_id=entry.get("emp_id"),
        employee_name=entry.get("operators_name"),
        source="offline-first",
    )
    try:
        _update_item("startstop", entry["sp_id"], updated_fields)
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
        "Quantity": quantity,
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
        "TranDescription": entry["description"],
        "Year": datetime.now().year,
        "StartStopID": int(_as_number(entry["id"], 0)),
    }
    ensure_db()
    queued_id = queue_record(
        "stop_time_entry",
        {"entry_id": entry_id, "fields": final_fields},
        machine_no=entry.get("machine_no"),
        emp_id=entry.get("emp_id"),
        employee_name=entry.get("operators_name"),
        source="offline-first",
    )
    try:
        _create_item("timeentry", final_fields)
        mark_record_synced(queued_id)
        return {"submitted": True, "worked_minutes": worked_minutes}
    except Exception:
        return {"submitted": False, "queued": True, "offline_only": True, "worked_minutes": worked_minutes}


def submit_misc_time(employee, shift_id, machine_no, detail_type_ii, hours, minutes, comments):
    total_minutes = int(_as_number(hours, 0) * 60 + _as_number(minutes, 0))
    if total_minutes <= 0:
        raise ValueError("Misc time must be greater than zero.")
    total = _hhmm_from_minutes(total_minutes)
    description = f"{detail_type_ii}-{comments}".strip("-")
    fields = {
        "Title": employee["full_name"],
        "LaborDate": datetime.now().date().isoformat(),
        "Status": "Submitted",
        "LaborType": "Indirect",
        "Shift": str(shift_id),
        "EmpID": int(_as_number(employee["emp_id"], 0)),
        "EmployeeID": str(employee["emp_id"]).zfill(6),
        "OperatorsName": employee["full_name"],
        "MachineNo": machine_no or "",
        "DetailsType": "Downtime",
        "DetailsTypeII": detail_type_ii,
        "Total": total,
        "TotalMinutes": total_minutes,
        "TranDescription": description,
        "Year": datetime.now().year,
    }
    ensure_db()
    queued_id = queue_record(
        "misc_time",
        {"employee": employee, "fields": fields},
        machine_no=machine_no,
        emp_id=employee.get("emp_id"),
        employee_name=employee.get("full_name"),
        source="offline-first",
    )
    try:
        _create_item("timeentry", fields)
        mark_record_synced(queued_id)
        return {"submitted": True}
    except Exception:
        return {"submitted": False, "queued": True, "offline_only": True}


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
        raise ValueError("The selected manual-time operation could not be found.")
    total_minutes = int(_as_number(hours, 0) * 60 + _as_number(minutes, 0))
    qty = _as_number(quantity, 0)
    if total_minutes <= 0:
        raise ValueError("Manual time must be greater than zero.")
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
        "DetailsType": detail_type,
        "OperationID": operation_id,
        "MachineNo": machine_no or "",
        "Total": _hhmm_from_minutes(total_minutes),
        "TotalMinutes": total_minutes,
        "Quantity": qty,
        "Average": average,
        "TranDescription": comments or "",
        "Year": datetime.now().year,
    }
    ensure_db()
    queued_id = queue_record(
        "manual_time",
        {"employee": employee, "fields": fields},
        machine_no=machine_no,
        emp_id=employee.get("emp_id"),
        employee_name=employee.get("full_name"),
        source="offline-first",
    )
    try:
        _create_item("timeentry", fields)
        mark_record_synced(queued_id)
        return {"submitted": True}
    except Exception:
        return {"submitted": False, "queued": True, "offline_only": True}


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
