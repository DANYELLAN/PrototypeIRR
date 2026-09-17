import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = Path(__file__).resolve().parents[1]
for candidate in (str(PROJECT_ROOT), str(APP_ROOT)):
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from time_entry_app.bridge.cnc_time_backend import (  # noqa: E402
    approve_time_entry,
    correct_time_entry,
    delete_time_entry,
    edit_active_time_entry,
    get_admin_dashboard_context,
    get_dashboard_context,
    get_employee_lookup,
    get_sign_in_context,
    get_time_export_rows,
    pause_for_lunch,
    resume_from_lunch,
    reject_time_entry,
    send_it_request,
    send_supervisor_message,
    sign_in,
    start_time_entry,
    stop_time_entry,
    sync_background_jobs,
    sync_pending_writes_to_sharepoint,
    sync_reference_data_from_sharepoint,
    submit_daily_checklist,
    submit_maintenance_request,
    submit_manual_time,
    submit_misc_time,
    update_approval_time_entry,
)


def _payload():
    if len(sys.argv) < 2:
        return {}
    try:
        return json.loads(sys.argv[1])
    except json.JSONDecodeError:
        return {}


def _result(data):
    print(json.dumps({"ok": True, "data": data}, default=str))


def _error(message):
    print(json.dumps({"ok": False, "error": str(message)}))
    raise SystemExit(1)


def main():
    payload = _payload()
    action = payload.get("action")

    try:
        if action == "get_sign_in_context":
            return _result(get_sign_in_context())
        if action == "get_employee_lookup":
            return _result(get_employee_lookup(payload.get("adp_number")))
        if action == "sign_in":
            return _result(
                sign_in(
                    payload.get("adp_number"),
                    payload.get("user_email"),
                    payload.get("shift_id"),
                    payload.get("machine_no"),
                )
            )
        if action == "get_dashboard_context":
            return _result(get_dashboard_context(payload.get("emp_id"), payload.get("user_email")))
        if action == "get_admin_dashboard_context":
            return _result(get_admin_dashboard_context())
        if action == "get_time_export_rows":
            return _result(get_time_export_rows(payload.get("start_date"), payload.get("end_date")))
        if action == "sync_reference_data_from_sharepoint":
            return _result(sync_reference_data_from_sharepoint())
        if action == "sync_pending_writes_to_sharepoint":
            return _result(sync_pending_writes_to_sharepoint(payload.get("limit") or 50))
        if action == "sync_background_jobs":
            return _result(sync_background_jobs())
        if action == "start_time_entry":
            return _result(
                start_time_entry(
                    payload.get("employee") or {},
                    payload.get("shift_id"),
                    payload.get("machine_no"),
                    payload.get("production_number"),
                    payload.get("operation_id"),
                    payload.get("detail_type"),
                )
            )
        if action == "pause_for_lunch":
            return _result(
                pause_for_lunch(
                    payload.get("entry_id"),
                    payload.get("break_type"),
                    payload.get("comments"),
                )
            )
        if action == "resume_from_lunch":
            return _result(resume_from_lunch(payload.get("entry_id")))
        if action == "stop_time_entry":
            return _result(stop_time_entry(payload.get("entry_id"), payload.get("quantity")))
        if action == "edit_active_time_entry":
            return _result(
                edit_active_time_entry(
                    payload.get("entry_id"),
                    payload.get("production_number"),
                    payload.get("operation_id"),
                    payload.get("detail_type"),
                    payload.get("downtime_reason"),
                    payload.get("quantity"),
                    payload.get("break_minutes"),
                    payload.get("comments"),
                )
            )
        if action == "correct_time_entry":
            return _result(
                correct_time_entry(
                    payload.get("entry_id"),
                    payload.get("production_number"),
                    payload.get("operation_id"),
                    payload.get("detail_type"),
                    payload.get("downtime_reason"),
                    payload.get("quantity"),
                    payload.get("break_minutes"),
                    payload.get("total_hours"),
                    payload.get("total_minutes_remainder"),
                    payload.get("comments"),
                )
            )
        if action == "delete_time_entry":
            return _result(delete_time_entry(payload.get("entry_id"), payload.get("entry_list")))
        if action == "approve_time_entry":
            return _result(
                approve_time_entry(
                    payload.get("approval_id"),
                    payload.get("reviewer") or {},
                    payload.get("note"),
                )
            )
        if action == "reject_time_entry":
            return _result(
                reject_time_entry(
                    payload.get("approval_id"),
                    payload.get("reviewer") or {},
                    payload.get("note"),
                )
            )
        if action == "update_approval_time_entry":
            return _result(update_approval_time_entry(payload.get("approval_id"), payload.get("fields") or {}))
        if action == "submit_misc_time":
            return _result(
                submit_misc_time(
                    payload.get("employee") or {},
                    payload.get("shift_id"),
                    payload.get("machine_no"),
                    payload.get("detail_type_ii"),
                    payload.get("production_number"),
                    payload.get("operation_id"),
                    payload.get("hours"),
                    payload.get("minutes"),
                    payload.get("comments"),
                )
            )
        if action == "submit_manual_time":
            return _result(
                submit_manual_time(
                    payload.get("employee") or {},
                    payload.get("shift_id"),
                    payload.get("machine_no"),
                    payload.get("production_number"),
                    payload.get("operation_id"),
                    payload.get("detail_type"),
                    payload.get("quantity"),
                    payload.get("hours"),
                    payload.get("minutes"),
                    payload.get("comments"),
                )
            )
        if action == "submit_daily_checklist":
            return _result(
                submit_daily_checklist(
                    payload.get("employee") or {},
                    payload.get("shift_id"),
                    payload.get("machine_no"),
                    payload.get("initials"),
                    payload.get("notes"),
                    payload.get("checks") or {},
                )
            )
        if action == "submit_maintenance_request":
            return _result(
                submit_maintenance_request(
                    payload.get("employee") or {},
                    payload.get("requester_id"),
                    payload.get("requester_name"),
                    payload.get("title"),
                    payload.get("description"),
                    payload.get("priority"),
                    payload.get("location_id"),
                    payload.get("asset_id"),
                    payload.get("user_email"),
                )
            )
        if action == "send_it_request":
            return _result(
                send_it_request(
                    payload.get("user_email"),
                    payload.get("user_name"),
                    payload.get("machine_no"),
                    payload.get("category"),
                    payload.get("issue"),
                )
            )
        if action == "send_supervisor_message":
            return _result(
                send_supervisor_message(
                    payload.get("user_name"),
                    payload.get("machine_no"),
                    payload.get("comment"),
                )
            )
    except Exception as exc:
        _error(exc)

    _error(f"Unknown action: {action}")


if __name__ == "__main__":
    main()
