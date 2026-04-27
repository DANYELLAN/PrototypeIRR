import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from node_app.bridge.cnc_time_backend import (  # noqa: E402
    get_dashboard_context,
    get_sign_in_context,
    pause_for_lunch,
    resume_from_lunch,
    send_it_request,
    send_supervisor_message,
    sign_in,
    start_time_entry,
    stop_time_entry,
    submit_daily_checklist,
    submit_maintenance_request,
    submit_manual_time,
    submit_misc_time,
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
        if action == "sign_in":
            return _result(sign_in(payload.get("adp_number"), payload.get("user_email"), payload.get("shift_id")))
        if action == "get_dashboard_context":
            return _result(get_dashboard_context(payload.get("emp_id"), payload.get("user_email")))
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
            return _result(pause_for_lunch(payload.get("entry_id")))
        if action == "resume_from_lunch":
            return _result(resume_from_lunch(payload.get("entry_id")))
        if action == "stop_time_entry":
            return _result(stop_time_entry(payload.get("entry_id")))
        if action == "submit_misc_time":
            return _result(
                submit_misc_time(
                    payload.get("employee") or {},
                    payload.get("shift_id"),
                    payload.get("machine_no"),
                    payload.get("detail_type_ii"),
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
