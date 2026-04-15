import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sharepoint_api import sync_work_orders_to_postgres  # noqa: E402
from workflow_db import (  # noqa: E402
    close_inspector_session,
    complete_inspection_attempt,
    create_inspection_attempt,
    create_inspector_session,
    delete_pipe_unit,
    determine_shift,
    evaluate_measurements,
    find_recipe_candidates,
    get_attempt_measurements,
    get_connection_types,
    get_cnc_operators,
    get_employee_by_adp,
    get_locations,
    get_manager_candidates,
    get_ncr_reports,
    get_open_work_orders,
    get_pipe_attempt_history,
    get_recipe_builder_options,
    get_local_recipe_by_id,
    get_pipe_unit_by_id,
    get_pipe_unit,
    get_recipe_elements,
    list_local_recipes,
    has_manager_pin,
    initialize_workflow_schema,
    is_admin_user,
    is_manager_or_supervisor,
    search_pipe_units,
    set_manager_pin,
    create_local_recipe,
    reset_in_progress_pipe_unit,
    update_local_recipe,
    update_pipe_unit,
    update_ncr_report,
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
        if action == "initialize":
            initialize_workflow_schema()
            return _result({"initialized": True})
        if action == "sync_work_orders":
            return _result(sync_work_orders_to_postgres())
        if action == "determine_shift":
            return _result(determine_shift())
        if action == "get_locations":
            return _result(get_locations())
        if action == "get_employee_by_adp":
            return _result(get_employee_by_adp(payload.get("adp_number")))
        if action == "get_cnc_operators":
            return _result(get_cnc_operators(payload.get("branch")))
        if action == "get_manager_candidates":
            return _result(get_manager_candidates(payload.get("branch")))
        if action == "is_admin_user":
            return _result(is_admin_user(payload.get("employee")))
        if action == "is_manager_or_supervisor":
            return _result(is_manager_or_supervisor(payload.get("employee")))
        if action == "set_manager_pin":
            set_manager_pin(payload.get("manager_employee"), payload.get("pin"))
            return _result({"saved": True})
        if action == "has_manager_pin":
            return _result(has_manager_pin(payload.get("manager_item_id")))
        if action == "get_open_work_orders":
            return _result(get_open_work_orders(payload.get("branch")))
        if action == "get_recipe_builder_options":
            return _result(get_recipe_builder_options(payload.get("branch")))
        if action == "list_local_recipes":
            return _result(list_local_recipes(payload.get("branch")))
        if action == "get_local_recipe_by_id":
            return _result(get_local_recipe_by_id(payload.get("recipe_header_id")))
        if action == "get_connection_types":
            return _result(
                get_connection_types(payload.get("production_number"), payload.get("branch"))
            )
        if action == "find_recipe_candidates":
            return _result(
                find_recipe_candidates(
                    payload.get("operation_description"), payload.get("branch")
                )
            )
        if action == "get_recipe_elements":
            return _result(get_recipe_elements(payload.get("recipe_name"), payload.get("branch")))
        if action == "get_pipe_unit":
            return _result(
                get_pipe_unit(
                    payload.get("production_number"),
                    payload.get("operation_description"),
                    payload.get("pipe_number"),
                )
            )
        if action == "get_pipe_unit_by_id":
            return _result(get_pipe_unit_by_id(payload.get("pipe_unit_id")))
        if action == "get_pipe_attempt_history":
            return _result(get_pipe_attempt_history(payload.get("pipe_unit_id")))
        if action == "get_attempt_measurements":
            return _result(get_attempt_measurements(payload.get("attempt_id")))
        if action == "search_pipe_units":
            return _result(
                search_pipe_units(
                    branch=payload.get("branch"),
                    production_number=payload.get("production_number"),
                    pipe_number=payload.get("pipe_number"),
                    status=payload.get("status"),
                    inspection_scope=payload.get("inspection_scope"),
                )
            )
        if action == "get_ncr_reports":
            return _result(get_ncr_reports(payload.get("branch"), payload.get("status")))
        if action == "update_ncr_report":
            update_ncr_report(
                payload.get("ncr_id"),
                status=payload.get("status"),
                disposition=payload.get("disposition"),
                tier_code=payload.get("tier_code"),
                nonconformance=payload.get("nonconformance"),
                immediate_containment=payload.get("immediate_containment"),
            )
            return _result({"updated": True})
        if action == "delete_pipe_unit":
            delete_pipe_unit(payload.get("pipe_unit_id"))
            return _result({"deleted": True})
        if action == "reset_in_progress_pipe_unit":
            return _result(reset_in_progress_pipe_unit(payload.get("pipe_unit_id")))
        if action == "update_pipe_unit":
            return _result(
                update_pipe_unit(
                    payload.get("pipe_unit_id"),
                    payload.get("production_number"),
                    payload.get("operation_description"),
                    payload.get("pipe_number"),
                )
            )
        if action == "create_local_recipe":
            return _result(create_local_recipe(payload.get("recipe_payload") or {}))
        if action == "update_local_recipe":
            return _result(
                update_local_recipe(
                    payload.get("recipe_header_id"),
                    payload.get("recipe_payload") or {},
                )
            )
        if action == "evaluate_measurements":
            return _result(
                evaluate_measurements(
                    payload.get("measurements", []),
                    approval_rules=payload.get("approval_rules", []),
                )
            )
        if action == "create_inspection_attempt":
            return _result(create_inspection_attempt(**payload.get("params", {})))
        if action == "complete_inspection_attempt":
            return _result(complete_inspection_attempt(**payload.get("params", {})))
        if action == "create_inspector_session":
            return _result(create_inspector_session(**payload.get("params", {})))
        if action == "close_inspector_session":
            close_inspector_session(payload.get("session_id"))
            return _result({"closed": True})
    except Exception as exc:
        _error(exc)

    _error(f"Unknown action: {action}")


if __name__ == "__main__":
    main()
