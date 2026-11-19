"""Read-only admin monitoring and isolated in-progress measurement drafts."""
import json
from psycopg2.extras import RealDictCursor
from postgres_sync import get_db_connection

LIVE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS irr_inspection_monitor_drafts (
    attempt_id BIGINT PRIMARY KEY REFERENCES inspection_attempts(id) ON DELETE CASCADE,
    session_id BIGINT NOT NULL REFERENCES inspector_sessions(id),
    readings JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


def require_admin(actor_adp):
    from workflow_db import get_employee_by_adp, is_admin_user
    if not is_admin_user(get_employee_by_adp(actor_adp)):
        raise ValueError("Admin access required.")


def save_live_draft(attempt_id, session_id, readings):
    if not isinstance(readings, dict) or len(readings) > 100:
        raise ValueError("Invalid measurement draft.")
    clean = {}
    for key, value in readings.items():
        if not str(key).isdigit() or len(str(value)) > 64:
            raise ValueError("Invalid draft element or value.")
        clean[str(int(key))] = str(value).strip()
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("""SELECT ia.id FROM inspection_attempts ia
                              JOIN inspector_sessions sess ON sess.id = ia.session_id
                              WHERE ia.id = %s AND ia.session_id = %s AND ia.status = 'in_progress'
                                AND sess.logged_out_at IS NULL FOR UPDATE OF ia""", (attempt_id, session_id))
            if not cursor.fetchone():
                raise ValueError("This inspection is no longer active in this session.")
            cursor.execute("""INSERT INTO irr_inspection_monitor_drafts (attempt_id, session_id, readings)
                              VALUES (%s,%s,%s) ON CONFLICT (attempt_id) DO UPDATE
                              SET readings = EXCLUDED.readings, updated_at = NOW()""",
                           (attempt_id, session_id, json.dumps(clean)))
        connection.commit()
        return {"saved": True}
    finally:
        connection.close()


def get_admin_inspection_data(actor_adp, work_order="", result=""):
    require_admin(actor_adp)
    from workflow_db import get_recipe_elements, build_inspection_plan, evaluate_measurements
    connection = get_db_connection()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""SELECT ia.id AS attempt_id, COALESCE(coverage.relief_name, ia.inspector_name) AS inspector_name, ia.recipe_name, ia.inspection_scope,
                                 ia.attempt_no, ia.relief_for_inspector_name, ia.started_at,
                                 pu.production_number, pu.operation_description, pu.pipe_number, pu.branch,
                                 sess.location_name, sess.shift, d.readings, d.updated_at AS draft_updated_at
                              FROM inspection_attempts ia JOIN pipe_units pu ON pu.id = ia.pipe_unit_id
                              JOIN inspector_sessions sess ON sess.id = ia.session_id
                              LEFT JOIN irr_inspection_monitor_drafts d ON d.attempt_id = ia.id
                              LEFT JOIN inspection_breaks coverage ON coverage.session_id=ia.session_id AND coverage.ended_at IS NULL AND coverage.mode='with_relief'
                              WHERE ia.status = 'in_progress' AND pu.current_status = 'in_progress'
                                AND sess.logged_out_at IS NULL
                              ORDER BY sess.location_name, ia.started_at DESC LIMIT 200""")
            live = [dict(row) for row in cursor.fetchall()]
            clauses = ["submitted_at IS NOT NULL"]
            params = []
            if str(work_order).strip():
                clauses.append("snapshot->>'production_number' ILIKE %s")
                params.append("%" + str(work_order).strip() + "%")
            if result in {"pass", "fail"}:
                clauses.append("result = %s")
                params.append(result)
            cursor.execute("""SELECT id, inspector_name, inspector_adp, branch, result, submitted_at,
                                 snapshot->>'production_number' AS production_number,
                                 snapshot->>'operation_description' AS operation_description,
                                 snapshot->>'pipe_number' AS pipe_number
                              FROM inspection_lunch_checks WHERE """ + " AND ".join(clauses) + " ORDER BY submitted_at DESC LIMIT 100", params)
            lunches = [dict(row) for row in cursor.fetchall()]
        recipes = {}
        for row in live:
            key = (row["recipe_name"], row["branch"])
            if key not in recipes:
                recipes[key] = get_recipe_elements(row["recipe_name"], branch=row["branch"])
            plan = build_inspection_plan(recipes[key], row["attempt_no"], inspection_scope=row["inspection_scope"], rotation_index=int(row["pipe_number"]) if str(row["pipe_number"]).isdigit() else None)
            readings = row.pop("readings") or {}
            filled = [{**element, "measured_value": readings.get(str(element["element_sequence"]), "")} for element in plan]
            evaluated = evaluate_measurements(filled)["measurements"]
            row["measurements"] = [{**element, "pass_fail": element["pass_fail"] if str(element["measured_value"]).strip() else "Pending"} for element in evaluated]
            row["entered"] = sum(bool(str(readings.get(str(element["element_sequence"]), "")).strip()) for element in plan)
            row["required"] = len(plan)
        return {"live": live, "lunches": lunches}
    finally:
        connection.close()


def get_admin_lunch_inspection(actor_adp, record_id):
    require_admin(actor_adp)
    from lunch_workflow import get_lunch_inspection
    return {"record": get_lunch_inspection(record_id, branch=None)}
