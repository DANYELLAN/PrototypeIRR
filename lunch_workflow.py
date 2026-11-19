"""Independent lunch verification records; never mutate production pipe history."""
import json
import math
import uuid

from psycopg2.extras import RealDictCursor
from postgres_sync import get_db_connection

LUNCH_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS inspection_lunch_checks (
    id UUID PRIMARY KEY,
    session_id BIGINT NOT NULL REFERENCES inspector_sessions(id),
    source_attempt_id BIGINT REFERENCES inspection_attempts(id) ON DELETE SET NULL,
    branch TEXT NOT NULL,
    snapshot JSONB NOT NULL,
    inspector_item_id BIGINT,
    inspector_adp TEXT,
    inspector_name TEXT,
    measurements JSONB NOT NULL DEFAULT '[]'::jsonb,
    result TEXT NOT NULL DEFAULT 'pending' CHECK (result IN ('pending', 'pass', 'fail')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    submitted_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS inspection_lunch_checks_session_idx ON inspection_lunch_checks(session_id, submitted_at DESC);
"""


def _records(cursor, branch, production_number, operation_description):
    cursor.execute("""SELECT id, inspector_name, result, submitted_at,
                          snapshot->>'pipe_number' AS pipe_number
                      FROM inspection_lunch_checks
                      WHERE branch = %s AND snapshot->>'production_number' = %s
                        AND snapshot->>'operation_description' = %s AND submitted_at IS NOT NULL
                      ORDER BY submitted_at DESC LIMIT 50""",
                   (branch, production_number, operation_description))
    return [dict(row) for row in cursor.fetchall()]


def begin_lunch_inspection(session_id, branch, production_number, operation_description):
    from workflow_db import get_recipe_elements, build_inspection_plan
    connection = get_db_connection()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            records = _records(cursor, branch, production_number, operation_description)
            cursor.execute("""SELECT ia.id, ia.recipe_name, ia.attempt_no, ia.inspector_name,
                                 ia.completed_at, pu.pipe_number, pu.production_number, pu.operation_description
                              FROM inspection_attempts ia JOIN pipe_units pu ON pu.id = ia.pipe_unit_id
                              WHERE ia.session_id = %s AND pu.branch = %s
                                AND pu.production_number = %s AND pu.operation_description = %s
                                AND ia.completed_at IS NOT NULL
                              ORDER BY ia.completed_at DESC, ia.id DESC LIMIT 1""",
                           (session_id, branch, production_number, operation_description))
            source = cursor.fetchone()
            if not source:
                return {"draft": None, "records": records, "message": "Submit a pipe for this work order and connection before starting a lunch inspection."}
            recipe = get_recipe_elements(source["recipe_name"], branch=branch)
            plan = build_inspection_plan(recipe, source["attempt_no"], inspection_scope="full")
            if not plan:
                raise ValueError("The last submitted pipe has no full inspection recipe available.")
            cursor.execute("SELECT element_sequence, measured_value, pass_fail FROM inspection_measurements WHERE attempt_id = %s ORDER BY element_sequence", (source["id"],))
            snapshot = {**dict(source), "elements": plan, "baseline": [dict(row) for row in cursor.fetchall()]}
            draft_id = str(uuid.uuid4())
            cursor.execute("INSERT INTO inspection_lunch_checks (id, session_id, source_attempt_id, branch, snapshot) VALUES (%s,%s,%s,%s,%s)",
                           (draft_id, session_id, source["id"], branch, json.dumps(snapshot, default=str)))
        connection.commit()
        return {"draft": {"id": draft_id, "snapshot": snapshot, "result": "pending"}, "records": records}
    finally:
        connection.close()


def evaluate_lunch_measurements(elements, values):
    from workflow_db import evaluate_measurements
    measurements = []
    for element in elements:
        raw = values.get(str(element["element_sequence"]))
        if raw is None or str(raw).strip() == "":
            raise ValueError("Enter every measurement in the full lunch inspection.")
        if element.get("capture_type") == "numeric":
            try:
                if not math.isfinite(float(str(raw).strip())):
                    raise ValueError()
            except (TypeError, ValueError, OverflowError):
                raise ValueError("Enter a finite numeric reading for every numeric element.") from None
        elif element.get("capture_type") == "boolean" and str(raw).strip().lower() not in {"yes", "no"}:
            raise ValueError("Choose Pass or Fail for every visual element.")
        measurements.append({**element, "measured_value": raw})
    if not measurements:
        raise ValueError("The full lunch inspection has no elements.")
    return evaluate_measurements(measurements)


def save_lunch_inspection(record_id, session_id, branch, adp_number, values):
    from workflow_db import get_employee_by_adp
    inspector = get_employee_by_adp(adp_number)
    if not inspector or inspector.get("branch") != branch:
        raise ValueError("Enter an active employee ADP number for this branch.")
    connection = get_db_connection()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT * FROM inspection_lunch_checks WHERE id = %s AND session_id = %s AND branch = %s FOR UPDATE", (record_id, session_id, branch))
            record = cursor.fetchone()
            if not record:
                raise ValueError("This lunch inspection does not belong to the current session.")
            if record["submitted_at"]:
                return dict(record)
            evaluated = evaluate_lunch_measurements(record["snapshot"]["elements"], values)
            result = "fail" if evaluated["has_failures"] else "pass"
            cursor.execute("""UPDATE inspection_lunch_checks SET inspector_item_id = %s, inspector_adp = %s,
                                 inspector_name = %s, measurements = %s, result = %s, submitted_at = NOW()
                              WHERE id = %s RETURNING *""",
                           (inspector.get("item_id"), inspector["adp_number"], inspector["name"], json.dumps(evaluated["measurements"], default=str), result, record_id))
            saved = dict(cursor.fetchone())
        connection.commit()
        return saved
    finally:
        connection.close()


def get_lunch_inspection(record_id, branch):
    connection = get_db_connection()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            query = "SELECT * FROM inspection_lunch_checks WHERE id = %s AND submitted_at IS NOT NULL"
            params = [record_id]
            if branch is not None:
                query += " AND branch = %s"
                params.append(branch)
            cursor.execute(query, params)
            row = cursor.fetchone()
            if not row:
                raise ValueError("Saved lunch inspection not found.")
            return dict(row)
    finally:
        connection.close()
