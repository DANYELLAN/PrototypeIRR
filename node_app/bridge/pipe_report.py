"""Read live station/pipe snapshots and record version-specific verification."""
import hashlib
import json

from psycopg2.extras import RealDictCursor, Json
import workflow_db as db

QUALITY_TITLES = {"director of quality", "quality control specialist", "quality manager", "quality supervisor"}
IT_TITLES = {"it", "it manager", "information technology", "information systems", "integration technologist"}


def can_view_pipe_report(employee):
    if not employee or employee.get("employee_status") not in {None, "", "Active"}:
        return False
    title = str(employee.get("adp_job_title_desc") or "").strip().lower()
    department = str(employee.get("department") or "").strip().lower()
    return title in QUALITY_TITLES | IT_TITLES or department in {"it", "information technology", "information systems"}


def inspection_snapshot_hash(row):
    keys = ("attempt_id", "pipe_unit_id", "production_number", "operation_description", "pipe_number", "attempt_no", "inspection_scope", "status", "notes", "manager_name", "recipe_name", "inspector_name", "inspector_adp", "relief_inspectors", "cnc_operator_name", "completed_at", "measurements")
    value = {key: row.get(key) for key in keys}
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


REPORT_SELECT = """
SELECT ia.id AS attempt_id, pu.id AS pipe_unit_id, pu.production_number, pu.operation_description,
       pu.pipe_number, pu.current_status, ia.attempt_no, ia.inspection_scope, ia.status,
       ia.notes, ia.manager_name, ia.recipe_name, ia.inspector_name, ia.cnc_operator_name,
       ia.inspector_adp, sess.inspector_name AS original_inspector_name,
       COALESCE((SELECT jsonb_agg(jsonb_build_object('adp_number', b.relief_adp, 'name', b.relief_name, 'break_id', b.id) ORDER BY b.id)
           FROM inspection_relief_attempts ra JOIN inspection_breaks b ON b.id=ra.break_id WHERE ra.attempt_id=ia.id), '[]'::jsonb) AS relief_inspectors,
       ia.started_at, ia.completed_at, sess.location_id, sess.location_name, sess.shift, sess.branch,
       COALESCE((SELECT jsonb_agg(to_jsonb(im) ORDER BY im.element_sequence, im.id)
                 FROM inspection_measurements im WHERE im.attempt_id = ia.id), '[]'::jsonb) AS measurements
FROM pipe_units pu
JOIN inspection_attempts ia ON ia.pipe_unit_id = pu.id AND ia.attempt_no = pu.latest_attempt_no
LEFT JOIN inspector_sessions sess ON sess.id = ia.session_id
"""


def get_live_pipe_report(page=1, station_id=None):
    try:
        page = max(1, int(page))
        station = int(station_id) if station_id not in (None, "", "all") else None
    except (TypeError, ValueError):
        raise ValueError("Invalid report page or station")
    connection = db.get_db_connection()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            cursor.execute("""
                SELECT sess.id, sess.location_id, sess.location_name, sess.inspector_name, sess.shift, sess.branch,
                       active.id AS attempt_id, active.pipe_number, active.production_number,
                       coverage.mode AS break_mode, coverage.relief_name, coverage.relief_adp,
                       COALESCE(draft.updated_at, active.started_at, sess.logged_in_at) AS last_activity
                FROM inspector_sessions sess
                LEFT JOIN LATERAL (
                    SELECT ia.id, ia.started_at, pu.pipe_number, pu.production_number
                    FROM inspection_attempts ia JOIN pipe_units pu ON pu.id = ia.pipe_unit_id
                    WHERE ia.session_id = sess.id AND ia.status = 'in_progress'
                    ORDER BY ia.started_at DESC LIMIT 1
                ) active ON TRUE
                LEFT JOIN inspection_live_drafts draft ON draft.attempt_id = active.id
                LEFT JOIN inspection_breaks coverage ON coverage.session_id=sess.id AND coverage.ended_at IS NULL
                WHERE sess.logged_out_at IS NULL ORDER BY sess.location_name, sess.id
            """)
            stations = [dict(row) for row in cursor.fetchall()]
            where = " WHERE sess.location_id = %s" if station is not None else ""
            params = (station,) if station is not None else ()
            cursor.execute("SELECT COUNT(*) AS total FROM pipe_units pu JOIN inspection_attempts ia ON ia.pipe_unit_id=pu.id AND ia.attempt_no=pu.latest_attempt_no LEFT JOIN inspector_sessions sess ON sess.id=ia.session_id" + where, params)
            total = int(cursor.fetchone()["total"])
            pages = max(1, (total + 19) // 20)
            page = min(page, pages)
            cursor.execute(REPORT_SELECT + where + " ORDER BY ia.started_at DESC, ia.id DESC LIMIT 20 OFFSET %s", (*params, (page - 1) * 20))
            rows = [dict(row) for row in cursor.fetchall()]
            ids = [row["attempt_id"] for row in rows]
            drafts, verifications = {}, {}
            if ids:
                cursor.execute("SELECT attempt_id, measurements, updated_at FROM inspection_live_drafts WHERE attempt_id = ANY(%s)", (ids,))
                drafts = {row["attempt_id"]: dict(row) for row in cursor.fetchall()}
                cursor.execute("SELECT * FROM inspection_verifications WHERE attempt_id = ANY(%s) ORDER BY verified_at DESC, id DESC", (ids,))
                for verification in cursor.fetchall():
                    verifications.setdefault(verification["attempt_id"], []).append(dict(verification))
            recipes = {}
            for row in rows:
                row["snapshot_hash"] = inspection_snapshot_hash(row)
                reviews = verifications.get(row["attempt_id"], [])
                row["verification"] = next((review for review in reviews if review["snapshot_hash"] == row["snapshot_hash"]), None)
                row["verification_changed"] = bool(reviews and not row["verification"])
                row["live_updated_at"] = None
                if row["status"] == "in_progress":
                    draft = drafts.get(row["attempt_id"])
                    row["measurements"] = draft["measurements"] if draft else []
                    row["live_updated_at"] = draft["updated_at"] if draft else None
                    row["verification"] = None
                recipe_key = (row.get("recipe_name"), row.get("branch"))
                if recipe_key not in recipes:
                    recipes[recipe_key] = db.get_recipe_elements(*recipe_key) if recipe_key[0] else None
                recipe = recipes[recipe_key] or {}
                row["recipe_elements"] = recipe.get("elements", [])
                row["recipe_drawing"] = recipe.get("drawing")
                if row["status"] == "in_progress" and recipe:
                    number = str(row.get("pipe_number") or "")
                    plan = db.build_inspection_plan(recipe, row["attempt_no"], inspection_scope=row["inspection_scope"], rotation_index=int(number) if number.isdigit() else None)
                    row["planned_sequences"] = [element["element_sequence"] for element in plan]
                elif row["status"] != "in_progress":
                    row["planned_sequences"] = [element.get("element_sequence") for element in row["measurements"]]
            return {"stations": stations, "pipes": rows, "page": page, "pages": pages, "total": total, "page_size": 20}
    finally:
        connection.close()


def save_live_inspection_draft(attempt_id, session_id, inspector_adp, measurements, client_sequence, coverage_version=None, edited_sequence=None):
    if not isinstance(measurements, list) or len(measurements) > 300:
        raise ValueError("Invalid live inspection measurements")
    evaluated = db.evaluate_measurements(measurements)["measurements"]
    for original, row in zip(measurements, evaluated):
        if row.get("capture_type") != "boolean" and str(original.get("measured_value") if original.get("measured_value") is not None else "").strip() == "":
            row["pass_fail"] = "Pending"
    connection = db.get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("""SELECT ia.id FROM inspection_attempts ia JOIN inspector_sessions sess ON sess.id=ia.session_id
                WHERE ia.id=%s AND ia.session_id=%s AND ia.status='in_progress' AND sess.inspector_adp=%s AND sess.logged_out_at IS NULL FOR UPDATE OF ia""", (attempt_id, session_id, inspector_adp))
            if not cursor.fetchone():
                raise ValueError("This inspection is no longer active for this inspector")
            from node_app.bridge.inspection_breaks import lock_attempt_context, record_relief
            context = lock_attempt_context(cursor, attempt_id, coverage_version)
            cursor.execute("""INSERT INTO inspection_live_drafts(attempt_id, measurements, client_sequence)
                VALUES (%s,%s,%s) ON CONFLICT (attempt_id) DO UPDATE SET measurements=EXCLUDED.measurements,
                client_sequence=EXCLUDED.client_sequence, updated_at=NOW()
                WHERE inspection_live_drafts.client_sequence <= EXCLUDED.client_sequence""", (attempt_id, Json(evaluated), int(client_sequence)))
            # Record only accepted edits; opening the auto-loaded blank pipe does not count as relief work.
            if cursor.rowcount and (str(edited_sequence) in {str(item.get("element_sequence")) for item in measurements} or any(str(item.get("measured_value") if item.get("measured_value") is not None else "").strip() not in {"", "No"} for item in measurements)):
                record_relief(cursor, attempt_id, context)
        connection.commit()
        return {"saved": True}
    finally:
        connection.close()


def verify_pipe_inspection(attempt_id, expected_hash, verifier_adp):
    employee = db.get_employee_by_adp(verifier_adp)
    if not can_view_pipe_report(employee):
        raise ValueError("You do not have permission to verify pipe inspections")
    connection = db.get_db_connection()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT id FROM inspection_attempts WHERE id=%s FOR UPDATE", (attempt_id,))
            if not cursor.fetchone():
                raise ValueError("Inspection not found")
            cursor.execute("SELECT pu.id FROM pipe_units pu JOIN inspection_attempts ia ON ia.pipe_unit_id=pu.id WHERE ia.id=%s FOR UPDATE OF pu", (attempt_id,))
            cursor.execute(REPORT_SELECT + " WHERE ia.id=%s", (attempt_id,))
            row = cursor.fetchone()
            if not row or row["status"] == "in_progress" or not row["completed_at"]:
                raise ValueError("Only submitted inspections can be verified")
            snapshot = inspection_snapshot_hash(row)
            if not expected_hash or snapshot != expected_hash:
                raise ValueError("This inspection changed after it was displayed. Refresh and review it again before verifying.")
            cursor.execute("""INSERT INTO inspection_verifications(attempt_id,snapshot_hash,verifier_adp,verifier_name,verifier_title)
                VALUES (%s,%s,%s,%s,%s) ON CONFLICT (attempt_id,snapshot_hash) DO NOTHING""",
                (attempt_id, snapshot, employee["adp_number"], employee["name"], employee.get("adp_job_title_desc") or employee.get("department") or "IT"))
        connection.commit()
        return {"verified": True}
    finally:
        connection.close()
