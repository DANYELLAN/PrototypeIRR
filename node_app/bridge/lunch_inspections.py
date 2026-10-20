"""Independent full inspections: never write production attempts, measurements or pipe status."""
import math
import json
from psycopg2.extras import Json
import workflow_db as db
from node_app.bridge.pipe_report import REPORT_SELECT, can_view_pipe_report


def reviewer(adp):
    employee = db.get_employee_by_adp(str(adp or "").strip())
    if not can_view_pipe_report(employee):
        raise ValueError("Lunch inspections require an active Quality admin or IT ADP number.")
    return employee


def latest_completed(station_id):
    row = db._fetch_one_dict(REPORT_SELECT.replace(" AND ia.attempt_no = pu.latest_attempt_no", "") + " WHERE sess.location_id=%s AND ia.completed_at IS NOT NULL AND ia.status <> 'in_progress' ORDER BY ia.completed_at DESC, ia.id DESC LIMIT 1", (int(station_id),))
    if not row:
        raise ValueError("No completed pipe is available at this station.")
    # Convert timestamps to JSON-safe source snapshot values.
    return json.loads(json.dumps(dict(row), default=str))


def lunch_target(station_id, reviewer_adp):
    employee = reviewer(reviewer_adp)
    return {"reviewer": employee, "pipe": latest_completed(station_id)}


def start_lunch(station_id, reviewer_adp, source_attempt_id, confirmed_pipe_number):
    employee = reviewer(reviewer_adp)
    source = latest_completed(station_id)
    if str(source["attempt_id"]) != str(source_attempt_id):
        raise ValueError("The station has completed another pipe. Review the last completed pipe again.")
    if str(source["pipe_number"]).strip() != str(confirmed_pipe_number or "").strip():
        raise ValueError("The confirmed pipe number must match the last completed pipe.")
    recipe = db.get_recipe_elements(source["recipe_name"], source.get("branch"))
    plan = db.build_inspection_plan(recipe, 1, inspection_scope="full")
    if not plan:
        raise ValueError("This pipe has no Digital IRR elements. Quality must check its recipe.")
    connection = db.get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("""INSERT INTO lunch_inspections(source_attempt_id,station_id,reviewer_adp,reviewer_name,reviewer_title,source_snapshot,recipe_snapshot,confirmed_pipe_number)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""", (source["attempt_id"], int(station_id), employee["adp_number"], employee["name"], employee.get("adp_job_title_desc") or employee.get("department") or "IT", Json(source), Json(plan), str(source["pipe_number"])))
            result = cursor.fetchone()[0]
        connection.commit()
        return get_lunch(result, reviewer_adp)
    finally:
        connection.close()


def get_lunch(inspection_id, reviewer_adp):
    employee = reviewer(reviewer_adp)
    row = db._fetch_one_dict("SELECT * FROM lunch_inspections WHERE id=%s AND reviewer_adp=%s", (int(inspection_id), employee["adp_number"]))
    if not row:
        raise ValueError("Lunch inspection not found for this reviewer.")
    return dict(row)


def evaluate_full(plan, values):
    measurements = []
    for element in plan:
        row = dict(element)
        value = values.get(str(element["element_sequence"]), "")
        if element.get("capture_type") == "boolean":
            value = "Yes" if str(value).lower() in {"yes", "true", "1", "on"} else "No"
        elif element.get("capture_type") == "numeric":
            try:
                if not math.isfinite(float(str(value).strip())):
                    raise ValueError()
            except (ValueError, TypeError):
                raise ValueError("Enter a complete finite measurement for every numeric element.")
        elif not str(value or "").strip():
            raise ValueError("Complete every inspection element before submitting.")
        row["measured_value"] = value
        measurements.append(row)
    return db.evaluate_measurements(measurements)


def complete_lunch(inspection_id, reviewer_adp, values, notes=""):
    row = get_lunch(inspection_id, reviewer_adp)
    if row["status"] != "in_progress":
        return row  # Retry is idempotent; submitted lunch records are immutable.
    result = evaluate_full(row["recipe_snapshot"], values)
    connection = db.get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("""UPDATE lunch_inspections SET measurements=%s,status=%s,notes=%s,completed_at=NOW()
                WHERE id=%s AND reviewer_adp=%s AND status='in_progress'""", (Json(result["measurements"]), "failed" if result["has_failures"] else "passed", str(notes or ""), int(inspection_id), row["reviewer_adp"]))
        connection.commit()
        return get_lunch(inspection_id, reviewer_adp)
    finally:
        connection.close()


def lunch_history(reviewer_adp, page=1):
    reviewer(reviewer_adp)
    page = max(1, int(page))
    total = db._fetch_one_dict("SELECT COUNT(*) AS total FROM lunch_inspections WHERE completed_at IS NOT NULL")["total"]
    pages = max(1, (total + 19) // 20)
    page = min(page, pages)
    rows = db._fetch_all_dicts("SELECT * FROM lunch_inspections WHERE completed_at IS NOT NULL ORDER BY completed_at DESC,id DESC LIMIT 20 OFFSET %s", ((page-1)*20,))
    return {"inspections": rows, "page": page, "pages": pages, "total": total}
