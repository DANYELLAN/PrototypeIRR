"""Persistent station breaks and relief attribution, independent of the login identity."""
import workflow_db as db


def _context(cursor, session_id, lock=False):
    cursor.execute("SELECT inspector_adp, inspector_name, logged_out_at, inspector_item_id FROM inspector_sessions WHERE id=%s" + (" FOR UPDATE" if lock else ""), (int(session_id),))
    session = cursor.fetchone()
    if not session or session[2] is not None:
        raise ValueError("An active inspector session is required.")
    cursor.execute("SELECT id,mode,relief_adp,relief_name,started_at,ended_at,relief_item_id FROM inspection_breaks WHERE session_id=%s ORDER BY id DESC LIMIT 1", (int(session_id),))
    row = cursor.fetchone()
    active = bool(row and row[5] is None)
    return {"session_id": int(session_id), "version": row[0]*2 + (0 if active else 1) if row else 0,
            "break_id": row[0] if active else None, "mode": row[1] if active else None,
            "original_adp": session[0], "original_name": session[1],
            "relief_adp": row[2] if active else None, "relief_name": row[3] if active else None,
            "started_at": row[4] if active else None,
            "acting_adp": row[2] if active and row[1]=="with_relief" else session[0],
            "acting_name": row[3] if active and row[1]=="with_relief" else session[1],
            "acting_item_id": row[6] if active and row[1]=="with_relief" else session[3]}


def inspection_break_state(session_id):
    connection = db.get_db_connection()
    try:
        with connection.cursor() as cursor:
            return _context(cursor, session_id)
    finally:
        connection.close()


def validate_context(context, expected_version=None):
    if expected_version is not None and int(expected_version) != context["version"]:
        raise ValueError("Break coverage changed. Review the current inspector before continuing.")
    if context["mode"] == "without_relief":
        raise ValueError("Inspection is paused. Click End Break before continuing.")
    return context


def lock_attempt_context(cursor, attempt_id, expected_version=None):
    cursor.execute("SELECT session_id FROM inspection_attempts WHERE id=%s", (attempt_id,))
    row = cursor.fetchone()
    if not row or row[0] is None:
        raise ValueError("This inspection has no active station session.")
    return validate_context(_context(cursor, row[0], lock=True), expected_version)


def start_inspection_break(session_id, mode, relief_adp=None, expected_version=0):
    if mode not in {"with_relief", "without_relief"}:
        raise ValueError("Choose With Relief or Without Relief.")
    employee = None
    if mode == "with_relief":
        employee = db.get_employee_by_adp(str(relief_adp or "").strip())
        if not employee or employee.get("employee_status") not in {None, "", "Active"}:
            raise ValueError("Enter a valid active employee ADP number for the relief inspector.")
    connection = db.get_db_connection()
    try:
        with connection.cursor() as cursor:
            context = _context(cursor, session_id, lock=True)
            validate_context(context, expected_version)
            if context["break_id"]:
                raise ValueError("A break is already active. Click End Break first.")
            if employee and str(employee["adp_number"]) == str(context["original_adp"]):
                raise ValueError("The relief inspector must have a different ADP number from the original inspector.")
            cursor.execute("INSERT INTO inspection_breaks(session_id,mode,relief_adp,relief_name,relief_item_id) VALUES (%s,%s,%s,%s,%s)",
                           (int(session_id), mode, str(employee["adp_number"]) if employee else None, employee["name"] if employee else None, db._coerce_bigint(employee.get("item_id")) if employee else None))
            result = _context(cursor, session_id)
        connection.commit()
        return result
    finally:
        connection.close()


def end_inspection_break(session_id, expected_version):
    connection = db.get_db_connection()
    try:
        with connection.cursor() as cursor:
            context = _context(cursor, session_id, lock=True)
            if int(expected_version) != context["version"]:
                raise ValueError("Break coverage changed. Refresh the break status before continuing.")
            if not context["break_id"]:
                raise ValueError("There is no active break to end.")
            cursor.execute("UPDATE inspection_breaks SET ended_at=NOW() WHERE id=%s AND ended_at IS NULL", (context["break_id"],))
            result = _context(cursor, session_id)
        connection.commit()
        return result
    finally:
        connection.close()


def record_relief(cursor, attempt_id, context):
    if context["mode"] == "with_relief":
        cursor.execute("""INSERT INTO inspection_relief_attempts(attempt_id,break_id) VALUES (%s,%s)
            ON CONFLICT(attempt_id,break_id) DO UPDATE SET last_recorded_at=NOW()""", (attempt_id,context["break_id"]))
