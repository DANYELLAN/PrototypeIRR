import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "data" / "cnc_time_local.db"
LOCAL_MACHINE_CONFIG = Path(__file__).resolve().parent / "config" / "machines.json"


def _utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def _connect():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def _ensure_column(conn, table_name, column_name, definition):
    columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
    if column_name not in columns:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


def ensure_db():
    conn = _connect()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS machines (
            machine_no TEXT PRIMARY KEY,
            email TEXT NOT NULL,
            label TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS time_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            record_type TEXT NOT NULL,
            machine_no TEXT,
            emp_id TEXT,
            employee_name TEXT,
            payload TEXT NOT NULL,
            created_at TEXT NOT NULL,
            sync_status TEXT NOT NULL DEFAULT 'pending',
            source TEXT NOT NULL DEFAULT 'local'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            adp_number TEXT,
            employee_name TEXT,
            machine_no TEXT,
            shift_id INTEGER,
            user_email TEXT,
            created_at TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            record_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS break_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_id TEXT NOT NULL,
            emp_id TEXT,
            employee_name TEXT,
            machine_no TEXT,
            break_type TEXT NOT NULL,
            comment TEXT,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            duration_minutes INTEGER,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS approval_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            record_type TEXT NOT NULL,
            machine_no TEXT,
            emp_id TEXT,
            employee_name TEXT,
            payload TEXT NOT NULL,
            approval_status TEXT NOT NULL DEFAULT 'pending',
            submitted_at TEXT NOT NULL,
            reviewed_at TEXT,
            reviewed_by_emp_id TEXT,
            reviewed_by_name TEXT,
            review_note TEXT
        )
        """
    )
    _ensure_column(conn, "approval_queue", "revision_no", "INTEGER NOT NULL DEFAULT 1")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS approval_revisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            approval_id INTEGER NOT NULL,
            revision_no INTEGER NOT NULL,
            payload TEXT NOT NULL,
            changed_at TEXT NOT NULL,
            changed_by_emp_id TEXT,
            changed_by_name TEXT,
            change_reason TEXT,
            UNIQUE (approval_id, revision_no)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS acumatica_batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            labor_date TEXT NOT NULL,
            area TEXT NOT NULL,
            batch_nbr TEXT NOT NULL UNIQUE,
            description TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'On Hold',
            is_supplemental INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS acumatica_sync_items (
            approval_id INTEGER PRIMARY KEY,
            revision_no INTEGER NOT NULL DEFAULT 1,
            status TEXT NOT NULL DEFAULT 'pending',
            batch_nbr TEXT,
            line_nbrs TEXT,
            sent_payload TEXT,
            last_attempt_at TEXT,
            sent_at TEXT,
            last_error TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS acumatica_sync_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trigger_name TEXT NOT NULL,
            actor_emp_id TEXT,
            actor_name TEXT,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            status TEXT NOT NULL DEFAULT 'running',
            summary TEXT
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_acumatica_batches_group ON acumatica_batches (labor_date, area, id DESC)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS notification_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            notification_type TEXT NOT NULL,
            requester_emp_id TEXT NOT NULL,
            requester_name TEXT NOT NULL,
            machine_no TEXT,
            work_order TEXT,
            request_details TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            delivery_status TEXT NOT NULL DEFAULT 'queued',
            callback_token TEXT UNIQUE,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            responded_at TEXT,
            responder_name TEXT,
            response_note TEXT,
            last_error TEXT
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_notification_requests_employee ON notification_requests (requester_emp_id, id DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_acumatica_sync_status ON acumatica_sync_items (status, updated_at)"
    )
    conn.commit()
    conn.close()


def _notification_record(row):
    if not row:
        return None
    record = dict(row)
    try:
        record["details"] = json.loads(record.pop("request_details") or "{}")
    except (TypeError, json.JSONDecodeError):
        record["details"] = {}
    return record


def create_notification_request(
    notification_type,
    requester_emp_id,
    requester_name,
    machine_no,
    work_order,
    details,
    callback_token=None,
    status="pending",
):
    ensure_db()
    now = _utc_now_iso()
    conn = _connect()
    cursor = conn.execute(
        """
        INSERT INTO notification_requests (
            notification_type, requester_emp_id, requester_name, machine_no, work_order,
            request_details, status, delivery_status, callback_token, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?)
        """,
        (
            str(notification_type or "").strip(),
            str(requester_emp_id or "").strip(),
            str(requester_name or "").strip(),
            str(machine_no or "").strip(),
            str(work_order or "").strip(),
            json.dumps(details or {}, default=str),
            str(status or "pending").strip(),
            callback_token,
            now,
            now,
        ),
    )
    request_id = cursor.lastrowid
    conn.commit()
    row = conn.execute("SELECT * FROM notification_requests WHERE id = ?", (request_id,)).fetchone()
    conn.close()
    return _notification_record(row)


def get_notification_request(request_id):
    ensure_db()
    conn = _connect()
    row = conn.execute("SELECT * FROM notification_requests WHERE id = ?", (request_id,)).fetchone()
    conn.close()
    return _notification_record(row)


def list_notification_requests(requester_emp_id=None, limit=50):
    ensure_db()
    conn = _connect()
    if requester_emp_id is None:
        rows = conn.execute(
            "SELECT * FROM notification_requests ORDER BY id DESC LIMIT ?",
            (int(limit),),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM notification_requests WHERE requester_emp_id = ? ORDER BY id DESC LIMIT ?",
            (str(requester_emp_id or "").strip(), int(limit)),
        ).fetchall()
    conn.close()
    return [_notification_record(row) for row in rows]


def update_notification_delivery(request_id, delivery_status, error=None):
    ensure_db()
    conn = _connect()
    conn.execute(
        """
        UPDATE notification_requests
        SET delivery_status = ?, last_error = ?, updated_at = ?
        WHERE id = ?
        """,
        (str(delivery_status or "").strip(), error, _utc_now_iso(), request_id),
    )
    conn.commit()
    conn.close()


def decide_notification_request(request_id, status, responder_name=None, response_note=None):
    ensure_db()
    now = _utc_now_iso()
    conn = _connect()
    cursor = conn.execute(
        """
        UPDATE notification_requests
        SET status = ?, responded_at = ?, responder_name = ?, response_note = ?, updated_at = ?
        WHERE id = ? AND notification_type = 'mold_approval' AND status = 'pending'
        """,
        (
            str(status or "").strip(),
            now,
            str(responder_name or "").strip(),
            str(response_note or "").strip(),
            now,
            request_id,
        ),
    )
    changed = cursor.rowcount == 1
    conn.commit()
    conn.close()
    return changed
    _seed_machine_config()


def _load_local_machine_config():
    if not LOCAL_MACHINE_CONFIG.exists():
        return []
    try:
        with LOCAL_MACHINE_CONFIG.open("r", encoding="utf-8") as handle:
            entries = json.load(handle) or []
    except (OSError, json.JSONDecodeError):
        return []

    machines = []
    for item in entries:
        machine_no = str(item.get("machine_no") or "").strip()
        email = str(item.get("email") or "").strip().lower()
        if machine_no and email:
            machines.append({
                "machine_no": machine_no,
                "email": email,
                "label": str(item.get("label") or f"Machine {machine_no}").strip(),
            })
    return machines


def _seed_machine_config():
    machines = _load_local_machine_config()
    conn = _connect()
    if not machines:
        conn.execute("DELETE FROM machines")
        conn.commit()
        conn.close()
        return

    configured = {str(machine["machine_no"]).strip() for machine in machines}
    placeholders = ", ".join("?" for _ in configured)
    if placeholders:
        conn.execute(f"DELETE FROM machines WHERE machine_no NOT IN ({placeholders})", list(configured))

    for machine in machines:
        conn.execute(
            """
            INSERT OR REPLACE INTO machines (machine_no, email, label, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (machine["machine_no"], machine["email"], machine["label"], _utc_now_iso()),
        )
    conn.commit()
    conn.close()


def get_machine_options():
    ensure_db()
    conn = _connect()
    rows = conn.execute(
        """
        SELECT machine_no, email, label
        FROM machines
        ORDER BY
            CASE WHEN machine_no GLOB '[0-9]*' THEN 0 ELSE 1 END,
            CAST(machine_no AS INTEGER),
            label
        """
    ).fetchall()
    conn.close()
    return [
        {"machine_no": str(row["machine_no"]), "email": row["email"], "label": row["label"]}
        for row in rows
    ]


def upsert_machine_options(machines):
    ensure_db()
    if not machines:
        return
    conn = _connect()
    for machine in machines:
        machine_no = str(machine.get("machine_no") or "").strip()
        email = str(machine.get("email") or "").strip().lower()
        if not machine_no or not email:
            continue
        conn.execute(
            """
            INSERT OR REPLACE INTO machines (machine_no, email, label, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (machine_no, email, machine.get("label") or f"Machine {machine_no}", _utc_now_iso()),
        )
    conn.commit()
    conn.close()


def save_session(adp_number, employee_name, machine_no, shift_id, user_email):
    ensure_db()
    conn = _connect()
    conn.execute(
        """
        INSERT INTO sessions (adp_number, employee_name, machine_no, shift_id, user_email, created_at, is_active)
        VALUES (?, ?, ?, ?, ?, ?, 1)
        """,
        (str(adp_number or "").strip(), employee_name or "", str(machine_no or "").strip(), shift_id, user_email or "", _utc_now_iso()),
    )
    conn.commit()
    conn.close()


def queue_record(record_type, payload, machine_no=None, emp_id=None, employee_name=None, source="local"):
    ensure_db()
    conn = _connect()
    cursor = conn.execute(
        """
        INSERT INTO time_entries (record_type, machine_no, emp_id, employee_name, payload, created_at, sync_status, source)
        VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)
        """,
        (
            record_type,
            str(machine_no or "").strip(),
            str(emp_id or "").strip(),
            employee_name or "",
            json.dumps(payload, default=str),
            _utc_now_iso(),
            source,
        ),
    )
    record_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return record_id


def list_pending_records(limit=50):
    ensure_db()
    conn = _connect()
    rows = conn.execute(
        "SELECT * FROM time_entries WHERE sync_status = 'pending' ORDER BY created_at DESC LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def mark_record_synced(record_id):
    conn = _connect()
    conn.execute(
        "UPDATE time_entries SET sync_status = 'synced' WHERE id = ?",
        (record_id,),
    )
    conn.commit()
    conn.close()


def discard_pending_record(record_id):
    ensure_db()
    conn = _connect()
    conn.execute(
        "DELETE FROM time_entries WHERE id = ? AND sync_status = 'pending'",
        (record_id,),
    )
    conn.commit()
    conn.close()


def patch_pending_record_fields(record_id, fields):
    ensure_db()
    if not fields:
        return False
    conn = _connect()
    row = conn.execute(
        "SELECT payload FROM time_entries WHERE id = ? AND sync_status = 'pending'",
        (record_id,),
    ).fetchone()
    if not row:
        conn.close()
        return False

    try:
        payload = json.loads(row["payload"] or "{}")
    except json.JSONDecodeError:
        payload = {}

    payload_fields = payload.setdefault("fields", {})
    payload_fields.update(fields)
    conn.execute(
        "UPDATE time_entries SET payload = ? WHERE id = ? AND sync_status = 'pending'",
        (json.dumps(payload, default=str), record_id),
    )
    conn.commit()
    conn.close()
    return True


def queue_approval(record_type, payload, machine_no=None, emp_id=None, employee_name=None):
    ensure_db()
    conn = _connect()
    cursor = conn.execute(
        """
        INSERT INTO approval_queue (
            record_type, machine_no, emp_id, employee_name, payload, approval_status, submitted_at
        )
        VALUES (?, ?, ?, ?, ?, 'pending', ?)
        """,
        (
            record_type,
            str(machine_no or "").strip(),
            str(emp_id or "").strip(),
            employee_name or "",
            json.dumps(payload, default=str),
            _utc_now_iso(),
        ),
    )
    record_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return record_id


def list_approval_records(status="pending", limit=500):
    ensure_db()
    conn = _connect()
    if status:
        rows = conn.execute(
            "SELECT * FROM approval_queue WHERE approval_status = ? ORDER BY submitted_at DESC LIMIT ?",
            (status, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM approval_queue ORDER BY submitted_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_approval_record(record_id):
    ensure_db()
    conn = _connect()
    row = conn.execute("SELECT * FROM approval_queue WHERE id = ?", (record_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def mark_approval_reviewed(record_id, status, reviewer=None, note=None):
    ensure_db()
    conn = _connect()
    conn.execute(
        """
        UPDATE approval_queue
        SET approval_status = ?,
            reviewed_at = ?,
            reviewed_by_emp_id = ?,
            reviewed_by_name = ?,
            review_note = ?
        WHERE id = ? AND approval_status = 'pending'
        """,
        (
            status,
            _utc_now_iso(),
            str((reviewer or {}).get("emp_id") or "").strip(),
            (reviewer or {}).get("full_name") or "",
            note or "",
            record_id,
        ),
    )
    changed = conn.total_changes > 0
    conn.commit()
    conn.close()
    return changed


def approve_approval_record(record_id, reviewer=None, note=None):
    ensure_db()
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT * FROM approval_queue WHERE id = ? AND approval_status = 'pending'",
            (record_id,),
        ).fetchone()
        if not row:
            conn.close()
            return None

        cursor = conn.execute(
            """
            INSERT INTO time_entries (record_type, machine_no, emp_id, employee_name, payload, created_at, sync_status, source)
            VALUES (?, ?, ?, ?, ?, ?, 'pending', 'approved')
            """,
            (
                row["record_type"],
                row["machine_no"] or "",
                row["emp_id"] or "",
                row["employee_name"] or "",
                row["payload"],
                _utc_now_iso(),
            ),
        )
        queued_id = cursor.lastrowid
        update = conn.execute(
            """
            UPDATE approval_queue
            SET approval_status = 'approved',
                reviewed_at = ?,
                reviewed_by_emp_id = ?,
                reviewed_by_name = ?,
                review_note = ?
            WHERE id = ? AND approval_status = 'pending'
            """,
            (
                _utc_now_iso(),
                str((reviewer or {}).get("emp_id") or "").strip(),
                (reviewer or {}).get("full_name") or "",
                note or "",
                record_id,
            ),
        )
        if update.rowcount != 1:
            conn.rollback()
            conn.close()
            return None
        conn.commit()
        conn.close()
        return {"approval": dict(row), "queued_id": queued_id}
    except Exception:
        conn.rollback()
        conn.close()
        raise


def patch_approval_record_fields(record_id, fields):
    ensure_db()
    if not fields:
        return False
    conn = _connect()
    row = conn.execute(
        "SELECT payload FROM approval_queue WHERE id = ? AND approval_status = 'pending'",
        (record_id,),
    ).fetchone()
    if not row:
        conn.close()
        return False

    try:
        payload = json.loads(row["payload"] or "{}")
    except json.JSONDecodeError:
        payload = {}

    payload_fields = payload.setdefault("fields", {})
    payload_fields.update(fields)
    conn.execute(
        "UPDATE approval_queue SET payload = ? WHERE id = ? AND approval_status = 'pending'",
        (json.dumps(payload, default=str), record_id),
    )
    conn.commit()
    conn.close()
    return True


def patch_approval_record_payload(record_id, payload):
    ensure_db()
    if not payload:
        return False
    fields = payload.get("fields") or {}
    conn = _connect()
    cursor = conn.execute(
        """
        UPDATE approval_queue
        SET payload = ?,
            machine_no = ?,
            emp_id = ?,
            employee_name = ?
        WHERE id = ? AND approval_status = 'pending'
        """,
        (
            json.dumps(payload, default=str),
            str(fields.get("MachineNo") or "").strip(),
            str(fields.get("EmpID") or fields.get("EmployeeID") or "").strip(),
            fields.get("OperatorsName") or "",
            record_id,
        ),
    )
    changed = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return changed


def update_approval_with_revision(record_id, payload, editor=None, reason=None):
    """Update an approval while retaining the complete previous payload."""
    ensure_db()
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT payload, approval_status, revision_no FROM approval_queue WHERE id = ?",
            (record_id,),
        ).fetchone()
        if not row:
            conn.close()
            return None

        current_revision = int(row["revision_no"] or 1)
        next_revision = current_revision + 1
        conn.execute(
            """
            INSERT OR IGNORE INTO approval_revisions (
                approval_id, revision_no, payload, changed_at,
                changed_by_emp_id, changed_by_name, change_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record_id,
                current_revision,
                row["payload"],
                _utc_now_iso(),
                str((editor or {}).get("emp_id") or "").strip(),
                (editor or {}).get("full_name") or "",
                reason or "",
            ),
        )
        fields = payload.get("fields") or {}
        conn.execute(
            """
            UPDATE approval_queue
            SET payload = ?, revision_no = ?, machine_no = ?, emp_id = ?, employee_name = ?
            WHERE id = ?
            """,
            (
                json.dumps(payload, default=str),
                next_revision,
                str(fields.get("MachineNo") or "").strip(),
                str(fields.get("EmpID") or fields.get("EmployeeID") or "").strip(),
                fields.get("OperatorsName") or "",
                record_id,
            ),
        )
        existing_sync = conn.execute(
            "SELECT status FROM acumatica_sync_items WHERE approval_id = ?",
            (record_id,),
        ).fetchone()
        if existing_sync:
            sync_status = "changed_after_send" if existing_sync["status"] == "sent" else existing_sync["status"]
            conn.execute(
                """
                UPDATE acumatica_sync_items
                SET status = ?, last_error = NULL, updated_at = ?
                WHERE approval_id = ?
                """,
                (sync_status, _utc_now_iso(), record_id),
            )
        conn.commit()
        conn.close()
        return {
            "approval_status": row["approval_status"],
            "revision_no": next_revision,
            "had_sync_record": bool(existing_sync),
        }
    except Exception:
        conn.rollback()
        conn.close()
        raise


def list_approval_revisions(record_id):
    ensure_db()
    conn = _connect()
    rows = conn.execute(
        "SELECT * FROM approval_revisions WHERE approval_id = ? ORDER BY revision_no DESC",
        (record_id,),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def list_acumatica_candidates(limit=500, reviewed_after=None):
    ensure_db()
    conn = _connect()
    rows = conn.execute(
        """
        SELECT q.*, s.status AS acumatica_status, s.revision_no AS sent_revision_no,
               s.batch_nbr, s.line_nbrs, s.last_error, s.sent_at
        FROM approval_queue q
        LEFT JOIN acumatica_sync_items s ON s.approval_id = q.id
        WHERE q.approval_status = 'approved'
          AND (? IS NULL OR q.reviewed_at >= ?)
          AND (
              s.approval_id IS NULL
              OR s.status IN ('pending', 'failed', 'changed_after_send')
              OR q.revision_no > s.revision_no
          )
        ORDER BY q.submitted_at, q.id
        LIMIT ?
        """,
        (reviewed_after, reviewed_after, limit),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_acumatica_sync_item(approval_id):
    ensure_db()
    conn = _connect()
    row = conn.execute(
        "SELECT * FROM acumatica_sync_items WHERE approval_id = ?",
        (approval_id,),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def upsert_acumatica_sync_item(
    approval_id,
    revision_no,
    status,
    batch_nbr=None,
    line_nbrs=None,
    sent_payload=None,
    error=None,
):
    ensure_db()
    now = _utc_now_iso()
    conn = _connect()
    conn.execute(
        """
        INSERT INTO acumatica_sync_items (
            approval_id, revision_no, status, batch_nbr, line_nbrs,
            sent_payload, last_attempt_at, sent_at, last_error, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(approval_id) DO UPDATE SET
            revision_no = excluded.revision_no,
            status = excluded.status,
            batch_nbr = COALESCE(excluded.batch_nbr, acumatica_sync_items.batch_nbr),
            line_nbrs = COALESCE(excluded.line_nbrs, acumatica_sync_items.line_nbrs),
            sent_payload = COALESCE(excluded.sent_payload, acumatica_sync_items.sent_payload),
            last_attempt_at = excluded.last_attempt_at,
            sent_at = CASE WHEN excluded.status = 'sent' THEN excluded.sent_at ELSE acumatica_sync_items.sent_at END,
            last_error = excluded.last_error,
            updated_at = excluded.updated_at
        """,
        (
            approval_id,
            int(revision_no or 1),
            status,
            batch_nbr,
            json.dumps(line_nbrs) if line_nbrs is not None else None,
            json.dumps(sent_payload, default=str) if sent_payload is not None else None,
            now,
            now if status == "sent" else None,
            error,
            now,
        ),
    )
    conn.commit()
    conn.close()


def save_acumatica_batch(labor_date, area, batch_nbr, description, status="On Hold", is_supplemental=False):
    ensure_db()
    now = _utc_now_iso()
    conn = _connect()
    conn.execute(
        """
        INSERT INTO acumatica_batches (
            labor_date, area, batch_nbr, description, status, is_supplemental, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(batch_nbr) DO UPDATE SET status = excluded.status, updated_at = excluded.updated_at
        """,
        (labor_date, area, batch_nbr, description, status, int(bool(is_supplemental)), now, now),
    )
    conn.commit()
    conn.close()


def find_latest_acumatica_batch(labor_date, area):
    ensure_db()
    conn = _connect()
    row = conn.execute(
        """
        SELECT * FROM acumatica_batches
        WHERE labor_date = ? AND area = ?
        ORDER BY id DESC LIMIT 1
        """,
        (labor_date, area),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def update_acumatica_batch_status(batch_nbr, status):
    ensure_db()
    conn = _connect()
    conn.execute(
        "UPDATE acumatica_batches SET status = ?, updated_at = ? WHERE batch_nbr = ?",
        (status, _utc_now_iso(), batch_nbr),
    )
    conn.commit()
    conn.close()


def begin_acumatica_sync_run(trigger_name, actor=None, stale_minutes=30):
    ensure_db()
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        active = conn.execute(
            """
            SELECT id FROM acumatica_sync_runs
            WHERE status = 'running'
              AND julianday(started_at) >= julianday('now', ?)
            ORDER BY id DESC LIMIT 1
            """,
            (f"-{int(stale_minutes)} minutes",),
        ).fetchone()
        if active:
            conn.rollback()
            conn.close()
            return None
        conn.execute(
            "UPDATE acumatica_sync_runs SET status = 'abandoned', completed_at = ? WHERE status = 'running'",
            (_utc_now_iso(),),
        )
        cursor = conn.execute(
            """
            INSERT INTO acumatica_sync_runs (
                trigger_name, actor_emp_id, actor_name, started_at, status
            ) VALUES (?, ?, ?, ?, 'running')
            """,
            (
                trigger_name,
                str((actor or {}).get("emp_id") or "").strip(),
                (actor or {}).get("full_name") or "",
                _utc_now_iso(),
            ),
        )
        run_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return run_id
    except Exception:
        conn.rollback()
        conn.close()
        raise


def finish_acumatica_sync_run(run_id, status, summary):
    ensure_db()
    conn = _connect()
    conn.execute(
        """
        UPDATE acumatica_sync_runs
        SET completed_at = ?, status = ?, summary = ?
        WHERE id = ?
        """,
        (_utc_now_iso(), status, json.dumps(summary, default=str), run_id),
    )
    conn.commit()
    conn.close()


def get_acumatica_sync_summary(reviewed_after=None):
    ensure_db()
    conn = _connect()
    rows = conn.execute(
        "SELECT status, COUNT(*) AS count FROM acumatica_sync_items GROUP BY status"
    ).fetchall()
    unsent_rows = conn.execute(
        """
        SELECT q.payload
        FROM approval_queue q
        LEFT JOIN acumatica_sync_items s ON s.approval_id = q.id
        WHERE q.approval_status = 'approved'
          AND s.approval_id IS NULL
          AND (? IS NULL OR q.reviewed_at >= ?)
        """
        ,
        (reviewed_after, reviewed_after),
    ).fetchall()
    latest = conn.execute(
        "SELECT * FROM acumatica_sync_runs ORDER BY id DESC LIMIT 1"
    ).fetchone()
    conn.close()
    counts = {row["status"]: row["count"] for row in rows}
    unsent = 0
    for row in unsent_rows:
        try:
            payload = json.loads(row["payload"] or "{}")
        except (TypeError, json.JSONDecodeError):
            continue
        if payload.get("acumatica_labor_transaction") or payload.get("acumatica_labor_transactions"):
            unsent += 1
    counts["not_sent"] = unsent
    return {"counts": counts, "latest_run": dict(latest) if latest else None}


def start_break_event(entry_id, break_type, comment=None, machine_no=None, emp_id=None, employee_name=None, started_at=None):
    ensure_db()
    started = started_at or _utc_now_iso()
    conn = _connect()
    conn.execute(
        """
        INSERT INTO break_events (
            entry_id, emp_id, employee_name, machine_no, break_type, comment, started_at, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(entry_id or "").strip(),
            str(emp_id or "").strip(),
            employee_name or "",
            str(machine_no or "").strip(),
            str(break_type or "").strip(),
            comment or "",
            started,
            _utc_now_iso(),
        ),
    )
    conn.commit()
    conn.close()


def finish_break_event(entry_id, ended_at=None, duration_minutes=None):
    ensure_db()
    ended = ended_at or _utc_now_iso()
    conn = _connect()
    row = conn.execute(
        """
        SELECT id FROM break_events
        WHERE entry_id = ? AND ended_at IS NULL
        ORDER BY started_at DESC
        LIMIT 1
        """,
        (str(entry_id or "").strip(),),
    ).fetchone()
    if not row:
        conn.close()
        return False
    conn.execute(
        """
        UPDATE break_events
        SET ended_at = ?, duration_minutes = ?
        WHERE id = ?
        """,
        (ended, duration_minutes, row["id"]),
    )
    conn.commit()
    conn.close()
    return True


def get_active_break_event(entry_id):
    ensure_db()
    conn = _connect()
    row = conn.execute(
        """
        SELECT * FROM break_events
        WHERE entry_id = ? AND ended_at IS NULL
        ORDER BY started_at DESC
        LIMIT 1
        """,
        (str(entry_id or "").strip(),),
    ).fetchone()
    conn.close()
    return dict(row) if row else None
