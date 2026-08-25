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
    conn.commit()
    conn.close()
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
            machines.append({"machine_no": machine_no, "email": email, "label": f"Machine {machine_no}"})
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
        "SELECT machine_no, email, label FROM machines ORDER BY CAST(machine_no AS INTEGER)"
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
