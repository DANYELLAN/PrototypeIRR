"""Durable, exactly-once downtime sessions for the regular time-entry screen."""
import json
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

from time_entry_app.bridge import cnc_time_backend as backend

store = __import__('local_store')
SCHEMA = '''
CREATE TABLE IF NOT EXISTS timed_downtime_sessions (
 id TEXT PRIMARY KEY, entry_id TEXT NOT NULL, emp_id TEXT NOT NULL,
 machine_no TEXT NOT NULL, state TEXT NOT NULL, ended_at TEXT,
 approval_id INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_timed_downtime
 ON timed_downtime_sessions(entry_id) WHERE ended_at IS NULL;
'''


def _db():
    store.ensure_db()
    connection = store._connect()
    connection.executescript(SCHEMA)
    return connection


def active_downtime(entry_id):
    connection = _db()
    try:
        row = connection.execute('SELECT state FROM timed_downtime_sessions WHERE entry_id=? AND ended_at IS NULL', (str(entry_id),)).fetchone()
        return json.loads(row['state']) if row else None
    finally:
        connection.close()


def require_no_downtime(entry_id):
    if active_downtime(entry_id):
        raise ValueError('End the active downtime before changing or stopping production.')


def start_downtime(session, entry_id, reason, comments=''):
    entry = backend._get_startstop_by_id(entry_id)
    backend._require_break_owner(entry, session['employee']['emp_id'])
    if str(entry.get('machine_no')) != str(session['machine_no']):
        raise ValueError('This entry belongs to another station.')
    if entry.get('status') != 'In Progress':
        raise ValueError('Resume production before starting downtime.')
    code = backend._downtime_reason_code(reason)
    started = backend._utc_now()
    state = {'id': str(uuid.uuid4()), 'entry_id': str(entry['sp_id']),
             'start': started.isoformat(), 'reason': code, 'comments': str(comments or '').strip(),
             'reason_label': next((item['label'] for item in backend.DOWNTIME_REASONS if item['code'] == code), code),
             'production_number': entry['production_number'], 'operation_id': entry['operation_id'],
             'entry': entry, 'employee': session['employee'], 'shift_id': entry.get('shift') or session['shift_id']}
    connection = _db()
    try:
        connection.execute('BEGIN IMMEDIATE')
        prior = connection.execute('SELECT state FROM timed_downtime_sessions WHERE entry_id=? AND ended_at IS NULL', (state['entry_id'],)).fetchone()
        if prior:
            raise ValueError('Downtime is already being logged for this entry.')
        connection.execute('INSERT INTO timed_downtime_sessions(id,entry_id,emp_id,machine_no,state) VALUES(?,?,?,?,?)',
                           (state['id'], state['entry_id'], str(session['employee']['emp_id']).lstrip('0'), str(session['machine_no']), json.dumps(state)))
        connection.commit()
        return state
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def end_downtime(session, downtime_id):
    connection = _db()
    try:
        connection.execute('BEGIN IMMEDIATE')
        row = connection.execute('SELECT * FROM timed_downtime_sessions WHERE id=?', (str(downtime_id),)).fetchone()
        if not row or row['emp_id'] != str(session['employee']['emp_id']).lstrip('0') or row['machine_no'] != str(session['machine_no']):
            raise ValueError('This downtime session does not belong to your operator and station.')
        if row['ended_at']:
            return {'approval_id': row['approval_id'], 'already_submitted': True}
        state = json.loads(row['state'])
        end = backend._utc_now()
        start = datetime.fromisoformat(state['start'])
        seconds = (end - start).total_seconds()
        if seconds < 0:
            raise ValueError('The server clock changed. Keep downtime open and contact a supervisor.')
        minutes = int(seconds // 60)
        entry = state['entry']
        employee = state['employee']
        fields = {
            'Title': employee['full_name'], 'LaborDate': start.astimezone(ZoneInfo('America/Chicago')).date().isoformat(),
            'Status': 'Submitted', 'LaborType': 'Direct', 'OrderType': entry.get('order_type') or 'EN',
            'ProductionNo': state['production_number'], 'OperationID': state['operation_id'],
            'InventoryID': entry.get('inventory_id'), 'Description': entry.get('description'),
            'OperationDescription': entry.get('operation_description'), 'Shift': str(state['shift_id']),
            'EmpID': int(employee['emp_id']), 'EmployeeID': str(employee['emp_id']).zfill(6),
            'OperatorsName': employee['full_name'], 'MachineNo': row['machine_no'],
            'DetailsType': 'DT', 'DetailsTypeII': state['reason'], 'Quantity': 1,
            'TotalMinutes': minutes, 'Total': backend._hhmm_from_minutes(minutes),
            'Start': state['start'], 'End': end.isoformat(), 'Year': start.astimezone(ZoneInfo('America/Chicago')).year,
            'TranDescription': state['reason'] + (' - ' + state['comments'] if state['comments'] else ''),
        }
        payload = {'employee': employee, 'fields': fields, 'active_direct_entry_id': row['entry_id'],
                   'timed_downtime_id': state['id'], 'downtime_seconds': seconds}
        backend._attach_acumatica_payload(payload, employee, fields, state['reason'])
        cursor = connection.execute('''INSERT INTO approval_queue
            (record_type,machine_no,emp_id,employee_name,payload,approval_status,submitted_at,active_direct_entry_id)
            VALUES('misc_time',?,?,?,?,'pending',?,?)''',
            (row['machine_no'], str(employee['emp_id']), employee['full_name'], json.dumps(payload), end.isoformat(), row['entry_id']))
        approval_id = cursor.lastrowid
        connection.execute('UPDATE timed_downtime_sessions SET ended_at=?,approval_id=? WHERE id=?', (end.isoformat(), approval_id, state['id']))
        connection.commit()
        return {'approval_id': approval_id, 'minutes': minutes}
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
