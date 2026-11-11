"""Atomic local submission/start handoff from Set-Up to the next detail type."""
import json
import math
from datetime import datetime
from zoneinfo import ZoneInfo

from time_entry_app.bridge import cnc_time_backend as backend
from time_entry_app.bridge.timed_downtime import active_downtime
from time_entry_app.bridge.entry_comments import saved_comment

store = __import__('local_store')
SCHEMA = '''
CREATE TABLE IF NOT EXISTS setup_transitions (
 entry_id TEXT PRIMARY KEY, emp_id TEXT NOT NULL, machine_no TEXT NOT NULL,
 result TEXT NOT NULL, completed_fields TEXT NOT NULL
);
'''


def is_setup(detail):
    return ''.join(char for char in str(detail or '') if char.isalnum()).casefold() == 'setup'


def _db():
    store.ensure_db()
    connection = store._connect()
    connection.executescript(SCHEMA)
    return connection


def project_completed_setups(entries):
    """Keep closed Set-Up entries closed while their remote update is pending."""
    connection = _db()
    try:
        completed = {row['entry_id']: json.loads(row['completed_fields']) for row in connection.execute('SELECT entry_id,completed_fields FROM setup_transitions')}
    finally:
        connection.close()
    result = []
    for entry in entries:
        fields = completed.get(str(entry['sp_id']))
        if fields:
            entry = {**entry, 'status': 'Submitted', 'end': fields['End'],
                     'total_minutes': fields['TotalMinutes'], 'total': fields['Total'], 'submitted': True}
        result.append(entry)
    from time_entry_app.bridge.active_timing import project_timing
    return project_timing(result)


def end_setup(session, entry_id, detail_type, quantity=None):
    owner = str(session['employee']['emp_id']).lstrip('0') or '0'
    machine = str(session['machine_no'])
    connection = _db()
    try:
        # The stable source entry ID makes retries, including lost responses, safe.
        prior = connection.execute('SELECT * FROM setup_transitions WHERE entry_id=?', (str(entry_id),)).fetchone()
        if prior:
            if prior['emp_id'] != owner or prior['machine_no'] != machine:
                raise ValueError('This Set-Up entry belongs to another operator or station.')
            return json.loads(prior['result'])
    finally:
        connection.close()
    entry = backend._get_startstop_by_id(entry_id)
    backend._require_break_owner(entry, session['employee']['emp_id'])
    if str(entry.get('machine_no')) != machine:
        raise ValueError('This Set-Up entry belongs to another station.')
    if not is_setup(entry.get('details_type')) or entry.get('status') != 'In Progress':
        raise ValueError('End Set-Up is available only for a running Set-Up entry. Resume any break first.')
    if active_downtime(entry['sp_id']):
        raise ValueError('End downtime before ending Set-Up.')
    context = backend.get_dashboard_context(session['employee']['emp_id'], session.get('user_email', ''))
    choices = {str(item['title']) for item in context['details_step_two']}
    if detail_type not in choices:
        raise ValueError('Select a valid detail type for the next entry.')
    try:
        qty = float(entry.get('quantity') or 0) if quantity is None else float(quantity)
    except (TypeError, ValueError):
        raise ValueError('Enter a valid Set-Up quantity.')
    if not math.isfinite(qty) or qty < 0 or qty > 1000000 or not qty.is_integer():
        raise ValueError('Enter a whole Set-Up quantity between 0 and 1000000.')
    active = context.get('active_entry')
    if active and str(active['sp_id']) != str(entry['sp_id']):
        raise ValueError('The active entry changed. Refresh before ending Set-Up.')
    end = backend._utc_now()
    elapsed = int((end - datetime.fromisoformat(str(entry['start']).replace('Z', '+00:00'))).total_seconds() // 60)
    if elapsed < 0 or elapsed > 4320:
        raise ValueError('The Set-Up duration needs supervisor review before continuing.')
    downtime_minutes = backend._active_downtime_minutes(entry)
    worked = max(elapsed - int(entry.get('break_minutes') or 0) - downtime_minutes, 0)
    employee = session['employee']
    fields = {
        'Title': entry['production_number'], 'LaborDate': entry['labor_date'], 'Status': 'Submitted',
        'OrderType': entry.get('order_type') or 'EN', 'LaborType': 'Direct',
        'ProductionNo': entry['production_number'], 'OperationID': entry['operation_id'],
        'InventoryID': entry.get('inventory_id'), 'Description': entry.get('description'),
        'OperationDescription': entry.get('operation_description'), 'DetailsType': entry['details_type'],
        'MachineNo': machine, 'Shift': entry['shift'], 'EmpID': int(employee['emp_id']),
        'EmployeeID': str(employee['emp_id']).zfill(6), 'OperatorsName': employee['full_name'],
        'Start': entry['start'], 'End': end.isoformat(), 'LunchStart': entry.get('lunch_start'),
        'LunchStop': entry.get('lunch_stop'), 'BreakMinutes': entry.get('break_minutes') or 0,
        'TotalMinutes': worked, 'Total': backend._hhmm_from_minutes(worked),
        'Quantity': int(qty), 'Year': end.astimezone(ZoneInfo('America/Chicago')).year,
        'TranDescription': saved_comment({**entry, 'entry_list': 'startstop'}),
        'StartStopID': entry.get('id') or 0,
    }
    fields['Average'] = backend._recalculate_average(fields['Quantity'], worked)
    approval = {'employee': employee, 'entry_id': str(entry['sp_id']), 'fields': fields, 'active_downtime_minutes': downtime_minutes, 'source': 'setup-handoff'}
    backend._attach_acumatica_payload(approval, employee, fields)
    completed = {key: fields[key] for key in ('End', 'Status', 'BreakMinutes', 'TotalMinutes', 'Total', 'Quantity', 'Average')}
    completed['Submitted'] = True
    next_fields = {**fields, 'Status': 'In Progress', 'Submitted': False, 'DetailsType': backend._detail_code(detail_type)[0],
                   'Start': end.isoformat(), 'End': None, 'LunchStart': None, 'LunchStop': None,
                   'BreakMinutes': 0, 'TotalMinutes': 0, 'Total': '00:00', 'Quantity': 0, 'Average': 0,
                   'LaborDate': end.astimezone(ZoneInfo('America/Chicago')).date().isoformat(),
                   'TranDescription': ''}
    next_fields.pop('StartStopID', None)
    connection = _db()
    try:
        connection.execute('BEGIN IMMEDIATE')
        prior = connection.execute('SELECT * FROM setup_transitions WHERE entry_id=?', (str(entry['sp_id']),)).fetchone()
        if prior:
            if prior['emp_id'] != owner or prior['machine_no'] != machine:
                raise ValueError('This Set-Up entry belongs to another operator or station.')
            return json.loads(prior['result'])
        def queue(kind, payload):
            return connection.execute('''INSERT INTO time_entries
                (record_type,machine_no,emp_id,employee_name,payload,created_at,sync_status,source)
                VALUES(?,?,?,?,?,?,'pending','offline-first')''',
                (kind, machine, str(employee['emp_id']), employee['full_name'], json.dumps(payload), end.isoformat())).lastrowid
        queue('complete_startstop', {'entry_id': str(entry['sp_id']), 'item_id': str(entry['sp_id']), 'fields': completed})
        approval_id = connection.execute('''INSERT INTO approval_queue
            (record_type,machine_no,emp_id,employee_name,payload,approval_status,submitted_at)
            VALUES('stop_time_entry',?,?,?,?,'pending',?)''',
            (machine, str(employee['emp_id']), employee['full_name'], json.dumps(approval), end.isoformat())).lastrowid
        connection.execute('INSERT OR REPLACE INTO entry_comment_overrides VALUES(?,?,?)',
                           ('timeentry', f'approval:{approval_id}', fields['TranDescription']))
        next_id = queue('startstop', {'employee': employee, 'shift_id': entry['shift'], 'machine_no': machine,
            'production_number': entry['production_number'], 'operation_id': entry['operation_id'],
            'detail_type': next_fields['DetailsType'], 'fields': next_fields})
        result = {'approval_id': approval_id, 'next_entry_id': f'local:{next_id}', 'detail_type': next_fields['DetailsType']}
        connection.execute('INSERT INTO setup_transitions VALUES(?,?,?,?,?)',
            (str(entry['sp_id']), owner, machine, json.dumps(result), json.dumps(completed)))
        connection.commit()
        return result
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
