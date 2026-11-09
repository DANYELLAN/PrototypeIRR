"""Atomic, idempotent tablet time events. No external writes occur on receipt."""
import hashlib
import json
import math
import uuid
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from time_entry_app.bridge import cnc_time_backend as backend

# Backend imports this module by its historical top-level name.
store = __import__('local_store')

SCHEMA = '''
CREATE TABLE IF NOT EXISTS tablet_time_events (
 id TEXT PRIMARY KEY, subject TEXT NOT NULL, body_hash TEXT NOT NULL,
 result TEXT NOT NULL, received_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS tablet_time_timers (
 id TEXT PRIMARY KEY, subject TEXT NOT NULL, state TEXT NOT NULL,
 last_event_at TEXT NOT NULL, closed INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS tablet_one_active_timer
 ON tablet_time_timers(subject) WHERE closed=0;
'''

def subject_for(session):
    return json.dumps([str(session['employee']['emp_id']).lstrip('0') or '0',
                       str(session['machine_no']), str(session['shift_id'])], separators=(',', ':'))

def _db():
    store.ensure_db()
    conn = store._connect()
    conn.executescript(SCHEMA)
    return conn

def active_timer(session):
    conn = _db()
    try:
        row = conn.execute('SELECT state FROM tablet_time_timers WHERE subject=? AND closed=0', (subject_for(session),)).fetchone()
        return json.loads(row['state']) if row else None
    finally:
        conn.close()

def _timestamp(value):
    dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if dt.tzinfo is None or dt > datetime.now(timezone.utc) + timedelta(minutes=5):
        raise ValueError('Check the tablet date and time before syncing this entry.')
    return dt

def _number(value, name, maximum):
    try:
        number = float(value)
    except (ValueError, TypeError):
        raise ValueError(f'{name} must be a number.')
    if not math.isfinite(number) or number < 0 or number > maximum:
        raise ValueError(f'{name} is outside the allowed range.')
    return number

def relief_operator(adp_number, original_adp):
    number=str(adp_number or '').strip()
    if not number or not number.isdigit():
        raise ValueError('Enter the relief operator ADP number.')
    employee=backend.lookup_employee_by_adp(number)
    if not employee:
        raise ValueError('The relief ADP number must belong to an active Benoit Ennis employee.')
    if str(employee['emp_id']).lstrip('0') == str(original_adp).lstrip('0'):
        raise ValueError('The relief operator must be different from the operator taking the break.')
    return {'emp_id':str(employee['emp_id']), 'full_name':employee['full_name']}


def apply_event(session, event):
    subject = subject_for(session)
    event_id = str(uuid.UUID(str(event.get('id'))))
    body_hash = hashlib.sha256(json.dumps(event, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    conn = _db()
    try:
        conn.execute('BEGIN IMMEDIATE')
        prior = conn.execute('SELECT * FROM tablet_time_events WHERE id=?', (event_id,)).fetchone()
        if prior:
            if prior['subject'] != subject or prior['body_hash'] != body_hash:
                raise ValueError('This event ID was already used for different data or another operator.')
            return json.loads(prior['result'])
        at = _timestamp(event.get('at'))
        kind = event.get('kind')
        data = event.get('data') or {}
        timer = None
        row = None
        timer_id = data.get('timer_id')
        if timer_id:
            row = conn.execute('SELECT * FROM tablet_time_timers WHERE id=? AND subject=?', (str(timer_id), subject)).fetchone()
            if row:
                timer = json.loads(row['state'])
                if row['closed'] or at < _timestamp(row['last_event_at']):
                    raise ValueError('Timer events are out of order or this timer is already stopped.')
            elif kind == 'misc':
                raise ValueError('The preceding timer start has not synced yet.')
        result = {'id': event_id, 'approval_id': None}
        if kind in {'start', 'manual', 'misc'}:
            # The live server resolves employee and WO metadata, never trusts the tablet's copies.
            context = backend.get_dashboard_context(session['employee']['emp_id'], session.get('user_email', ''))
            operation = next((item for item in context['operations']
                              if item['production_number'] == data.get('production_number')
                              and item['operation_id'] == data.get('operation_id')), None)
            if not operation:
                raise ValueError('This work-order operation is no longer available. The tablet copy has been kept for review.')
            detail = str(data.get('detail_type') or '')
            allowed_details = {str(item['title']) for item in context['details_step_two']}
            if kind != 'misc' and detail not in allowed_details:
                raise ValueError('Select a valid detail type.')
            reason = str(data.get('detail_type_ii') or '')
            if kind == 'misc' and reason not in {str(item['code']) for item in context['downtime_reasons']}:
                raise ValueError('Select a valid downtime reason.')
            fields = {
                **backend._operation_fields(operation, data['production_number'], data['operation_id']),
                'EmpID': int(session['employee']['emp_id']), 'EmployeeID': str(session['employee']['emp_id']).zfill(6),
                'OperatorsName': session['employee']['full_name'], 'Shift': str(session['shift_id']),
                'MachineNo': session['machine_no'], 'LaborType': 'Direct', 'Status': 'Submitted',
                'LaborDate': at.astimezone(ZoneInfo('America/Chicago')).date().isoformat(),
                'Year': at.astimezone(ZoneInfo('America/Chicago')).year,
                'DetailsType': 'DT' if kind == 'misc' else backend._detail_code(detail)[0],
                'TranDescription': str(data.get('comments') or operation.get('operation_description') or operation.get('description') or ''),
            }
            if kind == 'start':
                if context.get('active_entry'):
                    raise ValueError('An earlier server timer is still running. Stop it in the original time screen before starting a tablet timer.')
                timer_id = str(uuid.UUID(str(data.get('timer_id'))))
                timer = {'id': timer_id, 'fields': fields, 'start': at.isoformat(), 'status': 'In Progress',
                         'break_minutes': 0, 'downtime_minutes': 0, 'breaks': [], 'last_event_at': at.isoformat()}
                conn.execute('INSERT INTO tablet_time_timers(id,subject,state,last_event_at) VALUES(?,?,?,?)',
                             (timer_id, subject, json.dumps(timer), at.isoformat()))
            else:
                minutes = _number(data.get('hours', 0), 'Hours', 16) * 60 + _number(data.get('minutes', 0), 'Minutes', 59)
                if minutes <= 0 or minutes > 960 or int(minutes) != minutes:
                    raise ValueError('Enter between 1 and 960 whole minutes.')
                qty = 1 if kind == 'misc' else _number(data.get('quantity', 0), 'Quantity', 1000000)
                fields.update(TotalMinutes=int(minutes), Quantity=qty)
                if kind == 'misc':
                    fields['DetailsTypeII'] = reason
                    fields['TranDescription'] = reason + (' - ' + str(data['comments']) if data.get('comments') else '')
                    if timer:
                        if timer['fields']['ProductionNo'] != fields['ProductionNo'] or timer['fields']['OperationID'] != fields['OperationID']:
                            raise ValueError('Downtime must use the active timer work order and operation.')
                        timer['downtime_minutes'] += int(minutes)
                result['approval_id'] = _approval(conn, session, event, fields, kind + '_time', reason, timer if kind == 'misc' else None)
        elif kind in {'pause', 'relief_arrived', 'resume', 'stop'}:
            if not timer:
                raise ValueError('The preceding timer start has not synced yet.')
            if kind == 'pause':
                if timer['status'] != 'In Progress':
                    raise ValueError('This timer is already paused.')
                break_type = data.get('break_type')
                if break_type not in {'Lunch', 'No Relief', 'With Relief'}:
                    raise ValueError('Select a valid break type.')
                relief=relief_operator(data.get('relief_adp'),session['employee']['emp_id']) if break_type=='With Relief' else None
                timer['breaks'].append({'type': break_type, 'start': at.isoformat(), 'comment': str(data.get('comments') or ''),
                    'original_adp':str(session['employee']['emp_id']), 'original_name':session['employee']['full_name'],
                    **({'relief_adp':relief['emp_id'],'relief_name':relief['full_name']} if relief else {})})
                timer['status'] = 'Paused'
            elif kind == 'relief_arrived':
                if timer['status']!='Paused' or not timer['breaks'] or timer['breaks'][-1]['type']!='No Relief':
                    raise ValueError('Relief Arrived is available only during a break without relief.')
                relief=relief_operator(data.get('relief_adp'),session['employee']['emp_id'])
                pause=timer['breaks'][-1]
                duration=max(int((at-_timestamp(pause['start'])).total_seconds()//60),0)
                pause.update(end=at.isoformat(),minutes=duration,end_reason='relief_arrived')
                timer['break_minutes']+=duration
                timer['breaks'].append({'type':'With Relief','start':at.isoformat(),'break_started_at':pause.get('break_started_at',pause['start']),
                    'comment':str(data.get('comments') or ''),'relief_adp':relief['emp_id'],'relief_name':relief['full_name'],
                    'original_adp':str(session['employee']['emp_id']),'original_name':session['employee']['full_name']})
            else:
                if kind == 'resume' and timer['status'] != 'Paused':
                    raise ValueError('This timer is not paused.')
                if timer['status'] == 'Paused':
                    pause = timer['breaks'][-1]
                    duration = max(int((at - _timestamp(pause['start'])).total_seconds() // 60), 0)
                    pause.update(end=at.isoformat(), minutes=duration, end_reason='operator_returned' if kind=='resume' else 'entry_stopped', return_adp=str(session['employee']['emp_id']) if kind=='resume' else None, return_name=session['employee']['full_name'] if kind=='resume' else None)
                    if pause['type'] in {'Lunch', 'No Relief'}:
                        timer['break_minutes'] += duration
                timer['status'] = 'In Progress' if kind == 'resume' else 'Submitted'
                if kind == 'stop':
                    elapsed = int((at - _timestamp(timer['start'])).total_seconds() // 60)
                    if elapsed < 0 or elapsed > 4320:
                        raise ValueError('The timer duration needs supervisor review; its tablet data has been kept.')
                    worked = max(elapsed - timer['break_minutes'] - timer['downtime_minutes'], 0)
                    fields = {**timer['fields'], 'Start': timer['start'], 'End': at.isoformat(),
                              'BreakMinutes': timer['break_minutes'], 'TotalMinutes': worked,
                              'Quantity': _number(data.get('quantity', 0), 'Quantity', 1000000)}
                    result['approval_id'] = _approval(conn, session, event, fields, 'manual_time', timer=timer)
        else:
            raise ValueError('Unsupported tablet action.')
        if timer:
            timer['last_event_at'] = at.isoformat()
            conn.execute('UPDATE tablet_time_timers SET state=?,last_event_at=?,closed=? WHERE id=? AND subject=?',
                         (json.dumps(timer), at.isoformat(), int(timer['status'] == 'Submitted'), timer['id'], subject))
        result['timer'] = timer
        conn.execute('INSERT INTO tablet_time_events VALUES(?,?,?,?,?)',
                     (event_id, subject, body_hash, json.dumps(result), datetime.now(timezone.utc).isoformat()))
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def _approval(conn, session, event, fields, record_type, reason='', timer=None):
    minutes = fields['TotalMinutes']
    fields['Total'] = backend._hhmm_from_minutes(minutes)
    fields['Average'] = round(fields['Quantity'] / (minutes / 60), 2) if minutes else 0
    payload = {'employee': session['employee'], 'fields': fields,
               'tablet_event_id': event['id'], 'tablet_recorded_at': event['at'],
               'tablet_timer': timer, 'source': 'tablet-offline'}
    backend._attach_acumatica_payload(payload, session['employee'], fields, reason)
    cursor = conn.execute('''INSERT INTO approval_queue
        (record_type,machine_no,emp_id,employee_name,payload,approval_status,submitted_at)
        VALUES(?,?,?,?,?,'pending',?)''',
        (record_type, session['machine_no'], str(session['employee']['emp_id']), session['employee']['full_name'],
         json.dumps(payload), datetime.now(timezone.utc).isoformat()))
    return cursor.lastrowid
