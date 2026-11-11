"""Audited start corrections and idempotent missed-lunch deductions."""
import json
import uuid
import re
from datetime import datetime, timedelta, timezone, date
from zoneinfo import ZoneInfo

from time_entry_app.bridge import cnc_time_backend as backend
store = __import__('local_store')
CENTRAL = ZoneInfo('America/Chicago')
SCHEMA = '''
CREATE TABLE IF NOT EXISTS active_timing_state (entry_id TEXT PRIMARY KEY, fields TEXT NOT NULL, queue_id INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS active_timing_events (
 entry_id TEXT NOT NULL, kind TEXT NOT NULL, event_key TEXT NOT NULL,
 details TEXT NOT NULL, recorded_at TEXT NOT NULL,
 PRIMARY KEY(entry_id,kind,event_key)
);
'''


def _db():
    store.ensure_db()
    connection = store._connect()
    connection.executescript(SCHEMA)
    if 'queue_id' not in {row['name'] for row in connection.execute('PRAGMA table_info(active_timing_state)')}:
        connection.execute('ALTER TABLE active_timing_state ADD COLUMN queue_id INTEGER NOT NULL DEFAULT 0')
        connection.commit()
    return connection


def central_start(value, now):
    try:
        local = datetime.fromisoformat(str(value))
        if local.tzinfo is not None:
            raise ValueError()
        candidates = {local.replace(tzinfo=CENTRAL, fold=fold).astimezone(timezone.utc)
                      for fold in (0, 1)
                      if local.replace(tzinfo=CENTRAL, fold=fold).astimezone(timezone.utc).astimezone(CENTRAL).replace(tzinfo=None) == local}
    except (TypeError, ValueError):
        raise ValueError('Enter a valid start date and time in Central Time.')
    if len(candidates) != 1:
        raise ValueError('That Central Time is missing or ambiguous because of the clock change. Contact a supervisor.')
    start = candidates.pop()
    if start > now or now - start > timedelta(hours=72):
        raise ValueError('Start time must be within the past 72 hours and cannot be in the future.')
    return start


def lunch_interval(shift, now, day=None):
    night = str(shift).strip().lower() in {'41', 'night', 'night shift'}
    local = now.astimezone(CENTRAL)
    lunch_date = date.fromisoformat(str(day)) if day else local.date() - timedelta(days=int(night and local.hour < 12))
    start = datetime.combine(lunch_date, datetime.min.time()).replace(hour=22 if night else 11, minute=0 if night else 30, tzinfo=CENTRAL)
    return start.astimezone(timezone.utc), (start + timedelta(hours=1)).astimezone(timezone.utc), lunch_date.isoformat()


def start_on_existing_date(value, existing, now, period=None):
    day = datetime.fromisoformat(str(existing).replace('Z', '+00:00')).astimezone(CENTRAL).date().isoformat()
    value = str(value or '').strip()
    if period is not None:
        match = re.fullmatch(r'(0?[1-9]|1[0-2]):?([0-5][0-9])', value)
        if not match or period not in {'AM', 'PM'}:
            raise ValueError('Enter a valid time and select AM or PM.')
        value = f'{int(match[1]) % 12 + (12 if period == "PM" else 0):02d}:{match[2]}'
    if re.fullmatch(r'\d{4}', value):
        value = f'{value[:2]}:{value[2:]}'
    if 'T' in value:
        if value.split('T')[0] != day:
            raise ValueError('Only the start time can be changed. The entry date must stay the same.')
    elif re.fullmatch(r'\d{2}:\d{2}', value):
        value = f'{day}T{value}'
    else:
        raise ValueError('Enter a valid start time in Central Time.')
    return central_start(value, now)


def project_timing(entries):
    if not entries:
        return entries
    connection = _db()
    try:
        states = {row['entry_id']: dict(row) for row in connection.execute('SELECT * FROM active_timing_state')}
        if states:
            for row in connection.execute('SELECT id,payload FROM time_entries WHERE id>? ORDER BY id', (min(state['queue_id'] for state in states.values()),)):
                payload = json.loads(row['payload'])
                key = str(payload.get('item_id') or payload.get('entry_id') or '')
                if key in states and row['id'] > states[key]['queue_id']:
                    fields = json.loads(states[key]['fields'])
                    fields.update({key: value for key, value in (payload.get('fields') or {}).items() if key in {'Start', 'LaborDate', 'BreakMinutes'}})
                    states[key]['fields'] = json.dumps(fields)
    finally:
        connection.close()
    result = []
    for entry in entries:
        fields = json.loads(states[str(entry['sp_id'])]['fields']) if str(entry['sp_id']) in states else {}
        entry = dict(entry)
        for source, target in [('Start', 'start'), ('LaborDate', 'labor_date'), ('BreakMinutes', 'break_minutes')]:
            if source in fields:
                entry[target] = fields[source]
        result.append(entry)
    return result


def remember_local_correction(entry_id, fields):
    """Keep edits to an original queued entry ordered after its time adjustments."""
    connection = _db()
    try:
        state = connection.execute('SELECT fields FROM active_timing_state WHERE entry_id=?', (str(entry_id),)).fetchone()
        if state and 'BreakMinutes' in fields:
            connection.execute("INSERT INTO time_entries(record_type,payload,created_at,sync_status,source) VALUES('active_time_correction',?,?,'pending','offline-first')",
                (json.dumps({'entry_id': str(entry_id), 'item_id': str(entry_id), 'fields': fields}), backend._utc_now().isoformat()))
            connection.commit()
    finally:
        connection.close()


def _intervals(connection, entry_id, now):
    intervals = []
    for row in connection.execute('SELECT * FROM break_events WHERE entry_id=?', (str(entry_id),)):
        intervals.append((datetime.fromisoformat(row['started_at'].replace('Z', '+00:00')),
                          datetime.fromisoformat(row['ended_at'].replace('Z', '+00:00')) if row['ended_at'] else now,
                          row['break_type'] != 'With Relief', row['break_type']))
    if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='timed_downtime_sessions'").fetchone():
        for row in connection.execute('SELECT state,ended_at FROM timed_downtime_sessions WHERE entry_id=?', (str(entry_id),)):
            intervals.append((datetime.fromisoformat(json.loads(row['state'])['start']),
                              datetime.fromisoformat(row['ended_at']) if row['ended_at'] else now, True, 'Downtime'))
    return intervals


def adjust_active(session, entry_id, kind, start_time=None, lunch_day=None, start_period=None):
    entry = backend._get_startstop_by_id(entry_id)
    backend._require_break_owner(entry, session['employee']['emp_id'])
    if str(entry.get('machine_no')) != str(session['machine_no']):
        raise ValueError('This entry belongs to another station.')
    if entry.get('status') not in {'In Progress', 'Paused'}:
        raise ValueError('Only the currently active entry can be adjusted.')
    from time_entry_app.bridge.timed_downtime import require_no_downtime
    require_no_downtime(entry['sp_id'])
    if kind not in {'correct_start', 'log_lunch'}:
        raise ValueError('Select a valid time adjustment.')
    now = backend._utc_now()
    corrected = start_on_existing_date(start_time, entry['start'], now, start_period) if kind == 'correct_start' else None
    lunch = lunch_interval(entry.get('shift') or session['shift_id'], now, lunch_day) if kind == 'log_lunch' else None
    connection = _db()
    try:
        connection.execute('BEGIN IMMEDIATE')
        row = connection.execute('SELECT fields FROM active_timing_state WHERE entry_id=?', (str(entry['sp_id']),)).fetchone()
        fields = json.loads(row['fields']) if row else {}
        original_start = entry['start']
        if 'BreakMinutes' in fields:
            fields['BreakMinutes'] = entry.get('break_minutes') or 0
        intervals = _intervals(connection, entry['sp_id'], now)
        if corrected:
            if any(corrected > start for start, _end, _deducts, _type in intervals):
                raise ValueError('Start time cannot be later than an already recorded break or downtime.')
            fields.update(Start=corrected.isoformat(), LaborDate=corrected.astimezone(CENTRAL).date().isoformat(), Year=corrected.astimezone(CENTRAL).year)
            key = str(uuid.uuid4())
            audit = {'previous_start': original_start, 'corrected_start': corrected.isoformat(), 'employee': session['employee']['emp_id']}
        else:
            start, end, day = lunch
            key = day
            if connection.execute('SELECT 1 FROM active_timing_events WHERE entry_id=? AND kind=? AND event_key=?', (str(entry['sp_id']), kind, key)).fetchone():
                return {'updated': True, 'already_logged': True}
            if entry['status'] != 'In Progress':
                raise ValueError('Resume the current break before logging a missed lunch.')
            if end > now or start < datetime.fromisoformat(original_start.replace('Z', '+00:00')):
                raise ValueError('The full scheduled lunch hour must already have elapsed within this entry. Correct start time first if needed.')
            if any((deducts and start < finish and end > begin) or (label == 'Lunch' and begin.astimezone(CENTRAL).date().isoformat() == day)
                   for begin, finish, deducts, label in intervals):
                raise ValueError('Lunch or another deduction is already recorded for that period. No extra hour was deducted.')
            fields['BreakMinutes'] = int(fields.get('BreakMinutes', entry.get('break_minutes') or 0)) + 60
            connection.execute('INSERT INTO break_events(entry_id,emp_id,employee_name,machine_no,break_type,comment,started_at,ended_at,duration_minutes,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)',
                (str(entry['sp_id']), str(session['employee']['emp_id']), session['employee']['full_name'], session['machine_no'], 'Lunch', 'Missed lunch logged by operator', start.isoformat(), end.isoformat(), 60, now.isoformat()))
            audit = {'lunch_start': start.isoformat(), 'lunch_end': end.isoformat(), 'minutes': 60, 'employee': session['employee']['emp_id']}
        if corrected:
            from time_entry_app.bridge.entry_completion import _signature
            pipe = connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='entry_starting_pipes'").fetchone()
            if pipe:
                connection.execute('UPDATE entry_starting_pipes SET signature=? WHERE entry_id=? AND signature=?',
                    (_signature({**entry, 'start': fields['Start']}), str(entry['sp_id']), _signature(entry)))
        connection.execute('INSERT INTO active_timing_events VALUES(?,?,?,?,?)', (str(entry['sp_id']), kind, key, json.dumps(audit), now.isoformat()))
        queue_id = connection.execute("INSERT INTO time_entries(record_type,machine_no,emp_id,employee_name,payload,created_at,sync_status,source) VALUES('active_time_correction',?,?,?,?,?,'pending','offline-first')",
            (session['machine_no'], str(session['employee']['emp_id']), session['employee']['full_name'], json.dumps({'entry_id': str(entry['sp_id']), 'item_id': str(entry['sp_id']), 'fields': fields, 'timing_adjustment': audit}), now.isoformat())).lastrowid
        connection.execute('INSERT OR REPLACE INTO active_timing_state VALUES(?,?,?)', (str(entry['sp_id']), json.dumps(fields), queue_id))
        connection.commit()
        return {'updated': True, 'break_minutes': fields.get('BreakMinutes', entry.get('break_minutes') or 0)}
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
