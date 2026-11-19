"""Detail-specific completion quantities and durable starting-pipe selection."""
import json
import math
import re

from time_entry_app.bridge import cnc_time_backend as backend
store = __import__('local_store')
PIPE_DETAILS = {'inspection', 'sandblast', 'drift', 'stenciling'}


def completion_mode(detail):
    name = ''.join(char for char in str(detail or '').lower() if char.isalnum())
    return 'setup' if name == 'setup' else 'pipes' if name in PIPE_DETAILS else 'quantity'


def pipe_number(value):
    text = str(value if value is not None else '').strip()
    if len(text) > 16 or not re.fullmatch('[0-9]+', text) or int(text) > 9007199254740991:
        raise ValueError('Enter a whole, non-negative pipe number.')
    return text


def pipe_quantity(start, end):
    first, last = int(pipe_number(start)), int(pipe_number(end))
    if last < first:
        raise ValueError('Ending pipe cannot be less than starting pipe.')
    quantity = last - first + 1
    if quantity > 1000000:
        raise ValueError('The pipe range cannot exceed 1000000 pipes.')
    return quantity


def entered_quantity(value):
    try:
        number = float(value)
    except (ValueError, TypeError):
        raise ValueError('Enter a whole quantity between 0 and 1000000.')
    if not math.isfinite(number) or not number.is_integer() or not 0 <= number <= 1000000:
        raise ValueError('Enter a whole quantity between 0 and 1000000.')
    return int(number)


def _db():
    store.ensure_db()
    connection = store._connect()
    connection.execute('CREATE TABLE IF NOT EXISTS entry_starting_pipes (entry_id TEXT PRIMARY KEY, signature TEXT NOT NULL, pipe_number TEXT NOT NULL)')
    return connection


def _signature(entry):
    return json.dumps([str(entry.get(key) or '') for key in ('emp_id', 'machine_no', 'production_number', 'operation_id', 'details_type', 'start')])


def starting_pipe(entry):
    connection = _db()
    try:
        row = connection.execute('SELECT * FROM entry_starting_pipes WHERE entry_id=?', (str(entry['sp_id']),)).fetchone()
        return row['pipe_number'] if row and row['signature'] == _signature(entry) else None
    finally:
        connection.close()


def save_starting_pipe(session, entry_id, number):
    entry = backend._get_startstop_by_id(entry_id)
    backend._require_break_owner(entry, session['employee']['emp_id'])
    if entry.get('status') not in {'In Progress', 'Paused'} or completion_mode(entry['details_type']) != 'pipes':
        raise ValueError('Starting pipes apply only to active Inspection, Sandblast, Drift or Stenciling entries.')
    number = pipe_number(number)
    connection = _db()
    try:
        connection.execute('INSERT OR REPLACE INTO entry_starting_pipes VALUES(?,?,?)', (str(entry['sp_id']), _signature(entry), number))
        connection.commit()
        return {'starting_pipe': number}
    finally:
        connection.close()


def completion_quantity(entry, quantity, ending_pipe):
    if completion_mode(entry.get('details_type')) != 'pipes':
        return entered_quantity(quantity), None
    first = starting_pipe(entry)
    if first is None:
        raise ValueError('Enter the starting pipe for this entry before ending it.')
    last = pipe_number(ending_pipe)
    return pipe_quantity(first, last), {'starting_pipe': first, 'ending_pipe': last}
