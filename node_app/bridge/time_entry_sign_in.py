"""Carry the verified Time Entry identity/station into inspection setup."""
import re
from psycopg2.extras import RealDictCursor
import workflow_db as db


def _station_key(value):
    text = str(value or '').strip().casefold()
    inspection_stations = {'line1inspection1': '1', 'line1inspection2': '2',
                           'line2inspection1': '3', 'line2inspection2': '4'}
    inspection_machine = inspection_stations.get(re.sub(r'[^a-z0-9]', '', text))
    if inspection_machine:
        return 'machine:' + inspection_machine
    machine = re.fullmatch(r'(?:cnc|machine|m)?\s*0*(\d+)', text)
    return 'machine:' + str(int(machine[1])) if machine else 'station:' + re.sub(r'\s+', ' ', text)


def prepare_time_entry_inspection(adp_number, machine_no, shift_id, shift_title=None):
    inspector = db.get_employee_by_adp(adp_number)
    if not inspector or inspector.get('employee_status') != 'Active':
        raise ValueError('An active employee record is required to continue.')
    shifts = {40: 'Day', 41: 'Night'}
    try:
        shift = shifts[int(shift_id)]
    except (ValueError, TypeError, KeyError):
        raise ValueError('Choose a valid shift in sign in.')
    station = str(machine_no or '').strip()
    if not station or len(station) > 100 or any(ord(character) < 32 for character in station):
        raise ValueError('Choose a valid machine in sign in.')
    key = _station_key(station)
    connection = db.get_db_connection()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))', ('inspection-station:' + key,))
            cursor.execute('SELECT * FROM app_locations')
            matches = [dict(row) for row in cursor.fetchall()
                       if key in {_station_key(row['location_name']), _station_key(row.get('machine_code'))}]
            if len(matches) > 1:
                raise ValueError('This machine has more than one inspection station. Ask Quality to check its setup.')
            if matches:
                location = matches[0]
                if not location['is_active']:
                    raise ValueError('This inspection station is inactive. Ask Quality to check its setup.')
            else:
                raise ValueError('Select a configured inspection station in sign in. Ask Quality to check the station setup if it is unavailable.')
        connection.commit()
    finally:
        connection.close()
    for current in db.get_locations():
        if current['id'] == location['id']:
            location = current
            break
    if location.get('is_locked') and db._normalize_identifier(location.get('active_inspector_adp')) != db._normalize_identifier(inspector['adp_number']):
        raise ValueError('This machine is already in use by another inspector. End that session before continuing.')
    existing = db._fetch_one_dict('''SELECT id,cnc_operator_item_id FROM inspector_sessions
        WHERE id=%s AND inspector_adp=%s AND logged_out_at IS NULL''',
        (location['active_session_id'], inspector['adp_number'])) if location.get('active_session_id') else None
    return {'inspector': inspector, 'location': location, 'shift': shift, 'existing_session': existing}


def start_time_entry_inspection(adp_number, machine_no, shift_id, operator_item_id, shift_title=None):
    setup = prepare_time_entry_inspection(adp_number, machine_no, shift_id, shift_title)
    inspector = setup['inspector']
    operator = next((person for person in db.get_cnc_operators(inspector['branch'])
                     if str(person['item_id']) == str(operator_item_id)), None)
    if not operator:
        raise ValueError('Choose an active machinist for this branch.')
    with db.workflow_transaction() as connection:
        with connection.cursor() as cursor:
            cursor.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))', ('inspection-login:' + str(setup['location']['id']),))
        session = db.create_inspector_session(inspector, setup['shift'], setup['location'], operator)
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            if str(session.get('cnc_operator_item_id')) != str(operator['item_id']):
                cursor.execute("SELECT id FROM inspection_attempts WHERE session_id=%s AND status='in_progress' LIMIT 1", (session['id'],))
                if cursor.fetchone():
                    raise ValueError('Finish the current pipe before changing its machinist.')
            cursor.execute('''UPDATE inspector_sessions SET cnc_operator_item_id=%s, cnc_operator_name=%s
                WHERE id=%s AND inspector_adp=%s AND logged_out_at IS NULL RETURNING *''',
                (db._coerce_bigint(operator['item_id']), operator['name'], session['id'], inspector['adp_number']))
            session = cursor.fetchone()
            if not session:
                raise ValueError('This inspection session ended. Continue from sign in again.')
    return {**setup, 'session': dict(session), 'is_admin': db.is_admin_user(inspector),
            'is_manager': db.is_manager_or_supervisor(inspector)}
