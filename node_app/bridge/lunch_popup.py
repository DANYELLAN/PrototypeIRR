"""Station-bound lunch inspections alongside a snapshot of the production column."""
import json
from psycopg2.extras import Json
import workflow_db as db
from node_app.bridge.pipe_report import REPORT_SELECT
from node_app.bridge.lunch_inspections import reviewer, get_lunch, complete_lunch


def _station(session_id, inspector_adp):
    row = db._fetch_one_dict('SELECT id,inspector_adp,location_id,location_name,branch,logged_out_at FROM inspector_sessions WHERE id=%s', (int(session_id),))
    if not row or row['logged_out_at'] is not None or str(row['inspector_adp']) != str(inspector_adp):
        raise ValueError('An active inspection station session is required.')
    return row


def _source(station, production_number, operation_description, active_attempt_id=None, reference_mode='completed'):
    if not production_number or not operation_description:
        raise ValueError('Select the work order and connection in Inspection Entry first.')
    select = REPORT_SELECT.replace(' AND ia.attempt_no = pu.latest_attempt_no', '')
    if reference_mode == 'current':
        row = db._fetch_one_dict(select + ' WHERE ia.id=%s AND ia.session_id=%s AND ia.status=\'in_progress\'', (int(active_attempt_id or 0), int(station['id'])))
        if not row or row['production_number'] != production_number:
            raise ValueError('Load a current pipe in Inspection Entry first.')
        draft = db._fetch_one_dict('SELECT measurements FROM inspection_live_drafts WHERE attempt_id=%s', (row['attempt_id'],))
        row = {**row, 'measurements': draft['measurements'] if draft else []}
    elif reference_mode == 'completed':
        rows = db._fetch_all_dicts(select + " WHERE sess.location_id=%s AND pu.production_number=%s AND ia.completed_at IS NOT NULL AND ia.status <> 'in_progress' ORDER BY ia.completed_at DESC,ia.id DESC", (station['location_id'], production_number))
        wanted = db._inspection_connection_signature(operation_description)
        row = next((item for item in rows if item['operation_description'] == operation_description or (wanted and db._inspection_connection_signature(item['operation_description']) == wanted)), None)
        if not row:
            raise ValueError('Complete a pipe for this work order and connection before starting a lunch inspection.')
    else:
        raise ValueError('Invalid lunch inspection reference.')
    expected = db._inspection_connection_signature(operation_description)
    if row['operation_description'] != operation_description and (not expected or db._inspection_connection_signature(row['operation_description']) != expected):
        raise ValueError('The reference pipe does not match the selected connection.')
    return json.loads(json.dumps(dict(row), default=str))


def start_lunch_popup(session_id, inspector_adp, reviewer_adp, production_number, operation_description, active_attempt_id=None, reference_mode='completed'):
    employee = reviewer(reviewer_adp)
    station = _station(session_id, inspector_adp)
    source = _source(station, production_number, operation_description, active_attempt_id, reference_mode)
    recipe = db.get_recipe_elements(source['recipe_name'], source.get('branch'))
    plan = db.build_inspection_plan(recipe, 1, inspection_scope='full')
    if not plan:
        raise ValueError('This pipe has no Digital IRR elements. Quality must check its recipe.')
    source.update(popup_session_id=int(session_id), require_explicit_visual=True, reference_mode=reference_mode)
    connection = db.get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute('''INSERT INTO lunch_inspections(source_attempt_id,station_id,reviewer_adp,reviewer_name,reviewer_title,source_snapshot,recipe_snapshot,confirmed_pipe_number)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id''', (source['attempt_id'], station['location_id'], employee['adp_number'], employee['name'], employee.get('adp_job_title_desc') or employee.get('department') or 'IT', Json(source), Json(plan), str(source['pipe_number'])))
            inspection_id = cursor.fetchone()[0]
        connection.commit()
        return get_lunch(inspection_id, employee['adp_number'])
    finally:
        connection.close()


def get_popup_lunch(session_id, inspector_adp, inspection_id, reviewer_adp):
    station = _station(session_id, inspector_adp)
    row = get_lunch(inspection_id, reviewer_adp)
    if row['source_snapshot'].get('popup_session_id') != int(station['id']):
        raise ValueError('This lunch inspection belongs to a different station session.')
    return row


def complete_lunch_popup(session_id, inspector_adp, inspection_id, reviewer_adp, values, notes=''):
    get_popup_lunch(session_id, inspector_adp, inspection_id, reviewer_adp)
    return complete_lunch(inspection_id, reviewer_adp, values, notes)
