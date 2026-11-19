"""Inspection receipt and completion commit in the same PostgreSQL transaction."""
import hashlib
import json
import math
import uuid
from datetime import datetime, timezone, timedelta
import workflow_db as db
from node_app.bridge.inspection_breaks import inspection_break_state, validate_context

SCHEMA='''CREATE TABLE IF NOT EXISTS tablet_inspection_receipts (
    id UUID PRIMARY KEY, owner TEXT NOT NULL, body_hash TEXT NOT NULL,
    result JSONB NOT NULL, recorded_at TIMESTAMPTZ NOT NULL, received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)'''

def owner_for(params):
    return json.dumps([str(params['session_id']),params['production_number'],params['operation_description'],params['recipe_name']],separators=(',',':'))

def fingerprint(recipe):
    return hashlib.sha256(json.dumps(recipe,sort_keys=True,default=str,separators=(',',':')).encode()).hexdigest()

def prepare(params):
    coverage=validate_context(inspection_break_state(params['session_id']))
    recipe=db.get_recipe_elements(params['recipe_name'],params['branch'])
    if not recipe or not recipe.get('elements'):
        raise ValueError('Select a Digital IRR before preparing the tablet.')
    pipe=db.get_pipe_unit(params['production_number'],params['operation_description'],params['pipe_number'])
    initial_values={}
    if pipe:
        draft=db._fetch_one_dict('''SELECT d.measurements FROM inspection_live_drafts d
            JOIN inspection_attempts a ON a.id=d.attempt_id
            WHERE a.pipe_unit_id=%s AND a.session_id=%s AND a.status='in_progress'
            ORDER BY d.updated_at DESC LIMIT 1''',(pipe['id'],params['session_id']))
        if draft:
            initial_values={str(row['element_sequence']):str(row.get('entered_value',row['measured_value'])) for row in draft['measurements'] if row.get('measured_value') is not None}
    return {'owner':owner_for(params),'recipe':recipe,'recipe_hash':fingerprint(recipe),'initial_values':initial_values,
            'coverage_version':coverage['version'],'inspecting':coverage['acting_name'],'inspecting_adp':coverage['acting_adp'],
            'pipe_number':params['pipe_number'],'attempt_no':pipe['latest_attempt_no'] if pipe else 1,
            'inspection_scope':params.get('inspection_scope','standard'),
            'production_number':params['production_number'],'connection':params['operation_description'],
            'always_count':db.DEFAULT_ALWAYS_INSPECT_COUNT,'rotating_count':db.DEFAULT_ROTATING_COUNT,
            'prepared_at':datetime.now(timezone.utc).isoformat()}

def _timestamp(value):
    at=datetime.fromisoformat(str(value).replace('Z','+00:00'))
    if at.tzinfo is None or at>datetime.now(timezone.utc)+timedelta(minutes=5):
        raise ValueError('Check the tablet date and time before syncing.')
    return at

def apply_pipe(params,event):
    event_id=str(uuid.UUID(str(event.get('id'))))
    owner=owner_for(params)
    body_hash=hashlib.sha256(json.dumps(event,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    with db.workflow_transaction() as connection:
        with connection.cursor() as cursor:
            cursor.execute(SCHEMA)
            cursor.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))',(event_id,))
            cursor.execute('SELECT owner,body_hash,result FROM tablet_inspection_receipts WHERE id=%s',(event_id,))
            prior=cursor.fetchone()
            if prior:
                if prior[0]!=owner or prior[1]!=body_hash:
                    raise ValueError('This receipt ID belongs to different inspection data.')
                return prior[2]
        at=_timestamp(event.get('at'))
        started=_timestamp(event.get('started_at'))
        if started>at:raise ValueError('The pipe start time is later than its completion time.')
        if not isinstance(event.get('coverage_version'),int):raise ValueError('The saved inspector coverage version is missing.')
        validate_context(inspection_break_state(params['session_id']),event['coverage_version'])
        recipe=db.get_recipe_elements(params['recipe_name'],params['branch'])
        if event.get('recipe_hash')!=fingerprint(recipe):
            raise ValueError('The Digital IRR changed after this tablet was prepared. Its measurements have been kept for Quality review.')
        pipe_number=str(event.get('pipe_number') or '').strip()
        if not pipe_number or len(pipe_number)>100:raise ValueError('Enter a valid pipe number.')
        scope=event.get('scope')
        if scope not in {'standard','full'}:raise ValueError('Select a valid inspection scope.')
        attempt=db.create_inspection_attempt(**{**params,'pipe_number':pipe_number,'recipe_elements':recipe,'inspection_scope':scope})
        resumed=attempt.get('resumed_attempt',False)
        if attempt['inspection_scope']!=scope:
            attempt=db.update_inspection_attempt_scope(attempt['attempt_id'],recipe,scope)
        plan=attempt['inspection_plan']
        if not plan:raise ValueError('The Digital IRR has no required inspection elements. Ask Quality to check its sampling plan.')
        values=event.get('values') or {}
        measurements=[]
        for element in plan:
            raw=values.get(str(element['element_sequence']))
            if raw is None or str(raw).strip()=='':raise ValueError('Every required element must be inspected.')
            if element['capture_type']=='boolean' and raw not in {'Yes','No'}:raise ValueError('Select Pass or Fail for every visual element.')
            measurements.append({**element,'measured_value':raw,'inspected_this_pipe':True})
        evaluation=db.evaluate_measurements(measurements,recipe.get('approval_rules',[]))
        for row in evaluation['measurements']:
            if row['capture_type']=='numeric' and (not isinstance(row['measured_value'],(float,int)) or not math.isfinite(row['measured_value'])):
                raise ValueError('Enter a valid full measurement or signed offset for every numeric element.')
        notes=str(event.get('notes') or '').strip()
        if evaluation['has_failures'] and not notes:raise ValueError('Enter an NCR note for the failed measurement before syncing.')
        result=db.complete_inspection_attempt(attempt['attempt_id'],attempt['pipe_unit_id'],evaluation['measurements'],
            'rework' if evaluation['has_failures'] else 'pass',
            notes=f'Tablet recorded at {at.isoformat()}\n{notes}',coverage_version=event.get('coverage_version'),
            ncr_data={'nonconformance':notes,'immediate_containment':'Held for Quality review after tablet inspection.'})
        result.update(id=event_id,pipe_number=pipe_number,attempt_id=attempt['attempt_id'])
        with connection.cursor() as cursor:
            cursor.execute('UPDATE inspection_attempts SET completed_at=%s WHERE id=%s',(at,attempt['attempt_id']))
            if not resumed:
                cursor.execute('UPDATE inspection_attempts SET started_at=%s WHERE id=%s',(started,attempt['attempt_id']))
            cursor.execute('INSERT INTO tablet_inspection_receipts(id,owner,body_hash,result,recorded_at) VALUES(%s,%s,%s,%s::jsonb,%s)',
                           (event_id,owner,body_hash,json.dumps(result),at))
        return result


def save_draft(params, event):
    """Publish an online tablet draft to both monitoring views, without completing a pipe."""
    from node_app.bridge.pipe_report import save_live_inspection_draft
    with db.workflow_transaction():
        coverage = validate_context(inspection_break_state(params['session_id']), event.get('coverage_version'))
        recipe = db.get_recipe_elements(params['recipe_name'], params['branch'])
        if event.get('recipe_hash') != fingerprint(recipe):
            raise ValueError('The Digital IRR changed. Keep this tablet draft and ask Quality to review it.')
        pipe_number = str(event.get('pipe_number') or '').strip()
        if not pipe_number or len(pipe_number) > 100:
            raise ValueError('Enter a valid pipe number.')
        scope = event.get('scope')
        if scope not in {'standard', 'full'}:
            raise ValueError('Select a valid inspection scope.')
        attempt = db.create_inspection_attempt(**{**params, 'pipe_number': pipe_number, 'recipe_elements': recipe, 'inspection_scope': scope})
        if attempt['inspection_scope'] != scope:
            attempt = db.update_inspection_attempt_scope(attempt['attempt_id'], recipe, scope)
        values = event.get('values') or {}
        measurements = [{**element, 'measured_value': values.get(str(element['element_sequence']), 'No' if element['capture_type'] == 'boolean' else '')} for element in attempt['inspection_plan']]
        save_live_inspection_draft(attempt['attempt_id'], params['session_id'], params['inspector']['adp_number'], measurements, int(event.get('client_sequence') or 0), coverage['version'])
        return {'attempt': attempt, 'pipe_number': pipe_number}
