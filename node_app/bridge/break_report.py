"""Read-only admin reporting for recorded inspection breaks."""
from datetime import date
from psycopg2.extras import RealDictCursor
import workflow_db as db
from node_app.bridge.pipe_report import can_view_pipe_report


def get_break_report(reviewer_adp, page=1, station_id=None, date_from=None, date_to=None, employee='', status='all'):
    if not can_view_pipe_report(db.get_employee_by_adp(reviewer_adp)):
        raise ValueError('Break reports are available only to Quality admins and IT.')
    try:
        page=max(1,int(page))
        station=int(station_id) if station_id not in (None,'') else None
        start=date.fromisoformat(date_from) if date_from else None
        end=date.fromisoformat(date_to) if date_to else None
    except (TypeError,ValueError):
        raise ValueError('Enter a valid report date, station and page.')
    if start and end and end < start:
        raise ValueError('The end date must be on or after the start date.')
    if status not in {'all','active','completed'}:
        raise ValueError('Choose All, Active or Completed breaks.')
    conditions=[];params=[]
    if station is not None:conditions.append('s.location_id=%s');params.append(station)
    if start:conditions.append("(b.started_at AT TIME ZONE 'America/Chicago')::date >= %s");params.append(start)
    if end:conditions.append("(b.started_at AT TIME ZONE 'America/Chicago')::date <= %s");params.append(end)
    search=str(employee or '').strip()[:100]
    if search:
        conditions.append("(strpos(lower(concat_ws(' ',s.inspector_name,s.inspector_adp,b.relief_name,b.relief_adp)), lower(%s)) > 0)")
        params.append(search)
    if status=='active':conditions.append('b.ended_at IS NULL')
    if status=='completed':conditions.append('b.ended_at IS NOT NULL')
    source=' FROM inspection_breaks b JOIN inspector_sessions s ON s.id=b.session_id'
    where=' WHERE '+' AND '.join(conditions) if conditions else ''
    connection=db.get_db_connection()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            cursor.execute('SELECT COUNT(*) AS total, COUNT(*) FILTER(WHERE b.ended_at IS NULL) AS active'+source+where,params)
            counts=dict(cursor.fetchone());pages=max(1,(counts['total']+19)//20);page=min(page,pages)
            cursor.execute("""SELECT b.id,b.mode,b.started_at,b.ended_at,b.relief_adp,b.relief_name,
                s.inspector_adp,s.inspector_name,s.location_id,s.location_name,s.branch,s.shift,
                GREATEST(EXTRACT(EPOCH FROM (COALESCE(b.ended_at,NOW())-b.started_at)),0)::double precision AS duration_seconds
                """+source+where+' ORDER BY (b.ended_at IS NULL) DESC,b.started_at DESC,b.id DESC LIMIT 20 OFFSET %s',[*params,(page-1)*20])
            rows=[dict(row) for row in cursor.fetchall()]
            cursor.execute('SELECT NOW() AS as_of')
            return {'breaks':rows,**counts,'page':page,'pages':pages,'as_of':cursor.fetchone()['as_of']}
    finally:
        connection.close()
