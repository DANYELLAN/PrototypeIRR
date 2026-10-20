"""Inspection UI adapter for the existing durable mold approval workflow."""
import workflow_db as db
from time_entry_app.bridge import cnc_time_backend as notifications


def _employee(adp):
    employee = db.get_employee_by_adp(str(adp or '').strip())
    if not employee:
        raise ValueError('Sign in with an active employee badge.')
    return employee


def _owned_request(adp, request_id):
    notifications.ensure_db()
    record = notifications.get_notification_request(int(request_id))
    if (not record or record.get('notification_type') != 'mold_approval'
            or str(record.get('requester_emp_id')) != str(adp)):
        raise ValueError('Mold approval request not found.')
    return record


def mold_approval_status(inspector_adp, request_id):
    employee = _employee(inspector_adp)
    record = _owned_request(employee['adp_number'], request_id)
    return notifications._public_notification_record(notifications._refresh_sharepoint_mold_decision(record))


def mold_approval_context(inspector_adp, request_id=None):
    employee = _employee(inspector_adp)
    records = notifications.list_notification_requests(employee['adp_number'], limit=30, notification_type='mold_approval')
    watched = (_owned_request(employee['adp_number'], request_id) if request_id
               else next((row for row in records if row['status'] == 'pending'), None))
    if watched:
        watched = notifications._refresh_sharepoint_mold_decision(watched)
        records = [watched] + [row for row in records if row['id'] != watched['id']]
    return {'work_orders': notifications._notification_work_orders(),
            'requests': [notifications._public_notification_record(row) for row in records],
            'watch_id': watched['id'] if watched else None}


def request_mold_approval(inspector_adp, session_id, work_order):
    employee = _employee(inspector_adp)
    station = db._fetch_one_dict('SELECT inspector_adp, location_name, logged_out_at FROM inspector_sessions WHERE id=%s', (int(session_id),))
    if (not station or station['logged_out_at'] is not None
            or str(station['inspector_adp']) != str(employee['adp_number'])):
        raise ValueError('An active inspection station session is required.')
    if not station.get('location_name'):
        raise ValueError('Select an inspection station before requesting approval.')
    return notifications.submit_notification_request(
        employee={'emp_id': employee['adp_number'], 'full_name': employee['name']},
        machine_no=station['location_name'], notification_type='mold_approval', work_order=work_order)
