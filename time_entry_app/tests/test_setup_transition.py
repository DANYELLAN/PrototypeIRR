import json
import tempfile
import unittest
import uuid
from pathlib import Path
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

from time_entry_app.bridge import setup_transition as setup
from time_entry_app.bridge import tablet_time as tablet
from time_entry_app.tests import test_tablet_time as tablet_tests


class SetupTransitionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.session = {'employee': {'emp_id': '000747', 'full_name': 'Operator'}, 'machine_no': 'TEST CNC', 'shift_id': '40'}
        self.start = datetime(2026, 11, 11, 14, tzinfo=timezone.utc)
        self.entry = {'sp_id': 'TEST-SETUP', 'id': 10, 'emp_id': '747', 'machine_no': 'TEST CNC',
            'status': 'In Progress', 'details_type': 'Set-Up', 'production_number': 'WO1', 'operation_id': '10',
            'start': self.start.isoformat(), 'labor_date': '2026-11-11', 'break_minutes': 15,
            'shift': '40', 'description': 'Pipe part', 'operation_description': 'Thread PIN', 'quantity': 0}
        self.context = {'active_entry': self.entry, 'details_step_two': [{'title': 'Set-Up'}, {'title': 'Machining'}, {'title': 'Inspection'}]}
        for target, name, value in [(setup.store, 'DB_PATH', Path(self.temp.name) / 'time.db'),
                                    (setup.store, 'LOCAL_MACHINE_CONFIG', Path(self.temp.name) / 'machines.json')]:
            p=patch.object(target,name,value);p.start();self.addCleanup(p.stop)
        for name, kwargs in [('_get_startstop_by_id', {'side_effect': lambda _: dict(self.entry)}),
                             ('get_dashboard_context', {'return_value': self.context}),
                             ('_utc_now', {'return_value': self.start+timedelta(hours=2)}),
                             ('_active_downtime_minutes', {'return_value': 10})]:
            p=patch.object(setup.backend,name,**kwargs);p.start();self.addCleanup(p.stop)

    def rows(self, table):
        conn=setup._db()
        try:return [dict(row) for row in conn.execute('SELECT * FROM '+table)]
        finally:conn.close()

    def test_confirm_atomically_submits_setup_and_starts_same_work_order_and_description(self):
        result=setup.end_setup({**self.session, 'machine_no': 'OTHER'},'TEST-SETUP','Machining','7')
        self.assertEqual(setup.end_setup(self.session,'TEST-SETUP','Machining','7'),result)
        rows=self.rows('approval_queue');self.assertEqual(len(rows),1)
        old=json.loads(rows[0]['payload'])['fields']
        pending=self.rows('time_entries');self.assertEqual(len(pending),2)
        new=json.loads(pending[1]['payload'])['fields']
        self.assertEqual(old['DetailsType'],'Set-Up');self.assertEqual(old['TotalMinutes'],95)
        self.assertEqual(old['Quantity'],7)
        self.assertEqual(old['Average'],round(7/(95/60),2))
        self.assertEqual(old['End'],new['Start'])
        self.assertEqual(new['DetailsType'],'Machining');self.assertEqual(new['BreakMinutes'],0)
        for key in ('ProductionNo','OperationID','Description','OperationDescription','Shift','MachineNo'):
            self.assertEqual(old[key],new[key])
        self.assertEqual(new['MachineNo'], self.entry['machine_no'])
        self.assertEqual(new['Quantity'],0)
        self.assertEqual(new['TranDescription'],'')
        self.assertEqual(setup.project_startstop_entries([self.entry])[0]['status'],'Submitted')
        self.assertEqual(result['next_entry_id'],f"local:{pending[1]['id']}")
        with patch.object(setup.backend,'_read_list',return_value=[]):
            current=setup.backend._local_startstop_entries('747')
        self.assertEqual(current[0]['details_type'],'Machining')

    def test_selected_detail_is_not_forced_to_machining(self):
        setup.end_setup(self.session,'TEST-SETUP','Inspection')
        self.assertEqual(json.loads(self.rows('time_entries')[1]['payload'])['fields']['DetailsType'],'Inspection')

    def test_invalid_quantities_leave_setup_untouched(self):
        for quantity in ('-1','1.5','NaN','Infinity','1000001',''):
            with self.subTest(quantity=quantity),self.assertRaises(ValueError):
                setup.end_setup(self.session,'TEST-SETUP','Machining',quantity)
        self.assertEqual(self.rows('approval_queue'),[])
        self.assertEqual(self.rows('time_entries'),[])

    def test_invalid_detail_break_and_wrong_owner_do_not_write(self):
        with self.assertRaises(ValueError):setup.end_setup(self.session,'TEST-SETUP','FORGED')
        for key,value in [('status','Paused'),('details_type','Machining'),('emp_id','99')]:
            prior=self.entry[key];self.entry[key]=value
            with self.subTest(key=key),self.assertRaises(ValueError):setup.end_setup(self.session,'TEST-SETUP','Machining')
            self.entry[key]=prior
        self.assertEqual(self.rows('approval_queue'),[])
        self.assertEqual(self.rows('time_entries'),[])

    def test_active_downtime_blocks_handoff(self):
        with patch.object(setup,'active_downtime',return_value={'id':'dt'}):
            with self.assertRaisesRegex(ValueError,'downtime'):setup.end_setup(self.session,'TEST-SETUP','Machining')
        self.assertEqual(self.rows('setup_transitions'),[])

    def test_stale_stop_break_and_edit_controls_cannot_resubmit_closed_setup(self):
        setup.end_setup(self.session,'TEST-SETUP','Machining')
        self.entry['status']='Submitted'
        for action in (setup.backend.stop_time_entry,setup.backend.pause_for_lunch,setup.backend.edit_active_time_entry):
            with self.subTest(action=action.__name__),self.assertRaisesRegex(ValueError,'already been submitted'):
                action('TEST-SETUP')
        self.assertEqual(len(self.rows('approval_queue')),1)

    def test_database_failure_rolls_back_submission_and_new_entry(self):
        conn=setup._db()
        conn.execute("CREATE TRIGGER fail_setup_start BEFORE INSERT ON time_entries WHEN NEW.record_type='startstop' BEGIN SELECT RAISE(ABORT, 'test failure'); END")
        conn.commit();conn.close()
        with self.assertRaises(Exception):setup.end_setup(self.session,'TEST-SETUP','Machining')
        self.assertEqual(self.rows('approval_queue'),[])
        self.assertEqual(self.rows('time_entries'),[])
        self.assertEqual(self.rows('setup_transitions'),[])


class TabletSetupTests(unittest.TestCase):
    setUp=tablet_tests.TabletTimeTests.setUp
    event=tablet_tests.TabletTimeTests.event
    rows=tablet_tests.TabletTimeTests.rows

    def start_setup(self):
        self.context['details_step_two']=[{'title':'Set-Up'},{'title':'Machining'},{'title':'Inspection'}]
        tablet.apply_event(self.session,self.event('start',timer_id=self.timer,production_number='WO1',operation_id='10',detail_type='Set-Up'))

    def test_offline_handoff_is_atomic_idempotent_and_has_one_boundary(self):
        self.start_setup();next_id=str(uuid.uuid4())
        event=self.event('end_setup',60,timer_id=self.timer,next_timer_id=next_id,detail_type='Machining',quantity='9')
        first=tablet.apply_event(self.session,event)
        self.assertEqual(tablet.apply_event(self.session,event),first)
        self.assertEqual(first['timer']['id'],next_id)
        self.assertEqual(first['timer']['fields']['DetailsType'],'Machining')
        self.assertEqual(first['timer']['fields']['ProductionNo'],'WO1')
        self.assertEqual(first['timer']['fields']['OperationDescription'],'Thread Box')
        old=json.loads(self.rows('approval_queue')[0]['payload'])['fields']
        self.assertEqual(old['End'],first['timer']['start']);self.assertEqual(old['TotalMinutes'],60)
        self.assertEqual(old['Quantity'],9)
        self.assertEqual(len(self.rows('approval_queue')),1)
        self.assertEqual(sum(not row['closed'] for row in self.rows('tablet_time_timers')),1)

    def test_reused_next_id_rolls_back_old_setup_submission(self):
        self.start_setup()
        conn=tablet._db()
        conn.execute('INSERT INTO tablet_time_timers VALUES(?,?,?,?,1)',('00000000-0000-4000-8000-000000000001','other','{}',self.start.isoformat()))
        conn.commit();conn.close()
        with self.assertRaises(Exception):tablet.apply_event(self.session,self.event('end_setup',60,timer_id=self.timer,next_timer_id='00000000-0000-4000-8000-000000000001',detail_type='Machining'))
        self.assertEqual(tablet.active_timer(self.session)['fields']['DetailsType'],'Set-Up')
        self.assertEqual(self.rows('approval_queue'),[])

    def test_invalid_detail_and_stale_source_are_rejected(self):
        self.start_setup()
        for data in [dict(timer_id=self.timer,detail_type='INVALID'),dict(timer_id=str(uuid.uuid4()),detail_type='Machining')]:
            with self.assertRaises(ValueError):tablet.apply_event(self.session,self.event('end_setup',30,next_timer_id=str(uuid.uuid4()),**data))
        self.assertEqual(self.rows('approval_queue'),[])

    def test_invalid_tablet_quantity_does_not_submit_or_restart(self):
        self.start_setup()
        for quantity in ('-1','1.5','NaN','Infinity','1000001',''):
            with self.subTest(quantity=quantity),self.assertRaises(ValueError):
                tablet.apply_event(self.session,self.event('end_setup',30,timer_id=self.timer,next_timer_id=str(uuid.uuid4()),detail_type='Machining',quantity=quantity))
        self.assertEqual(self.rows('approval_queue'),[])
        self.assertEqual(tablet.active_timer(self.session)['id'],self.timer)


if __name__=='__main__':unittest.main()
