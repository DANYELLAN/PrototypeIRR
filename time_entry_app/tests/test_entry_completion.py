import json
import tempfile
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import patch

from time_entry_app.bridge import entry_completion as completion
from time_entry_app.bridge import tablet_time as tablet
from time_entry_app.tests import test_tablet_time as tablet_tests


class EntryCompletionTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.session={'employee':{'emp_id':'000747','full_name':'Operator'},'machine_no':'Line 1 - Inspection 1','shift_id':'40'}
        self.entry={'id':1,'sp_id':'TEST-ENTRY','emp_id':'747','machine_no':self.session['machine_no'],'status':'In Progress',
            'details_type':'Inspection','production_number':'WO1','operation_id':'10','start':'2026-11-11T14:00:00+00:00',
            'labor_date':'2026-11-11','shift':'40','operators_name':'Operator','employee_id':'000747','break_minutes':0,
            'lunch_start':None,'lunch_stop':None,'inventory_id':'PIPE','description':'Pipe','operation_description':'Inspect'}
        for target,name,value in [(completion.store,'DB_PATH',Path(self.temp.name)/'time.db'),(completion.store,'LOCAL_MACHINE_CONFIG',Path(self.temp.name)/'machines.json')]:
            p=patch.object(target,name,value);p.start();self.addCleanup(p.stop)
        p=patch.object(completion.backend,'_get_startstop_by_id',side_effect=lambda _:dict(self.entry));p.start();self.addCleanup(p.stop)

    def test_classification_includes_machining_in_cnc_popup_and_keeps_setup_separate(self):
        for detail in ('Machining','Turn & Bore','Recut','Rebore','Change Over'):
            self.assertEqual(completion.completion_mode(detail),'quantity')
        for detail in ('Inspection','SANDBLAST','Drift','Stenciling'):
            self.assertEqual(completion.completion_mode(detail),'pipes')
        self.assertEqual(completion.completion_mode('Set-Up'),'setup')

    def test_inclusive_ranges_and_invalid_numbers(self):
        self.assertEqual(completion.pipe_quantity('10','20'),11)
        self.assertEqual(completion.pipe_quantity('0010','0010'),1)
        for first,last in [('20','10'),('10','20.5'),('A10','20'),('0','1000000'),('','20'),('0','9007199254740992')]:
            with self.subTest(first=first,last=last),self.assertRaises(ValueError):completion.pipe_quantity(first,last)

    def test_start_survives_reads_but_cannot_transfer_to_another_entry_or_edited_job(self):
        completion.save_starting_pipe(self.session,'TEST-ENTRY','0010')
        self.assertEqual(completion.starting_pipe(dict(self.entry)),'0010')
        self.assertEqual(completion.completion_quantity(self.entry,'999','0020'),(11,{'starting_pipe':'0010','ending_pipe':'0020'}))
        for key,value in [('sp_id','OTHER'),('operation_id','20'),('production_number','WO2'),('start','2026-11-11T15:00:00+00:00')]:
            with self.subTest(key=key):self.assertIsNone(completion.starting_pipe({**self.entry,key:value}))

    def test_owner_detail_and_missing_start_are_enforced(self):
        with self.assertRaises(ValueError):
            completion.save_starting_pipe({**self.session, 'employee': {'emp_id': '999'}}, 'TEST-ENTRY', '10')
        with self.assertRaises(ValueError):completion.completion_quantity(self.entry,999,'20')
        self.entry['details_type']='Machining'
        with self.assertRaises(ValueError):completion.save_starting_pipe(self.session,'TEST-ENTRY','10')
        self.assertEqual(completion.completion_quantity(self.entry,'7',None),(7,None))

    def test_stop_uses_authoritative_range_quantity_and_keeps_pipe_metadata(self):
        completion.save_starting_pipe({**self.session, 'machine_no': 'OTHER'}, 'TEST-ENTRY', '10')
        with patch.object(completion.backend,'_utc_now',return_value=datetime(2026,11,11,16,tzinfo=timezone.utc)), \
             patch.object(completion.backend,'_update_item',side_effect=RuntimeError('No remote writes during tests')), \
             patch.object(completion.backend,'lookup_employee_by_adp',return_value=self.session['employee']):
            with self.assertRaisesRegex(ValueError, 'another operator'):
                completion.backend.stop_time_entry('TEST-ENTRY', '999', '20', '999')
            result=completion.backend.stop_time_entry('TEST-ENTRY','999','20','000747')
        payload=json.loads(completion.store.get_approval_record(result['approval_id'])['payload'])
        self.assertEqual(payload['fields']['MachineNo'], self.session['machine_no'])
        self.assertEqual(payload['fields']['Quantity'],11)
        self.assertEqual(payload['fields']['TotalMinutes'],120)
        self.assertEqual(payload['fields']['Average'],5.5)
        self.assertEqual(payload['pipe_range'],{'starting_pipe':'10','ending_pipe':'20'})

    def test_two_workstations_submit_a_72_hour_timer_only_once(self):
        self.entry.update(machine_no='1', details_type='Machining')
        end = datetime(2026, 11, 14, 14, tzinfo=timezone.utc)

        def stop():
            try:
                return completion.backend.stop_time_entry('TEST-ENTRY', '7', original_adp='747')
            except ValueError as error:
                return str(error)

        with patch.object(completion.backend, '_utc_now', return_value=end), \
             patch.object(completion.backend, '_update_item', side_effect=RuntimeError('Offline')), \
             patch.object(completion.backend, 'lookup_employee_by_adp', return_value=self.session['employee']), \
             ThreadPoolExecutor(max_workers=2) as workers:
            results = list(workers.map(lambda _: stop(), range(2)))

        submitted = [result for result in results if isinstance(result, dict)]
        self.assertEqual(len(submitted), 1)
        self.assertIn('This entry has already been submitted. Refresh the active entry.', results)
        row = completion.store.get_approval_record(submitted[0]['approval_id'])
        fields = json.loads(row['payload'])['fields']
        self.assertEqual((row['machine_no'], fields['MachineNo'], fields['Shift']), ('1', '1', '40'))
        self.assertEqual(fields['TotalMinutes'], 4320)
        self.assertEqual(len(completion.store.list_pending_records(record_types=['complete_startstop'])), 1)
        from time_entry_app.bridge.setup_transition import project_startstop_entries
        self.assertEqual(project_startstop_entries([self.entry])[0]['status'], 'Submitted')


class TabletPipeCompletionTests(unittest.TestCase):
    setUp=tablet_tests.TabletTimeTests.setUp
    event=tablet_tests.TabletTimeTests.event
    rows=tablet_tests.TabletTimeTests.rows

    def begin(self):
        self.context['details_step_two']=[{'title':'Inspection'},{'title':'Machining'}]
        tablet.apply_event(self.session,self.event('start',timer_id=self.timer,production_number='WO1',operation_id='10',detail_type='Inspection'))

    def test_offline_start_and_end_count_inclusively_and_ignore_forged_quantity(self):
        self.begin()
        tablet.apply_event(self.session,self.event('starting_pipe',5,timer_id=self.timer,starting_pipe='10'))
        self.assertEqual(tablet.active_timer(self.session)['starting_pipe'],'10')
        stop=self.event('stop',60,timer_id=self.timer,ending_pipe='20',quantity=999,completion_version=1)
        result=tablet.apply_event(self.session,stop)
        self.assertEqual(tablet.apply_event(self.session,stop),result)
        rows=self.rows('approval_queue');self.assertEqual(len(rows),1)
        payload=json.loads(rows[0]['payload'])
        self.assertEqual(payload['fields']['Quantity'],11)
        self.assertEqual(payload['pipe_range'],{'starting_pipe':'10','ending_pipe':'20'})

    def test_missing_start_bad_range_and_stale_timer_do_not_close_entry(self):
        self.begin()
        with self.assertRaises(ValueError):tablet.apply_event(self.session,self.event('stop',10,timer_id=self.timer,ending_pipe='20',completion_version=1))
        tablet.apply_event(self.session,self.event('starting_pipe',10,timer_id=self.timer,starting_pipe='20'))
        with self.assertRaises(ValueError):tablet.apply_event(self.session,self.event('stop',30,timer_id=self.timer,ending_pipe='10',completion_version=1))
        with self.assertRaises(ValueError):tablet.apply_event(self.session,self.event('starting_pipe',30,timer_id=str(uuid.uuid4()),starting_pipe='10'))
        self.assertIsNotNone(tablet.active_timer(self.session))
        self.assertEqual(self.rows('approval_queue'),[])

    def test_pre_update_offline_quantity_events_still_sync_without_loss(self):
        self.begin()
        tablet.apply_event(self.session,self.event('stop',30,timer_id=self.timer,quantity=7))
        payload=json.loads(self.rows('approval_queue')[0]['payload'])
        self.assertEqual(payload['fields']['Quantity'],7)
        self.assertTrue(payload['legacy_pipe_quantity'])
