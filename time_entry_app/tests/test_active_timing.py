import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from time_entry_app.bridge import active_timing as timing
from time_entry_app.bridge import entry_completion
from time_entry_app.bridge import tablet_time as tablet
from time_entry_app.tests import test_tablet_time as tablet_tests


class ActiveTimingTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.session={'employee':{'emp_id':'747','full_name':'Operator'},'machine_no':'TEST CNC','shift_id':'40'}
        self.now=datetime(2026,11,11,19,tzinfo=timezone.utc)
        for name,value in [('DB_PATH',Path(self.temp.name)/'time.db'),('LOCAL_MACHINE_CONFIG',Path(self.temp.name)/'machines.json')]:
            p=patch.object(timing.store,name,value);p.start();self.addCleanup(p.stop)
        for name,kwargs in [('_read_list',{'return_value':[]}),('_utc_now',{'side_effect':lambda:self.now}),
                            ('_update_item',{'side_effect':RuntimeError('No external writes during tests')}),
                            ('lookup_employee_by_adp',{'return_value':self.session['employee']})]:
            p=patch.object(timing.backend,name,**kwargs);p.start();self.addCleanup(p.stop)
        fields={'Status':'In Progress','Start':'2026-11-11T16:00:00+00:00','LaborDate':'2026-11-11','MachineNo':'TEST CNC',
                'EmpID':747,'OperatorsName':'Operator','Shift':'40','ProductionNo':'WO1','OperationID':'10','DetailsType':'Machining','BreakMinutes':0}
        self.record=timing.store.queue_record('startstop',{'fields':fields})
        self.entry_id=f'local:{self.record}'

    def rows(self,table):
        connection=timing._db()
        try:return [dict(row) for row in connection.execute('SELECT * FROM '+table)]
        finally:connection.close()

    def test_start_correction_changes_existing_record_and_lunch_deducts_once_on_completion(self):
        timing.adjust_active(self.session,self.entry_id,'correct_start','2026-11-11T08:00')
        current=timing.backend._get_startstop_by_id(self.entry_id)
        self.assertEqual(datetime.fromisoformat(current['start']).hour,14)
        self.assertEqual(current['production_number'],'WO1')
        self.assertEqual(self.rows('approval_queue'),[])
        timing.adjust_active(self.session,self.entry_id,'log_lunch')
        self.assertTrue(timing.adjust_active(self.session,self.entry_id,'log_lunch')['already_logged'])
        self.assertEqual(len(self.rows('break_events')),1)
        self.assertEqual(timing.backend._get_startstop_by_id(self.entry_id)['break_minutes'],60)
        self.assertEqual(self.rows('approval_queue'),[])
        self.assertEqual(sum(row['record_type']=='startstop' for row in self.rows('time_entries')),1)
        stopped=timing.backend.stop_time_entry(self.entry_id,4)
        fields=json.loads(timing.store.get_approval_record(stopped['approval_id'])['payload'])['fields']
        self.assertEqual(fields['TotalMinutes'],240)
        self.assertEqual(fields['BreakMinutes'],60)

    def test_start_change_keeps_saved_starting_pipe_for_the_same_job(self):
        timing.store.patch_pending_record_fields(self.record,{'DetailsType':'Inspection'})
        entry_completion.save_starting_pipe(self.session,self.entry_id,'10')
        timing.adjust_active(self.session,self.entry_id,'correct_start','2026-11-11T08:00')
        self.assertEqual(entry_completion.starting_pipe(timing.backend._get_startstop_by_id(self.entry_id)),'10')

    def test_time_only_edit_preserves_date_and_rejects_supplied_different_date(self):
        timing.adjust_active(self.session,self.entry_id,'correct_start','0800')
        self.assertEqual(timing.backend._get_startstop_by_id(self.entry_id)['labor_date'],'2026-11-11')
        with self.assertRaisesRegex(ValueError,'date must stay'):
            timing.adjust_active(self.session,self.entry_id,'correct_start','2026-11-10T08:00')
        for value in ['2400','0860','800']:
            with self.assertRaises(ValueError):timing.adjust_active(self.session,self.entry_id,'correct_start',value)

    def test_am_pm_corrections_handle_noon_midnight_and_preserve_date(self):
        self.now=datetime(2026,11,11,23,tzinfo=timezone.utc)
        for value,period,hour in [('800','AM',14),('230','PM',20),('1200','AM',6),('12:00','PM',18)]:
            timing.adjust_active(self.session,self.entry_id,'correct_start',value,start_period=period)
            entry=timing.backend._get_startstop_by_id(self.entry_id)
            self.assertEqual(datetime.fromisoformat(entry['start']).hour,hour)
            self.assertEqual(entry['labor_date'],'2026-11-11')
        for value,period in [('000','AM'),('1300','PM'),('1260','AM'),('800','')]:
            with self.assertRaises(ValueError):timing.adjust_active(self.session,self.entry_id,'correct_start',value,start_period=period)

    def test_later_breaks_and_saved_corrections_remain_visible_after_missed_lunch(self):
        timing.adjust_active(self.session,self.entry_id,'log_lunch')
        timing.store.queue_record('resume_lunch',{'entry_id':self.entry_id,'fields':{'BreakMinutes':75}})
        self.assertEqual(timing.backend._get_startstop_by_id(self.entry_id)['break_minutes'],75)
        timing.adjust_active(self.session,self.entry_id,'correct_start','2026-11-11T08:00')
        self.assertEqual(timing.backend._get_startstop_by_id(self.entry_id)['break_minutes'],75)
        timing.backend.edit_active_time_entry(self.entry_id,break_minutes=80)
        self.assertEqual(timing.backend._get_startstop_by_id(self.entry_id)['break_minutes'],80)

    def test_future_start_wrong_owner_closed_entry_and_premature_lunch_are_rejected(self):
        with self.assertRaises(ValueError):timing.adjust_active(self.session,self.entry_id,'correct_start','2026-11-11T14:00')
        with self.assertRaises(ValueError):timing.adjust_active({**self.session,'employee':{'emp_id':'999'}},self.entry_id,'log_lunch')
        self.now=datetime(2026,11,11,18,29,tzinfo=timezone.utc)
        with self.assertRaises(ValueError):timing.adjust_active(self.session,self.entry_id,'log_lunch')
        timing.store.patch_pending_record_fields(self.record,{'Status':'Submitted'})
        with self.assertRaises(ValueError):timing.adjust_active(self.session,self.entry_id,'correct_start','2026-11-11T08:00')
        self.assertEqual(self.rows('active_timing_events'),[])

    def test_existing_lunch_or_overlapping_downtime_cannot_be_deducted_again(self):
        timing.store.start_break_event(self.entry_id,'Lunch',started_at='2026-11-11T17:30:00+00:00')
        timing.store.finish_break_event(self.entry_id,ended_at='2026-11-11T18:30:00+00:00',duration_minutes=60)
        with self.assertRaises(ValueError):timing.adjust_active(self.session,self.entry_id,'log_lunch')
        with self.assertRaises(ValueError):timing.adjust_active(self.session,self.entry_id,'correct_start','2026-11-11T12:00')
        self.assertEqual(len(self.rows('break_events')),1)

    def test_failure_rolls_back_lunch_and_timing_correction(self):
        connection=timing._db()
        connection.execute("CREATE TRIGGER fail_correction BEFORE INSERT ON time_entries WHEN NEW.record_type='active_time_correction' BEGIN SELECT RAISE(ABORT, 'test failure'); END")
        connection.commit();connection.close()
        with self.assertRaises(Exception):timing.adjust_active(self.session,self.entry_id,'log_lunch')
        self.assertEqual(self.rows('break_events'),[])
        self.assertEqual(self.rows('active_timing_state'),[])
        self.assertEqual(self.rows('active_timing_events'),[])

    def test_central_clock_changes_and_night_shift_lunch(self):
        for value,now in [('2026-11-01T01:30',datetime(2026,11,1,9,tzinfo=timezone.utc)),('2026-03-08T02:30',datetime(2026,3,8,10,tzinfo=timezone.utc))]:
            with self.assertRaises(ValueError):timing.central_start(value,now)
        timing.store.patch_pending_record_fields(self.record,{'Start':'2026-11-12T02:00:00+00:00','Shift':'41'})
        self.now=datetime(2026,11,12,6,tzinfo=timezone.utc)
        timing.adjust_active(self.session,self.entry_id,'log_lunch')
        self.assertEqual(self.rows('break_events')[0]['duration_minutes'],60)


class TabletActiveTimingTests(unittest.TestCase):
    setUp=tablet_tests.TabletTimeTests.setUp
    event=tablet_tests.TabletTimeTests.event
    rows=tablet_tests.TabletTimeTests.rows
    def begin(self):
        self.start=datetime(2026,11,10,16,tzinfo=timezone.utc)
        tablet.apply_event(self.session,self.event('start',timer_id=self.timer,production_number='WO1',operation_id='10',detail_type='Production'))

    def test_correct_start_and_missed_lunch_replay_without_creating_extra_entries(self):
        self.begin()
        tablet.apply_event(self.session,self.event('correct_start',30,timer_id=self.timer,start_time='2026-11-10T08:00'))
        lunch=self.event('log_lunch',180,timer_id=self.timer,lunch_day='2026-11-10')
        receipt=tablet.apply_event(self.session,lunch)
        self.assertEqual(tablet.apply_event(self.session,lunch),receipt)
        self.assertEqual(receipt['timer']['break_minutes'],60)
        self.assertEqual(len(self.rows('tablet_time_timers')),1)
        self.assertEqual(self.rows('approval_queue'),[])
        with self.assertRaises(ValueError):tablet.apply_event(self.session,self.event('log_lunch',181,timer_id=self.timer,lunch_day='2026-11-10'))
        tablet.apply_event(self.session,self.event('stop',240,timer_id=self.timer,quantity=10))
        fields=json.loads(self.rows('approval_queue')[0]['payload'])['fields']
        self.assertEqual(fields['TotalMinutes'],300)

    def test_future_start_and_existing_lunch_do_not_change_timer(self):
        self.begin()
        with self.assertRaises(ValueError):tablet.apply_event(self.session,self.event('correct_start',30,timer_id=self.timer,start_time='2026-11-11T14:00'))
        tablet.apply_event(self.session,self.event('pause',90,timer_id=self.timer,break_type='Lunch'))
        tablet.apply_event(self.session,self.event('resume',150,timer_id=self.timer))
        with self.assertRaises(ValueError):tablet.apply_event(self.session,self.event('log_lunch',180,timer_id=self.timer))
        self.assertEqual(tablet.active_timer(self.session)['break_minutes'],60)

    def test_tablet_time_only_edit_cannot_change_entry_date(self):
        self.begin()
        tablet.apply_event(self.session,self.event('correct_start',30,timer_id=self.timer,start_time='800',start_period='AM'))
        self.assertEqual(tablet.active_timer(self.session)['fields']['LaborDate'],'2026-11-10')
        with self.assertRaisesRegex(ValueError,'date must stay'):
            tablet.apply_event(self.session,self.event('correct_start',31,timer_id=self.timer,start_time='2026-11-09T08:00'))
