import tempfile
import unittest
from pathlib import Path
from datetime import datetime,timezone,timedelta
from unittest.mock import patch
from time_entry_app.bridge import cnc_time_backend as backend
store=__import__('local_store')

class TraditionalReliefTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.state={'sp_id':'TEST-ENTRY','status':'In Progress','emp_id':'000747','operators_name':'Original Operator','machine_no':'TEST CNC','break_minutes':0,'lunch_start':None,'lunch_stop':None}
        self.base=datetime(2026,11,5,16,0,tzinfo=timezone.utc);self.minute=0
        def update(_key,_id,fields):
            for key,value in fields.items():self.state[{'Status':'status','LunchStart':'lunch_start','LunchStop':'lunch_stop','BreakMinutes':'break_minutes'}[key]]=value
        for target,key,value in [(store,'DB_PATH',Path(self.temp.name)/'time.db'),(store,'LOCAL_MACHINE_CONFIG',Path(self.temp.name)/'machines.json')]:
            p=patch.object(target,key,value);p.start();self.addCleanup(p.stop)
        for key,options in [('_get_startstop_by_id',{'side_effect':lambda _id:dict(self.state)}),('_update_item',{'side_effect':update}),('_utc_now',{'side_effect':lambda:self.base+timedelta(minutes=self.minute)}),('_iso_now',{'side_effect':lambda:(self.base+timedelta(minutes=self.minute)).isoformat()}),('lookup_employee_by_adp',{'return_value':{'emp_id':'000123','full_name':'Relief Operator'}})]:
            p=patch.object(backend,key,**options);p.start();self.addCleanup(p.stop)
    def test_relief_arrival_closes_unrelieved_interval_and_resume_records_original_operator(self):
        self.minute=10;backend.pause_for_lunch('TEST-ENTRY','No Relief')
        self.minute=25;backend.relief_arrived('TEST-ENTRY','123')
        self.assertEqual(self.state['break_minutes'],15)
        covered=store.get_active_break_event('TEST-ENTRY')
        self.assertEqual(covered['break_type'],'With Relief');self.assertEqual(covered['relief_adp'],'000123');self.assertIsNotNone(covered['parent_break_id'])
        self.minute=50;backend.resume_from_lunch('TEST-ENTRY')
        self.assertEqual(self.state['break_minutes'],15);self.assertEqual(self.state['status'],'In Progress')
        conn=store._connect()
        try:rows=conn.execute('SELECT * FROM break_events ORDER BY id').fetchall()
        finally:conn.close()
        self.assertEqual(len(rows),2);self.assertEqual(rows[0]['duration_minutes'],15);self.assertEqual(rows[0]['end_reason'],'relief_arrived')
        self.assertEqual(rows[1]['duration_minutes'],25);self.assertEqual(rows[1]['returned_adp'],'000747')
    def test_browser_cannot_change_another_operators_break(self):
        with self.assertRaisesRegex(ValueError,'another operator'):
            backend.pause_for_lunch('TEST-ENTRY','No Relief',original_adp='999')
        self.assertEqual(self.state['status'],'In Progress')

    def test_direct_relief_requires_identity_and_never_deducts_relief_minutes(self):
        with self.assertRaises(ValueError):backend.pause_for_lunch('TEST-ENTRY','With Relief')
        self.assertIsNone(store.get_active_break_event('TEST-ENTRY'))
        self.minute=10;backend.pause_for_lunch('TEST-ENTRY','With Relief',relief_adp='123')
        self.assertEqual(store.get_active_break_event('TEST-ENTRY')['relief_name'],'Relief Operator')
        with self.assertRaises(ValueError):backend.relief_arrived('TEST-ENTRY','456')
        self.minute=30;backend.resume_from_lunch('TEST-ENTRY')
        self.assertEqual(self.state['break_minutes'],0)
