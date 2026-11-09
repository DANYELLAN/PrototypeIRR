import json
import tempfile
import unittest
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch
from time_entry_app.bridge import tablet_time as tablet


class TabletTimeTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.session={'employee':{'emp_id':'000747','full_name':'Test Operator','machinist':False},'machine_no':'Machine 2','shift_id':40,'user_email':'test@example.com'}
        self.context={'active_entry':None,'details_step_two':[{'title':'Production'}],
          'downtime_reasons':[{'code':'MECH','label':'Mechanical'}],
          'operations':[{'production_number':'WO1','operation_id':'10','order_type':'EN','inventory_id':'P1','description':'Pipe','operation_description':'Thread Box'}]}
        for target,key,value in [(tablet.store,'DB_PATH',Path(self.temp.name)/'tablet.db'),
            (tablet.store,'LOCAL_MACHINE_CONFIG',Path(self.temp.name)/'machines.json')]:
            p=patch.object(target,key,value);p.start();self.addCleanup(p.stop)
        p=patch.object(tablet.backend,'get_dashboard_context',return_value=self.context);p.start();self.addCleanup(p.stop)
        p=patch.object(tablet.backend,'_create_item',side_effect=AssertionError('No remote writes during receipt'));p.start();self.addCleanup(p.stop)
        p=patch.object(tablet.backend,'lookup_employee_by_adp',return_value={'emp_id':'000123','full_name':'Relief Operator'});p.start();self.addCleanup(p.stop)
        self.start=datetime(2026,10,21,23,30,tzinfo=timezone.utc)
        self.timer=str(uuid.uuid4())
    def event(self,kind,minute=0,**data):
        return {'id':str(uuid.uuid4()),'kind':kind,'at':(self.start+timedelta(minutes=minute)).isoformat(),'data':data}
    def start_event(self):
        return self.event('start',timer_id=self.timer,production_number='WO1',operation_id='10',detail_type='Production')
    def rows(self,table):
        conn=tablet._db()
        try:return [dict(row) for row in conn.execute('SELECT * FROM '+table)]
        finally:conn.close()
    def test_response_loss_retry_creates_exactly_one_approval(self):
        event=self.event('manual',hours=1,minutes=5,quantity=2,production_number='WO1',operation_id='10',detail_type='Production')
        first=tablet.apply_event(self.session,event)
        self.assertEqual(tablet.apply_event(self.session,event),first)
        self.assertEqual(len(self.rows('approval_queue')),1)
        fields=json.loads(self.rows('approval_queue')[0]['payload'])['fields']
        self.assertEqual(fields['TotalMinutes'],65)
        self.assertEqual(fields['LaborDate'],'2026-10-21')
    def test_breaks_downtime_dates_and_order_survive_reconnect(self):
        tablet.apply_event(self.session,self.start_event())
        tablet.apply_event(self.session,self.event('pause',30,timer_id=self.timer,break_type='Lunch'))
        tablet.apply_event(self.session,self.event('resume',60,timer_id=self.timer))
        tablet.apply_event(self.session,self.event('misc',70,timer_id=self.timer,production_number='WO1',operation_id='10',detail_type_ii='MECH',hours=0,minutes=5))
        tablet.apply_event(self.session,self.event('pause',80,timer_id=self.timer,break_type='With Relief',relief_adp='123'))
        tablet.apply_event(self.session,self.event('resume',90,timer_id=self.timer))
        stop=self.event('stop',120,timer_id=self.timer,quantity=12)
        result=tablet.apply_event(self.session,stop)
        self.assertEqual(result['timer']['status'],'Submitted')
        self.assertIsNone(tablet.active_timer(self.session))
        self.assertEqual(len(self.rows('approval_queue')),2)
        fields=json.loads(self.rows('approval_queue')[-1]['payload'])['fields']
        self.assertEqual(fields['TotalMinutes'],85)
        self.assertEqual(fields['BreakMinutes'],30)
        self.assertEqual(fields['LaborDate'],'2026-10-21')
        self.assertEqual(fields['End'],(self.start+timedelta(minutes=120)).isoformat())
        tablet.apply_event(self.session,stop)
        self.assertEqual(len(self.rows('approval_queue')),2)
    def test_failed_approval_rolls_back_timer_and_receipt(self):
        tablet.apply_event(self.session,self.start_event())
        stop=self.event('stop',60,timer_id=self.timer,quantity=12)
        with patch.object(tablet.backend,'_attach_acumatica_payload',side_effect=RuntimeError('simulated failure')):
            with self.assertRaises(RuntimeError):tablet.apply_event(self.session,stop)
        self.assertEqual(tablet.active_timer(self.session)['status'],'In Progress')
        self.assertEqual(len(self.rows('tablet_time_events')),1)
        self.assertEqual(self.rows('approval_queue'),[])
        tablet.apply_event(self.session,stop)
        self.assertEqual(len(self.rows('approval_queue')),1)
    def test_other_employee_and_changed_body_cannot_reuse_receipt(self):
        event=self.start_event();tablet.apply_event(self.session,event)
        other={**self.session,'employee':{'emp_id':'123','full_name':'Other'}}
        with self.assertRaises(ValueError):tablet.apply_event(other,event)
        event['data']['operation_id']='BAD'
        with self.assertRaises(ValueError):tablet.apply_event(self.session,event)
    def test_invalid_and_duplicate_transitions_are_not_saved(self):
        tablet.apply_event(self.session,self.start_event())
        with self.assertRaises(Exception):tablet.apply_event(self.session,self.start_event())
        with self.assertRaises(ValueError):tablet.apply_event(self.session,self.event('resume',1,timer_id=self.timer))
        with self.assertRaises(ValueError):tablet.apply_event(self.session,self.event('pause',-1,timer_id=self.timer,break_type='Lunch'))
        self.assertEqual(len(self.rows('tablet_time_events')),1)
    def test_invalid_work_order_and_quantity_retain_no_partial_records(self):
        for data in [dict(production_number='BAD',operation_id='10',detail_type='Production',hours=1,quantity=1),
                     dict(production_number='WO1',operation_id='10',detail_type='Production',hours=1,quantity='NaN')]:
            with self.subTest(data=data):
                with self.assertRaises(ValueError):tablet.apply_event(self.session,self.event('manual',**data))
        self.assertEqual(self.rows('approval_queue'),[])



    def test_relief_arrival_splits_interval_and_only_unrelieved_time_is_deducted(self):
        tablet.apply_event(self.session,self.start_event())
        tablet.apply_event(self.session,self.event('pause',10,timer_id=self.timer,break_type='No Relief'))
        arrived=self.event('relief_arrived',25,timer_id=self.timer,relief_adp='123',relief_name='FORGED NAME')
        receipt=tablet.apply_event(self.session,arrived)
        self.assertEqual(tablet.apply_event(self.session,arrived),receipt)
        self.assertEqual(receipt['timer']['break_minutes'],15)
        self.assertEqual(len(receipt['timer']['breaks']),2)
        self.assertEqual(receipt['timer']['breaks'][1]['relief_name'],'Relief Operator')
        resumed=tablet.apply_event(self.session,self.event('resume',50,timer_id=self.timer))
        covered=resumed['timer']['breaks'][1]
        self.assertEqual(covered['relief_adp'],'000123')
        self.assertEqual(covered['minutes'],25)
        self.assertEqual(covered['return_adp'],'000747')
        self.assertEqual(covered['end_reason'],'operator_returned')
        stopped=tablet.apply_event(self.session,self.event('stop',60,timer_id=self.timer,quantity=10))
        fields=json.loads(self.rows('approval_queue')[0]['payload'])['fields']
        self.assertEqual(fields['BreakMinutes'],15)
        self.assertEqual(fields['TotalMinutes'],45)
        self.assertIsNotNone(stopped['approval_id'])

    def test_relief_requires_valid_different_employee_and_allowed_transition(self):
        tablet.apply_event(self.session,self.start_event())
        for employee in [None,self.session['employee']]:
            with patch.object(tablet.backend,'lookup_employee_by_adp',return_value=employee):
                with self.assertRaises(ValueError):tablet.apply_event(self.session,self.event('pause',10,timer_id=self.timer,break_type='With Relief',relief_adp='747'))
        self.assertEqual(tablet.active_timer(self.session)['status'],'In Progress')
        with self.assertRaises(ValueError):tablet.apply_event(self.session,self.event('relief_arrived',10,timer_id=self.timer,relief_adp='123'))
        tablet.apply_event(self.session,self.event('pause',10,timer_id=self.timer,break_type='Lunch'))
        with self.assertRaises(ValueError):tablet.apply_event(self.session,self.event('relief_arrived',15,timer_id=self.timer,relief_adp='123'))
        self.assertEqual(len(self.rows('tablet_time_events')),2)

if __name__=='__main__':unittest.main()
