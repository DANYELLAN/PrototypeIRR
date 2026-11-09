import json
import os
import unittest
import uuid
from datetime import datetime,timezone,timedelta
from unittest.mock import patch
from psycopg2 import sql
import workflow_db as db
from node_app.bridge import tablet_inspection as tablet

RECIPE={'recipe_name':'TEST-RECIPE','elements':[
 {'item_id':1,'element_sequence':1,'element_description':'Diameter','capture_type':'numeric','nominal':2.881,'min':2.8779999999999997,'max':2.8859999999999997,'dwg_dim':'2.881 +.005/-.003','gauge':'MRP'},
 {'item_id':2,'element_sequence':2,'element_description':'Visual','capture_type':'boolean','dwg_dim':'Visual','gauge':''},
]}

@unittest.skipUnless(os.getenv('IRR_REPORT_DB_TESTS')=='1','Enable isolated PostgreSQL tests')
class TabletInspectionTests(unittest.TestCase):
 def setUp(self):
  self.connect=db._open_workflow_connection
  self.admin=self.connect();self.schema='codex_tablet_'+uuid.uuid4().hex
  with self.admin.cursor() as cursor:cursor.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(self.schema)))
  self.admin.commit()
  def isolated():
   connection=self.connect()
   with connection.cursor() as cursor:cursor.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(self.schema)))
   connection.commit();return connection
  self.isolated=isolated
  seed=isolated()
  with seed.cursor() as cursor:
   cursor.execute(db.WORKFLOW_SCHEMA_SQL)
   cursor.execute("INSERT INTO app_locations(location_name,branch,device_type) VALUES('Test Station','Ennis','machine') RETURNING id")
   station=cursor.fetchone()[0]
   cursor.execute("INSERT INTO inspector_sessions(inspector_adp,inspector_name,branch,shift,location_id,location_name) VALUES('TEST-I','Test Inspector','Ennis','Day',%s,'Test Station') RETURNING id",(station,))
   session=cursor.fetchone()[0]
  seed.commit();seed.close()
  self.params={'production_number':'TEST-WO','operation_description':'2.875 7.90# BEN-HT6 BOX','pipe_number':'001','branch':'Ennis','session_id':session,
   'inspector':{'item_id':1,'name':'Test Inspector','adp_number':'TEST-I','branch':'Ennis'},'cnc_operator':{'item_id':None,'name':'Test CNC'},'recipe_name':'TEST-RECIPE','inspection_scope':'full'}
  for target,key,options in [(db,'_open_workflow_connection',{'side_effect':isolated}),(db,'get_recipe_elements',{'return_value':RECIPE})]:
   p=patch.object(target,key,**options);p.start();self.addCleanup(p.stop)
 def tearDown(self):
  with self.admin.cursor() as cursor:cursor.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(self.schema)))
  self.admin.commit();self.admin.close()
 def event(self,**changes):
  at=datetime.now(timezone.utc)-timedelta(minutes=1)
  return {'id':str(uuid.uuid4()),'at':at.isoformat(),'started_at':(at-timedelta(minutes=5)).isoformat(),
   'pipe_number':'001','scope':'full','values':{'1':'+.005','2':'Yes'},'notes':'','coverage_version':0,'recipe_hash':tablet.fingerprint(RECIPE),**changes}
 def count(self,table):return db._fetch_one_dict('SELECT COUNT(*) AS n FROM '+table)['n']
 def test_response_retry_saves_exactly_one_attempt_and_actual_value(self):
  event=self.event();result=tablet.apply_pipe(self.params,event)
  self.assertEqual(tablet.apply_pipe(self.params,event),result)
  self.assertEqual(result['pipe_status'],'completed')
  self.assertEqual(self.count('inspection_attempts'),1)
  self.assertEqual(self.count('inspection_measurements'),2)
  self.assertEqual(self.count('tablet_inspection_receipts'),1)
  row=db._fetch_one_dict('SELECT measured_value FROM inspection_measurements WHERE element_sequence=1')
  self.assertEqual(float(row['measured_value']),2.886)
  row=db._fetch_one_dict('SELECT completed_at FROM inspection_attempts')
  self.assertEqual(row['completed_at'],datetime.fromisoformat(event['at']))
 def test_failure_after_completion_rolls_back_attempt_measurements_and_receipt(self):
  event=self.event();complete=db.complete_inspection_attempt
  def fail_after_save(*args,**kwargs):
   complete(*args,**kwargs)
   raise RuntimeError('Simulated connection failure after completion')
  with patch.object(db,'complete_inspection_attempt',side_effect=fail_after_save):
   with self.assertRaises(RuntimeError):tablet.apply_pipe(self.params,event)
  for table in ['pipe_units','inspection_attempts','inspection_measurements']:
   self.assertEqual(self.count(table),0)
  tablet.apply_pipe(self.params,event)
  self.assertEqual(self.count('inspection_attempts'),1)
 def test_failed_pipe_is_rework_with_one_ncr_and_cannot_be_overridden(self):
  event=self.event(values={'1':'+.006','2':'Yes'},notes='Diameter outside upper tolerance',disposition='manager_approved')
  result=tablet.apply_pipe(self.params,event)
  self.assertEqual(result['pipe_status'],'rework')
  self.assertEqual(self.count('ncr_reports'),1)
  tablet.apply_pipe(self.params,event)
  self.assertEqual(self.count('ncr_reports'),1)
 def test_stale_recipe_and_coverage_do_not_create_a_partial_pipe(self):
  for changes in [{'recipe_hash':'outdated'},{'coverage_version':1},{'coverage_version':None}]:
   with self.subTest(changes=changes):
    with self.assertRaises(ValueError):tablet.apply_pipe(self.params,self.event(**changes))
    self.assertEqual(self.count('pipe_units'),0)
 def test_missing_and_invalid_measurements_do_not_complete(self):
  for values in [{'1':'+.005'},{'1':'abc','2':'Yes'},{'1':'+.005','2':'maybe'}]:
   with self.subTest(values=values):
    with self.assertRaises(ValueError):tablet.apply_pipe(self.params,self.event(values=values))
    self.assertEqual(self.count('inspection_attempts'),0)
 def test_other_body_and_completed_pipe_cannot_duplicate_saved_measurements(self):
  event=self.event();tablet.apply_pipe(self.params,event)
  with self.assertRaises(ValueError):tablet.apply_pipe(self.params,{**event,'notes':'changed'})
  with self.assertRaises(ValueError):tablet.apply_pipe(self.params,self.event())
  self.assertEqual(self.count('inspection_measurements'),2)
 def test_prepare_preserves_current_recipe_and_sampling_settings(self):
  prepared=tablet.prepare(self.params)
  self.assertEqual(prepared['owner'],tablet.owner_for(self.params))
  self.assertEqual(prepared['recipe_hash'],tablet.fingerprint(RECIPE))
  self.assertEqual(prepared['inspecting'],'Test Inspector')
  self.assertEqual(prepared['initial_values'],{})

if __name__=='__main__':unittest.main()
