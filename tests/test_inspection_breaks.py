import os
import unittest
import uuid
from unittest.mock import patch
from psycopg2 import sql
import workflow_db as db
from node_app.bridge import inspection_breaks as breaks, pipe_report as report

OWNER={'adp_number':'TEST-I','name':'Original Inspector','item_id':1,'employee_status':'Active'}
RELIEF={'adp_number':'TEST-R','name':'Relief Inspector','item_id':2,'employee_status':'Active'}
PLAN=[{'element_sequence':1,'element_description':'Diameter','capture_type':'numeric','min':1,'max':2,'item_id':1}, {'element_sequence':2,'element_description':'Visual','capture_type':'boolean','item_id':2}]

class BreakValidationTests(unittest.TestCase):
    def test_pause_and_stale_versions_are_rejected(self):
        with self.assertRaisesRegex(ValueError,'paused'):
            breaks.validate_context({'mode':'without_relief','version':2},2)
        with self.assertRaisesRegex(ValueError,'coverage changed'):
            breaks.validate_context({'mode':None,'version':3},2)
        self.assertEqual(breaks.validate_context({'mode':'with_relief','version':2},2)['version'],2)

@unittest.skipUnless(os.getenv('IRR_REPORT_DB_TESTS')=='1','Enable isolated database tests')
class BreakDatabaseTests(unittest.TestCase):
    def test_break_pause_relief_partial_pipe_and_restoration(self):
        connect=db.get_db_connection
        admin=connect();schema='codex_break_test_'+uuid.uuid4().hex
        with admin.cursor() as cursor:cursor.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
        admin.commit()
        def isolated():
            connection=connect()
            with connection.cursor() as cursor:cursor.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(schema)))
            connection.commit();return connection
        try:
            seed=isolated()
            with seed.cursor() as cursor:
                cursor.execute(db.WORKFLOW_SCHEMA_SQL)
                cursor.execute("INSERT INTO app_locations(location_name,branch,device_type) VALUES ('Test Break Station','Ennis','machine') RETURNING id")
                station=cursor.fetchone()[0]
                cursor.execute("INSERT INTO inspector_sessions(inspector_adp,inspector_name,branch,shift,location_id,location_name) VALUES ('TEST-I','Original Inspector','Ennis','Days',%s,'Test Break Station') RETURNING id",(station,))
                session=cursor.fetchone()[0]
            seed.commit();seed.close()
            def lookup(adp):return {'TEST-I':OWNER,'TEST-R':RELIEF,'INACTIVE':{**RELIEF,'employee_status':'Inactive'}}.get(adp)
            with patch.object(db,'get_db_connection',side_effect=isolated),patch.object(db,'get_employee_by_adp',side_effect=lookup),patch.object(db,'get_recipe_elements',return_value={'elements':PLAN}):
                def create(number):return db.create_inspection_attempt('TEST-WO','TEST BOX',str(number),'Ennis',session,OWNER,None,'Test IRR',{'elements':PLAN})
                measurements=db.evaluate_measurements([{**PLAN[0],'measured_value':'1.234'},{**PLAN[1],'measured_value':'Yes'}])['measurements']
                def complete(attempt,version):return db.complete_inspection_attempt(attempt['attempt_id'],attempt['pipe_unit_id'],measurements,'pass',coverage_version=version)
                initial=breaks.inspection_break_state(session)
                self.assertEqual(initial['acting_adp'],'TEST-I');self.assertEqual(initial['version'],0)
                first=create('001')
                paused=breaks.start_inspection_break(session,'without_relief',expected_version=0)
                with self.assertRaisesRegex(ValueError,'paused'):complete(first,paused['version'])
                with self.assertRaisesRegex(ValueError,'paused'):create('002')
                with self.assertRaisesRegex(ValueError,'paused'):report.save_live_inspection_draft(first['attempt_id'],session,'TEST-I',measurements,1,paused['version'])
                resumed=breaks.end_inspection_break(session,paused['version'])
                for adp in ['MISSING','INACTIVE','TEST-I']:
                    with self.assertRaises(ValueError):breaks.start_inspection_break(session,'with_relief',adp,resumed['version'])
                relief=breaks.start_inspection_break(session,'with_relief','TEST-R',resumed['version'])
                with self.assertRaises(ValueError):breaks.start_inspection_break(session,'with_relief','TEST-R',relief['version'])
                with self.assertRaisesRegex(ValueError,'coverage changed'):complete(first,resumed['version'])
                complete(first,relief['version'])
                saved=db.get_pipe_attempt_history(first['pipe_unit_id'])[0]
                self.assertEqual(saved['inspector_adp'],'TEST-R');self.assertEqual(saved['inspector_name'],'Relief Inspector')
                self.assertEqual(saved['original_inspector_adp'],'TEST-I')
                self.assertEqual(saved['relief_inspectors'][0]['adp_number'],'TEST-R')
                # Loading the next empty pipe during relief must not permanently mark it as relief work.
                second=create('002')
                ended=breaks.end_inspection_break(session,relief['version'])
                with self.assertRaisesRegex(ValueError,'coverage changed'):report.save_live_inspection_draft(second['attempt_id'],session,'TEST-I',measurements,2,relief['version'])
                complete(second,ended['version'])
                saved=db.get_pipe_attempt_history(second['pipe_unit_id'])[0]
                self.assertEqual(saved['inspector_adp'],'TEST-I');self.assertEqual(saved['relief_inspectors'],[])
                # Partial relief work remains auditable when original inspector finishes the pipe.
                third=create('003')
                relief2=breaks.start_inspection_break(session,'with_relief','TEST-R',ended['version'])
                report.save_live_inspection_draft(third['attempt_id'],session,'TEST-I',measurements,5,relief2['version'],1)
                end2=breaks.end_inspection_break(session,relief2['version'])
                complete(third,end2['version'])
                saved=db.get_pipe_attempt_history(third['pipe_unit_id'])[0]
                self.assertEqual(saved['inspector_adp'],'TEST-I');self.assertEqual(saved['relief_inspectors'][0]['adp_number'],'TEST-R')
                details=db.get_pipe_history_details([third['pipe_unit_id']])
                self.assertEqual(details['attempts_by_pipe'][str(third['pipe_unit_id'])][0]['relief_inspectors'],saved['relief_inspectors'])
                live=report.get_live_pipe_report()
                self.assertTrue(any(pipe['relief_inspectors'] for pipe in live['pipes']))
                # Session ownership and location locking stay with the original inspector.
                session_row=db._fetch_one_dict('SELECT * FROM inspector_sessions WHERE id=%s',(session,))
                self.assertEqual(session_row['inspector_adp'],'TEST-I')
                unfinished=create('004')
                other=isolated()
                with other.cursor() as cursor:
                    cursor.execute("INSERT INTO inspector_sessions(inspector_adp,inspector_name,branch,shift) VALUES ('TEST-I','Original Inspector','Ennis','Days') RETURNING id")
                    next_session=cursor.fetchone()[0]
                other.commit();other.close()
                with self.assertRaisesRegex(ValueError,'another active station'):
                    db.create_inspection_attempt('TEST-WO','TEST BOX','004','Ennis',next_session,OWNER,None,'Test IRR',{'elements':PLAN})
                pause2=breaks.start_inspection_break(session,'without_relief',expected_version=end2['version'])
                db.close_inspector_session(session)
                self.assertIsNotNone(db._fetch_one_dict('SELECT ended_at FROM inspection_breaks WHERE id=%s',(pause2['break_id'],))['ended_at'])
                resumed_pipe=db.create_inspection_attempt('TEST-WO','TEST BOX','004','Ennis',next_session,OWNER,None,'Test IRR',{'elements':PLAN})
                self.assertEqual(resumed_pipe['attempt_id'],unfinished['attempt_id'])
                self.assertTrue(resumed_pipe['resumed_attempt'])
                complete(resumed_pipe,0)
                self.assertEqual(db._fetch_one_dict('SELECT session_id FROM inspection_attempts WHERE id=%s',(resumed_pipe['attempt_id'],))['session_id'],next_session)
        finally:
            with admin.cursor() as cursor:cursor.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))
            admin.commit();admin.close()
