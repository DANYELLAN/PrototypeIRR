import os
import unittest
import uuid
from unittest.mock import patch
from psycopg2 import sql
import workflow_db as db
from node_app.bridge import lunch_inspections as lunch
from node_app.bridge import lunch_popup as popup

PLAN=[{'element_sequence':1,'element_description':'Diameter','capture_type':'numeric','min':1.2,'max':1.3},{'element_sequence':2,'element_description':'Visual','capture_type':'boolean'},{'element_sequence':3,'element_description':'Rotating','capture_type':'numeric','min':0,'max':2}]
ADMIN={'adp_number':'TEST-Q','name':'Quality Reviewer','adp_job_title_desc':'Quality Manager','employee_status':'Active'}
class PopupValidationTests(unittest.TestCase):
    def test_every_visual_requires_an_explicit_decision(self):
        for value in ['',None,'on','maybe']:
            with self.assertRaisesRegex(ValueError,'Pass or Fail'):
                lunch.evaluate_full(PLAN,{'1':'1.234','2':value,'3':'0'},require_explicit_visual=True)
        self.assertFalse(lunch.evaluate_full(PLAN,{'1':'1.234','2':'Yes','3':'0'},True)['has_failures'])
        self.assertTrue(lunch.evaluate_full(PLAN,{'1':'1.234','2':'No','3':'0'},True)['has_failures'])
    def test_foreign_or_closed_session_denied(self):
        for station in [None,{'id':1,'inspector_adp':'OTHER','logged_out_at':None},{'id':1,'inspector_adp':'I','logged_out_at':'ended'}]:
            with patch.object(db,'_fetch_one_dict',return_value=station),self.assertRaises(ValueError):popup._station(1,'I')

@unittest.skipUnless(os.getenv('IRR_REPORT_DB_TESTS')=='1','Enable isolated database tests')
class PopupDatabaseTests(unittest.TestCase):
    def test_snapshot_same_context_full_plan_and_independent_completion(self):
        connect=db.get_db_connection;admin=connect();schema='codex_lunch_popup_'+uuid.uuid4().hex
        with admin.cursor() as cursor:cursor.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
        admin.commit()
        def isolated():
            conn=connect()
            with conn.cursor() as c:c.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(schema)))
            conn.commit();return conn
        try:
            seed=isolated()
            with seed.cursor() as c:
                c.execute(db.WORKFLOW_SCHEMA_SQL)
                c.execute("INSERT INTO app_locations(location_name,branch,device_type) VALUES ('Station','Ennis','machine') RETURNING id");station=c.fetchone()[0]
                c.execute("INSERT INTO inspector_sessions(inspector_adp,inspector_name,branch,shift,location_id,location_name) VALUES ('I','Inspector','Ennis','Days',%s,'Station') RETURNING id",(station,));session=c.fetchone()[0]
                attempts=[]
                for wo,number,status in [('WO','007','passed'),('OTHER','999','passed'),('WO','008','in_progress')]:
                    c.execute("INSERT INTO pipe_units(production_number,operation_description,pipe_number,branch,current_status,latest_attempt_no) VALUES (%s,'BOX',%s,'Ennis',%s,1) RETURNING id",(wo,number,'completed' if status=='passed' else 'in_progress'));pipe=c.fetchone()[0]
                    c.execute("INSERT INTO inspection_attempts(pipe_unit_id,session_id,attempt_no,inspector_name,inspection_scope,status,completed_at,recipe_name) VALUES (%s,%s,1,'Inspector','standard',%s,CASE WHEN %s='passed' THEN NOW() ELSE NULL END,'TEST RECIPE') RETURNING id",(pipe,session,status,status));attempts.append(c.fetchone()[0])
                c.execute("INSERT INTO inspection_measurements(attempt_id,element_sequence,element_description,measured_value,pass_fail) VALUES (%s,1,'Diameter','1.234','Pass')",(attempts[0],))
            seed.commit();seed.close()
            with patch.object(db,'get_db_connection',side_effect=isolated),patch.object(db,'get_employee_by_adp',return_value=ADMIN),patch.object(db,'get_recipe_elements',return_value={'elements':PLAN}):
                row=popup.start_lunch_popup(session,'I','TEST-Q','WO','BOX',attempts[2])
                self.assertEqual(row['source_snapshot']['pipe_number'],'007')
                self.assertEqual(row['source_snapshot']['measurements'][0]['measured_value'],'1.234')
                self.assertEqual(len(row['recipe_snapshot']),3)
                with self.assertRaises(ValueError):popup.complete_lunch_popup(session,'OTHER',row['id'],'TEST-Q',{})
                with self.assertRaises(ValueError):popup.complete_lunch_popup(session,'I',row['id'],'TEST-Q',{'1':'1.234','3':'0'})
                saved=popup.complete_lunch_popup(session,'I',row['id'],'TEST-Q',{'1':'1.234','2':'Yes','3':'0'})
                self.assertEqual(saved['status'],'passed')
                self.assertEqual(db._fetch_one_dict('SELECT COUNT(*) AS n FROM inspection_attempts')['n'],3)
                self.assertEqual(db._fetch_one_dict('SELECT COUNT(*) AS n FROM inspection_measurements')['n'],1)
                self.assertEqual(db._fetch_one_dict('SELECT status FROM inspection_attempts WHERE id=%s',(attempts[2],))['status'],'in_progress')
                self.assertEqual(lunch.lunch_history('TEST-Q')['total'],1)
                offset_plan=[{**PLAN[0], 'nominal':2.881, 'min':2.878, 'max':2.886}, *PLAN[1:]]
                with patch.object(db,'get_recipe_elements',return_value={'elements':offset_plan}):
                    offset_row=popup.start_lunch_popup(session,'I','TEST-Q','WO','BOX',attempts[2])
                    offset_saved=popup.complete_lunch_popup(session,'I',offset_row['id'],'TEST-Q',{'1':'-.002','2':'Yes','3':'0'})
                    self.assertEqual(offset_saved['measurements'][0]['measured_value'],2.879)
                from node_app.bridge.pipe_report import save_live_inspection_draft
                save_live_inspection_draft(attempts[2],session,'I',[{**offset_plan[0], 'measured_value':'-.002'}],1,0,1)
                draft=db._fetch_one_dict('SELECT measurements FROM inspection_live_drafts WHERE attempt_id=%s',(attempts[2],))
                self.assertEqual(draft['measurements'][0]['measured_value'],2.879)

        finally:
            with admin.cursor() as c:c.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))
            admin.commit();admin.close()

if __name__=='__main__':unittest.main()
