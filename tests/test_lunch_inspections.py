import os
import unittest
import uuid
from unittest.mock import patch
from psycopg2 import sql
import workflow_db as db
from node_app.bridge import lunch_inspections as lunch

PLAN = [
    {"element_sequence":1,"element_description":"Diameter","capture_type":"numeric","min":1.2,"max":1.3},
    {"element_sequence":2,"element_description":"Visual","capture_type":"boolean"},
    {"element_sequence":3,"element_description":"Rotating check","capture_type":"numeric","min":0,"max":2},
]
ADMIN = {"adp_number":"TEST-Q","name":"Quality Reviewer","adp_job_title_desc":"Quality Manager","employee_status":"Active"}

class LunchEvaluationTests(unittest.TestCase):
    def test_full_measurements_visual_and_zero(self):
        good=lunch.evaluate_full(PLAN,{"1":"1.234","2":"Yes","3":"0"})
        self.assertFalse(good['has_failures'])
        self.assertEqual(len(good['measurements']),3)
        self.assertTrue(lunch.evaluate_full(PLAN,{"1":"1.234","3":"0"})['has_failures'])
        self.assertTrue(lunch.evaluate_full(PLAN,{"1":"9","2":"Yes","3":"0"})['has_failures'])
        for value in ['', 'nan', 'inf', 'no']:
            with self.assertRaises(ValueError):
                lunch.evaluate_full(PLAN,{"1":value,"2":"Yes","3":"0"})
    def test_reject_inspector_adp(self):
        with patch.object(db,'get_employee_by_adp',return_value={"adp_job_title_desc":"Inspector"}):
            with self.assertRaisesRegex(ValueError,'Quality admin'):
                lunch.reviewer('TEST-I')

@unittest.skipUnless(os.getenv('IRR_REPORT_DB_TESTS')=='1','Enable isolated database tests')
class LunchDatabaseTests(unittest.TestCase):
    def test_separate_full_history_and_immutable_submissions(self):
        connect=db.get_db_connection
        admin=connect()
        schema='codex_lunch_test_'+uuid.uuid4().hex
        with admin.cursor() as cursor:
            cursor.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
        admin.commit()
        def isolated():
            connection=connect()
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(schema)))
            connection.commit()
            return connection
        try:
            seed=isolated()
            with seed.cursor() as cursor:
                cursor.execute(db.WORKFLOW_SCHEMA_SQL)
                cursor.execute("INSERT INTO app_locations(location_name,branch,device_type) VALUES ('Test Station','Ennis','machine') RETURNING id")
                station=cursor.fetchone()[0]
                cursor.execute("INSERT INTO inspector_sessions(inspector_adp,inspector_name,branch,shift,location_id,location_name) VALUES ('TEST-I','Inspector','Ennis','Days',%s,'Test Station') RETURNING id",(station,))
                session=cursor.fetchone()[0]
                cursor.execute("INSERT INTO pipe_units(production_number,operation_description,pipe_number,branch,current_status,latest_attempt_no) VALUES ('WO','BOX','005','Ennis','completed',1) RETURNING id")
                pipe=cursor.fetchone()[0]
                cursor.execute("INSERT INTO inspection_attempts(pipe_unit_id,session_id,attempt_no,inspector_name,inspection_scope,status,completed_at,recipe_name) VALUES (%s,%s,1,'Inspector','standard','passed',NOW(),'TEST RECIPE') RETURNING id",(pipe,session))
                attempt=cursor.fetchone()[0]
            seed.commit()
            seed.close()
            with patch.object(db,'get_db_connection',side_effect=isolated),patch.object(db,'get_employee_by_adp',return_value=ADMIN),patch.object(db,'get_recipe_elements',return_value={"elements":PLAN}):
                target=lunch.lunch_target(station,'TEST-Q')
                self.assertEqual(target['pipe']['pipe_number'],'005')
                with self.assertRaisesRegex(ValueError,'another pipe'):
                    lunch.start_lunch(station,'TEST-Q',attempt+1,'005')
                with self.assertRaisesRegex(ValueError,'must match'):
                    lunch.start_lunch(station,'TEST-Q',attempt,'006')
                started=lunch.start_lunch(station,'TEST-Q',attempt,'005')
                self.assertEqual(len(started['recipe_snapshot']),3)
                self.assertEqual(lunch.lunch_history('TEST-Q')['total'],0)
                with self.assertRaises(ValueError):
                    lunch.complete_lunch(started['id'],'TEST-Q',{})
                saved=lunch.complete_lunch(started['id'],'TEST-Q',{'1':'1.234','3':'0'},'Visual defect')
                self.assertEqual(saved['status'],'failed')
                self.assertEqual(saved['reviewer_adp'],'TEST-Q')
                retry=lunch.complete_lunch(started['id'],'TEST-Q',{'1':'1.234','2':'Yes','3':'0'},'changed')
                self.assertEqual(retry['status'],'failed')
                self.assertEqual(retry['notes'],'Visual defect')
                self.assertEqual(lunch.lunch_history('TEST-Q')['total'],1)
                self.assertEqual(db._fetch_one_dict('SELECT COUNT(*) AS n FROM inspection_attempts')['n'],1)
                self.assertEqual(db._fetch_one_dict('SELECT COUNT(*) AS n FROM inspection_measurements')['n'],0)
                self.assertEqual(db._fetch_one_dict('SELECT current_status FROM pipe_units')['current_status'],'completed')
                self.assertEqual(db._fetch_one_dict('SELECT status FROM inspection_attempts')['status'],'passed')
        finally:
            with admin.cursor() as cursor:
                cursor.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))
            admin.commit()
            admin.close()
