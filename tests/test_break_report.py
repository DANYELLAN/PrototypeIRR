import os
import unittest
import uuid
from unittest.mock import patch
from psycopg2 import sql
import workflow_db as db
from node_app.bridge.break_report import get_break_report
ADMIN={'adp_number':'TEST-Q','name':'Reviewer','adp_job_title_desc':'Quality Manager','employee_status':'Active'}
class BreakReportPolicyTests(unittest.TestCase):
    def test_only_current_admin_and_valid_filters(self):
        with patch.object(db,'get_employee_by_adp',return_value={'adp_job_title_desc':'Inspector'}):
            with self.assertRaisesRegex(ValueError,'Quality admins'):get_break_report('TEST-I')
        with patch.object(db,'get_employee_by_adp',return_value=ADMIN):
            for filters in [{'date_from':'invalid'},{'date_from':'2026-10-20','date_to':'2026-10-19'},{'status':'invalid'},{'station_id':'bad'}]:
                with self.assertRaises(ValueError):get_break_report('TEST-Q',**filters)
@unittest.skipUnless(os.getenv('IRR_REPORT_DB_TESTS')=='1','Enable isolated PostgreSQL tests')
class BreakReportDatabaseTests(unittest.TestCase):
    def test_pagination_filters_central_dates_duration_and_read_only(self):
        connect=db.get_db_connection;admin=connect();schema='codex_break_report_test_'+uuid.uuid4().hex
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
                sessions=[];stations=[]
                for n in [1,2]:
                    cursor.execute("INSERT INTO app_locations(location_name,branch,device_type) VALUES (%s,'Ennis','machine') RETURNING id",(f'TEST CNC {n}',));station=cursor.fetchone()[0];stations.append(station)
                    cursor.execute("INSERT INTO inspector_sessions(inspector_adp,inspector_name,branch,shift,location_id,location_name) VALUES (%s,%s,'Ennis','Day',%s,%s) RETURNING id",(f'TEST-I{n}',f'Test Inspector {n}',station,f'TEST CNC {n}'));sessions.append(cursor.fetchone()[0])
                for n in range(25):
                    start='2020-01-01T12:00:00Z' if n==0 else '2026-10-19T03:00:00Z' if n==1 else '2026-10-19T16:00:00Z'
                    with_relief=n%2==0
                    cursor.execute("""INSERT INTO inspection_breaks(session_id,mode,relief_adp,relief_name,started_at,ended_at)
                      VALUES (%s,%s,%s,%s,%s::timestamptz,CASE WHEN %s THEN NULL ELSE %s::timestamptz+interval '10 minutes' END)""",
                      (sessions[1] if n==24 else sessions[0],'with_relief' if with_relief else 'without_relief','TEST-R' if with_relief else None,'Test Relief' if with_relief else None,start,n==0,start))
            seed.commit();seed.close()
            with patch.object(db,'get_db_connection',side_effect=isolated),patch.object(db,'get_employee_by_adp',return_value=ADMIN):
                first=get_break_report('TEST-Q');second=get_break_report('TEST-Q',page=2)
                self.assertEqual(first['total'],25);self.assertEqual(first['active'],1)
                self.assertEqual(len(first['breaks']),20);self.assertEqual(len(second['breaks']),5)
                self.assertFalse({r['id'] for r in first['breaks']}&{r['id'] for r in second['breaks']})
                self.assertIsNone(first['breaks'][0]['ended_at']);self.assertGreater(first['breaks'][0]['duration_seconds'],0)
                self.assertEqual(first['breaks'][1]['duration_seconds'],600)
                self.assertEqual(get_break_report('TEST-Q',station_id=stations[1])['total'],1)
                self.assertEqual(get_break_report('TEST-Q',employee='test-r')['total'],13)
                self.assertEqual(get_break_report('TEST-Q',employee='Inspector 2')['total'],1)
                self.assertEqual(get_break_report('TEST-Q',employee="' OR TRUE --")['total'],0)
                self.assertEqual(get_break_report('TEST-Q',status='completed')['total'],24)
                self.assertEqual(get_break_report('TEST-Q',status='active')['total'],1)
                self.assertEqual(get_break_report('TEST-Q',date_from='2026-10-18',date_to='2026-10-18')['total'],1)
                self.assertEqual(get_break_report('TEST-Q',date_from='2026-10-19',date_to='2026-10-19')['total'],23)
                self.assertEqual(get_break_report('TEST-Q',page=999)['page'],2)
                self.assertEqual(db._fetch_one_dict('SELECT COUNT(*) AS n FROM inspection_breaks')['n'],25)
        finally:
            with admin.cursor() as cursor:cursor.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))
            admin.commit();admin.close()
