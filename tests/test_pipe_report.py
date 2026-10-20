import os
import unittest
import uuid
from unittest.mock import patch

from psycopg2 import sql
import workflow_db as db
from node_app.bridge import pipe_report as report


class PipeReportPolicyTests(unittest.TestCase):
    def test_only_quality_admins_and_it_can_view_or_verify(self):
        for title in ["Director of Quality", "Quality Control Specialist", "Quality Manager", "Quality Supervisor", "IT", "IT Manager", "Integration Technologist"]:
            self.assertTrue(report.can_view_pipe_report({"adp_job_title_desc": title}))
            self.assertTrue(db.is_admin_user({"adp_job_title_desc": title}))
        self.assertTrue(report.can_view_pipe_report({"department": "IT"}))
        for employee in [None, {}, {"role_title": "Production Manager"}, {"adp_job_title_desc": "Quality Inspector"}, {"adp_job_title_desc": "Quality Manager", "employee_status": "Inactive"}]:
            self.assertFalse(report.can_view_pipe_report(employee))

    def test_verification_hash_changes_only_when_the_inspection_content_changes(self):
        row = {"attempt_id": 1, "pipe_unit_id": 1, "pipe_number": "001", "status": "passed", "measurements": [{"id": 5, "measured_value": "1.234", "pass_fail": "Pass"}]}
        original = report.inspection_snapshot_hash(row)
        self.assertEqual(original, report.inspection_snapshot_hash({**row, "live_updated_at": "now"}))
        self.assertNotEqual(original, report.inspection_snapshot_hash({**row, "pipe_number": "002"}))
        self.assertNotEqual(original, report.inspection_snapshot_hash({**row, "measurements": [{"id": 6, "measured_value": "1.234", "pass_fail": "Pass"}]}))
        self.assertNotEqual(original, report.inspection_snapshot_hash({**row, "notes": "Corrected"}))


@unittest.skipUnless(os.getenv("IRR_REPORT_DB_TESTS") == "1", "Set IRR_REPORT_DB_TESTS=1 for isolated PostgreSQL integration tests")
class PipeReportDatabaseTests(unittest.TestCase):
    def test_live_report_pagination_drafts_verification_and_edit_invalidation(self):
        schema = "codex_pipe_report_test_" + uuid.uuid4().hex
        connect = db.get_db_connection
        admin = connect()
        with admin.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        admin.commit()

        def isolated_connection():
            connection = connect()
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
            connection.commit()
            return connection

        try:
            seed = isolated_connection()
            with seed.cursor() as cursor:
                cursor.execute(db.WORKFLOW_SCHEMA_SQL)
                cursor.execute("INSERT INTO app_locations(location_name, branch, device_type) VALUES ('TEST Station 1','Ennis','machine') RETURNING id")
                station1 = cursor.fetchone()[0]
                cursor.execute("INSERT INTO app_locations(location_name, branch, device_type) VALUES ('TEST Station 2','Ennis','machine') RETURNING id")
                station2 = cursor.fetchone()[0]
                sessions = []
                for station in [station1, station2]:
                    cursor.execute("INSERT INTO inspector_sessions(inspector_adp,inspector_name,branch,shift,location_id,location_name) VALUES ('TEST-I','Test Inspector','Ennis','Days',%s,%s) RETURNING id", (station, f"TEST Station {station}"))
                    sessions.append(cursor.fetchone()[0])
                attempts = []
                for number in range(1, 26):
                    status = 'in_progress' if number == 25 else 'passed'
                    cursor.execute("INSERT INTO pipe_units(production_number,operation_description,pipe_number,branch,current_status,latest_attempt_no) VALUES ('TEST-WO','TEST BOX',%s,'Ennis',%s,1) RETURNING id", (str(number), 'in_progress' if number == 25 else 'completed'))
                    pipe_id = cursor.fetchone()[0]
                    cursor.execute("INSERT INTO inspection_attempts(pipe_unit_id,session_id,attempt_no,inspector_name,inspection_scope,status,completed_at,started_at) VALUES (%s,%s,1,'Test Inspector','standard',%s,CASE WHEN %s='in_progress' THEN NULL ELSE NOW() END,NOW() + %s * interval '1 second') RETURNING id", (pipe_id, sessions[1] if number == 24 else sessions[0], status, status, number))
                    attempt_id = cursor.fetchone()[0]
                    attempts.append(attempt_id)
                    cursor.execute("INSERT INTO inspection_measurements(attempt_id,element_sequence,element_description,measured_value,pass_fail) VALUES (%s,1,'Diameter','1.234','Pass')", (attempt_id,))
            seed.commit()
            seed.close()
            reviewer = {"adp_number": "TEST-Q", "name": "Test Quality Reviewer", "adp_job_title_desc": "Quality Manager", "employee_status": "Active"}
            with patch.object(db, 'get_db_connection', side_effect=isolated_connection), patch.object(db, 'get_employee_by_adp', return_value=reviewer):
                first = report.get_live_pipe_report()
                self.assertEqual(first['total'], 25)
                self.assertEqual(len(first['pipes']), 20)
                self.assertEqual(len(first['stations']), 2)
                second = report.get_live_pipe_report(2)
                self.assertEqual(len(second['pipes']), 5)
                self.assertFalse({p['attempt_id'] for p in first['pipes']} & {p['attempt_id'] for p in second['pipes']})
                self.assertEqual(report.get_live_pipe_report(1, station2)['total'], 1)
                self.assertEqual(report.get_live_pipe_report(999)['page'], 2)
                submitted = next(p for p in first['pipes'] if p['attempt_id'] == attempts[23])
                report.verify_pipe_inspection(submitted['attempt_id'], submitted['snapshot_hash'], 'TEST-Q')
                verified = next(p for p in report.get_live_pipe_report()['pipes'] if p['attempt_id'] == submitted['attempt_id'])
                self.assertEqual(verified['verification']['verifier_name'], reviewer['name'])
                with self.assertRaisesRegex(ValueError, 'submitted'):
                    report.verify_pipe_inspection(attempts[24], 'bad', 'TEST-Q')
                db.update_attempt_measurements(submitted['attempt_id'], [{"element_sequence": 1, "element_description": "Diameter", "measured_value": "1.236", "pass_fail": "Fail"}])
                changed = next(p for p in report.get_live_pipe_report()['pipes'] if p['attempt_id'] == submitted['attempt_id'])
                self.assertIsNone(changed['verification'])
                self.assertTrue(changed['verification_changed'])
                with self.assertRaisesRegex(ValueError, 'changed'):
                    report.verify_pipe_inspection(submitted['attempt_id'], submitted['snapshot_hash'], 'TEST-Q')
                report.verify_pipe_inspection(changed['attempt_id'], changed['snapshot_hash'], 'TEST-Q')
                draft = [{"element_sequence": 1, "element_description": "Diameter", "capture_type": "numeric", "min": 0, "max": 1, "measured_value": "0"}]
                report.save_live_inspection_draft(attempts[24], sessions[0], 'TEST-I', draft, 100)
                report.save_live_inspection_draft(attempts[24], sessions[0], 'TEST-I', [{**draft[0], "measured_value": "1"}], 99)
                live = report.get_live_pipe_report()['pipes'][0]
                self.assertEqual(live['measurements'][0]['measured_value'], 0)
                self.assertEqual(live['measurements'][0]['pass_fail'], 'Pass')
                self.assertIsNotNone(live['live_updated_at'])
                with self.assertRaisesRegex(ValueError, 'no longer active'):
                    report.save_live_inspection_draft(attempts[24], sessions[1], 'TEST-I', draft, 101)
                with patch.object(db, 'get_employee_by_adp', return_value={"adp_job_title_desc": "Inspector"}):
                    with self.assertRaisesRegex(ValueError, 'permission'):
                        report.verify_pipe_inspection(submitted['attempt_id'], changed['snapshot_hash'], 'TEST-I')
        finally:
            self.assertTrue(schema.startswith('codex_pipe_report_test_'))
            with admin.cursor() as cursor:
                cursor.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
            admin.commit()
            admin.close()


if __name__ == '__main__':
    unittest.main()
