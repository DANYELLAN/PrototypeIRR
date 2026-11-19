import os
import unittest
import uuid
from unittest.mock import patch
from psycopg2 import sql
import workflow_db as db
from node_app.bridge import time_entry_sign_in as handoff

INSPECTOR = {'item_id': 1, 'adp_number': 'TEST-I', 'name': 'Test Inspector',
             'branch': 'Ennis', 'employee_status': 'Active', 'adp_job_title_desc': 'Thread Inspector'}
MACHINISTS = [{'item_id': 2, 'adp_number': 'TEST-M', 'name': 'Test Machinist', 'branch': 'Ennis'},
              {'item_id': 3, 'adp_number': 'TEST-M2', 'name': 'Another Machinist', 'branch': 'Ennis'}]


class StationMappingTests(unittest.TestCase):
    def test_four_inspection_stations_map_to_existing_machine_numbers(self):
        for station, machine in [('Line 1- Inspection 1', 1), ('Line 1- Inspection 2', 2),
                                 ('Line 2 - Inspection 1', 3), ('Line 2 - Inspection 2', 4)]:
            self.assertEqual(handoff._station_key(station), f'machine:{machine}')
            self.assertEqual(handoff._station_key(f'Machine {machine}'), f'machine:{machine}')
            self.assertEqual(handoff._station_key(f'M{machine}'), f'machine:{machine}')

    def test_other_stations_are_not_guessed_as_inspection_machines(self):
        self.assertEqual(handoff._station_key('Line 1- Sandblast 1'), 'station:line 1- sandblast 1')


@unittest.skipUnless(os.getenv('IRR_REPORT_DB_TESTS') == '1', 'Enable isolated PostgreSQL tests')
class TimeEntrySignInTests(unittest.TestCase):
    def setUp(self):
        self.open = db._open_workflow_connection
        self.admin = self.open()
        self.schema = 'codex_sign_in_' + uuid.uuid4().hex
        with self.admin.cursor() as cursor:
            cursor.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(self.schema)))
        self.admin.commit()
        def isolated():
            connection = self.open()
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(self.schema)))
            connection.commit()
            return connection
        for target, name, options in [(db, '_open_workflow_connection', {'side_effect': isolated}),
                                      (db, 'get_employee_by_adp', {'return_value': INSPECTOR}),
                                      (db, 'get_cnc_operators', {'return_value': MACHINISTS})]:
            mock = patch.object(target, name, **options)
            mock.start()
            self.addCleanup(mock.stop)
        connection = isolated()
        with connection.cursor() as cursor:
            cursor.execute(db.WORKFLOW_SCHEMA_SQL)
            for machine in range(1, 5):
                cursor.execute("INSERT INTO app_locations(location_name,machine_code,branch,device_type) VALUES(%s,%s,'QMS','machine')",
                               (f'Machine {machine}', f'M{machine}'))
        connection.commit()
        connection.close()

    def tearDown(self):
        self.admin.rollback()
        with self.admin.cursor() as cursor:
            cursor.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(self.schema)))
        self.admin.commit()
        self.admin.close()

    def test_mapping_uses_all_four_existing_stations_without_creating_duplicates(self):
        for station, machine in [('Line 1- Inspection 1', 1), ('Line 1- Inspection 2', 2),
                                 ('Line 2 - Inspection 1', 3), ('Line 2 - Inspection 2', 4)]:
            setup = handoff.prepare_time_entry_inspection('TEST-I', station, 41, 'Day Shift')
            self.assertEqual(setup['location']['location_name'], f'Machine {machine}')
            self.assertEqual(setup['shift'], 'Night')
            self.assertEqual(setup['inspector']['adp_number'], 'TEST-I')
        self.assertEqual(len(db.get_locations()), 4)

    def test_unmapped_time_entry_station_does_not_create_an_inspection_machine(self):
        with self.assertRaisesRegex(ValueError, 'configured inspection station'):
            handoff.prepare_time_entry_inspection('TEST-I', 'Line 1 - Sandblast 1', 40)
        self.assertEqual(len(db.get_locations()), 4)

    def test_machinist_is_selected_from_active_same_branch_directory(self):
        with self.assertRaisesRegex(ValueError, 'active machinist'):
            handoff.start_time_entry_inspection('TEST-I', 'Line 1- Inspection 1', 40, 'FORGED')
        result = handoff.start_time_entry_inspection('TEST-I', 'Line 1- Inspection 1', 40, 2)
        self.assertEqual(result['session']['inspector_adp'], 'TEST-I')
        self.assertEqual(result['session']['location_name'], 'Machine 1')
        self.assertEqual(result['session']['cnc_operator_name'], 'Test Machinist')
        self.assertEqual(result['session']['shift'], 'Day')
        setup = handoff.prepare_time_entry_inspection('TEST-I', 'Line 1- Inspection 1', 40)
        self.assertEqual(setup['existing_session']['id'], result['session']['id'])
        self.assertEqual(setup['existing_session']['cnc_operator_item_id'], 2)

        resumed = handoff.start_time_entry_inspection('TEST-I', 'Line 1- Inspection 1', 40, 3)
        self.assertEqual(resumed['session']['id'], result['session']['id'])
        self.assertEqual(resumed['session']['cnc_operator_name'], 'Another Machinist')
        connection = db.get_db_connection()
        with connection.cursor() as cursor:
            cursor.execute("INSERT INTO pipe_units(production_number,operation_description,pipe_number,branch,current_status) VALUES('TEST','TEST','1','Ennis','in_progress') RETURNING id")
            pipe = cursor.fetchone()[0]
            cursor.execute("INSERT INTO inspection_attempts(pipe_unit_id,session_id,inspector_name,attempt_no,inspection_scope,status) VALUES(%s,%s,'Test Inspector',1,'full','in_progress')", (pipe, resumed['session']['id']))
        connection.commit()
        connection.close()
        with self.assertRaisesRegex(ValueError, 'Finish the current pipe'):
            handoff.start_time_entry_inspection('TEST-I', 'Line 1- Inspection 1', 40, 2)


    def test_another_inspectors_station_cannot_be_taken_over(self):
        location = db.get_locations()[0]
        other = {**INSPECTOR, 'item_id': 99, 'adp_number': 'OTHER', 'name': 'Other Inspector'}
        db.create_inspector_session(other, 'Day', location, MACHINISTS[0])
        with self.assertRaisesRegex(ValueError, 'another inspector'):
            handoff.prepare_time_entry_inspection('TEST-I', 'Line 1- Inspection 1', 40)

    def test_inactive_employee_station_and_invalid_shift_are_rejected(self):
        with patch.object(db, 'get_employee_by_adp', return_value=None):
            with self.assertRaisesRegex(ValueError, 'active employee'):
                handoff.prepare_time_entry_inspection('TEST-I', 'Line 1- Inspection 1', 40)
        with self.assertRaisesRegex(ValueError, 'valid shift'):
            handoff.prepare_time_entry_inspection('TEST-I', 'Line 1- Inspection 1', 999)
        connection = db.get_db_connection()
        with connection.cursor() as cursor:
            cursor.execute("UPDATE app_locations SET is_active=FALSE WHERE machine_code='M1'")
        connection.commit()
        connection.close()
        with self.assertRaisesRegex(ValueError, 'inactive'):
            handoff.prepare_time_entry_inspection('TEST-I', 'Line 1- Inspection 1', 40)
