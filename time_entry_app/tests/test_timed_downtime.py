import json
import tempfile
import unittest
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch

from time_entry_app.bridge import timed_downtime as downtime
from time_entry_app.bridge import tablet_time as tablet
from time_entry_app.tests import test_tablet_time as tablet_tests


class TimedDowntimeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.session = {'employee': {'emp_id': '000747', 'full_name': 'Operator'}, 'machine_no': 'TEST CNC', 'shift_id': '40'}
        self.entry = {'sp_id': 'test-entry', 'emp_id': '747', 'machine_no': 'TEST CNC',
                      'status': 'In Progress', 'production_number': 'WO1', 'operation_id': '10'}
        self.start = datetime(2026, 11, 10, 23, 59, 30, tzinfo=timezone.utc)
        self.now = self.start
        for target, name, value in [(downtime.store, 'DB_PATH', Path(self.temp.name) / 'time.db'),
                                    (downtime.store, 'LOCAL_MACHINE_CONFIG', Path(self.temp.name) / 'machines.json')]:
            p = patch.object(target, name, value); p.start(); self.addCleanup(p.stop)
        for name, kwargs in [('_get_startstop_by_id', {'side_effect': lambda _: dict(self.entry)}),
                             ('_utc_now', {'side_effect': lambda: self.now}),
                             ('_downtime_reason_code', {'return_value': 'M1'})]:
            p = patch.object(downtime.backend, name, **kwargs); p.start(); self.addCleanup(p.stop)

    def records(self):
        conn = downtime._db()
        try:
            return [dict(row) for row in conn.execute('SELECT * FROM approval_queue')]
        finally:
            conn.close()

    def test_start_persists_and_end_queues_once_without_manual_duration(self):
        state = downtime.start_downtime(self.session, 'test-entry', 'M1', 'Repair chuck')
        self.assertEqual(downtime.active_downtime('test-entry')['id'], state['id'])
        self.assertEqual(self.records(), [])
        with self.assertRaisesRegex(ValueError, 'already'):
            downtime.start_downtime(self.session, 'test-entry', 'M1')
        self.now += timedelta(minutes=17, seconds=12)
        result = downtime.end_downtime(self.session, state['id'])
        self.assertEqual(result['minutes'], 17)
        repeated = downtime.end_downtime(self.session, state['id'])
        self.assertEqual(result['approval_id'], repeated['approval_id'])
        self.assertEqual(len(self.records()), 1)
        row = self.records()[0]
        payload = json.loads(row['payload'])
        self.assertEqual(row['active_direct_entry_id'], 'test-entry')
        self.assertEqual(payload['fields']['TotalMinutes'], 17)
        self.assertEqual(payload['fields']['DetailsTypeII'], 'M1')
        self.assertIn('Repair chuck', payload['fields']['TranDescription'])
        self.assertIsNone(downtime.active_downtime('test-entry'))

    def test_failure_rolls_back_log_and_keeps_production_paused(self):
        state = downtime.start_downtime(self.session, 'test-entry', 'M1')
        self.now += timedelta(minutes=10)
        with patch.object(downtime.backend, '_attach_acumatica_payload', side_effect=RuntimeError('failure')):
            with self.assertRaises(RuntimeError): downtime.end_downtime(self.session, state['id'])
        self.assertIsNotNone(downtime.active_downtime('test-entry'))
        self.assertEqual(self.records(), [])
        downtime.end_downtime(self.session, state['id'])
        self.assertEqual(len(self.records()), 1)

    def test_owner_station_break_and_clock_guards(self):
        other = {**self.session, 'employee': {'emp_id': '888', 'full_name': 'Other'}}
        with self.assertRaises(ValueError): downtime.start_downtime(other, 'test-entry', 'M1')
        with self.assertRaises(ValueError): downtime.start_downtime({**self.session, 'machine_no': 'OTHER'}, 'test-entry', 'M1')
        self.entry['status'] = 'Paused'
        with self.assertRaises(ValueError): downtime.start_downtime(self.session, 'test-entry', 'M1')
        self.entry['status'] = 'In Progress'
        state = downtime.start_downtime(self.session, 'test-entry', 'M1')
        with self.assertRaises(ValueError): downtime.require_no_downtime('test-entry')
        with self.assertRaises(ValueError): downtime.end_downtime(other, state['id'])
        self.now -= timedelta(seconds=1)
        with self.assertRaises(ValueError): downtime.end_downtime(self.session, state['id'])
        self.assertEqual(self.records(), [])

    def test_short_interval_keeps_exact_seconds_without_inventing_a_minute(self):
        state = downtime.start_downtime(self.session, 'test-entry', 'M1')
        self.now += timedelta(seconds=12)
        downtime.end_downtime(self.session, state['id'])
        payload = json.loads(self.records()[0]['payload'])
        self.assertEqual(payload['downtime_seconds'], 12)
        self.assertEqual(payload['fields']['TotalMinutes'], 0)


class TabletTimedDowntimeTests(unittest.TestCase):
    setUp = tablet_tests.TabletTimeTests.setUp
    event = tablet_tests.TabletTimeTests.event
    start_event = tablet_tests.TabletTimeTests.start_event
    rows = tablet_tests.TabletTimeTests.rows
    def begin(self):
        tablet.apply_event(self.session, self.start_event())
        event = self.event('downtime_start', 10, timer_id=self.timer, detail_type_ii='MECH', comments='Repair')
        tablet.apply_event(self.session, event)
        return event

    def test_timed_interval_syncs_once_then_production_excludes_it_once(self):
        start = self.begin()
        self.assertEqual(tablet.active_timer(self.session)['downtime']['id'], start['id'])
        self.assertEqual(self.rows('approval_queue'), [])
        end = self.event('downtime_end', 25, timer_id=self.timer, downtime_id=start['id'])
        receipt = tablet.apply_event(self.session, end)
        self.assertEqual(tablet.apply_event(self.session, end), receipt)
        self.assertEqual(receipt['timer']['downtime_minutes'], 15)
        self.assertNotIn('downtime', receipt['timer'])
        tablet.apply_event(self.session, self.event('stop', 60, timer_id=self.timer, quantity=3))
        rows = self.rows('approval_queue')
        self.assertEqual(len(rows), 2)
        self.assertEqual(json.loads(rows[0]['payload'])['fields']['TotalMinutes'], 15)
        self.assertEqual(json.loads(rows[0]['payload'])['fields']['DetailsTypeII'], 'MECH')
        self.assertEqual(json.loads(rows[1]['payload'])['fields']['TotalMinutes'], 45)

    def test_overlap_and_stale_downtime_end_are_blocked(self):
        self.begin()
        for kind, data in [('pause', {'break_type': 'Lunch'}), ('stop', {'quantity': 1}),
                           ('downtime_start', {'detail_type_ii': 'MECH'}),
                           ('downtime_end', {'downtime_id': 'stale'})]:
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                tablet.apply_event(self.session, self.event(kind, 15, timer_id=self.timer, **data))
        self.assertEqual(self.rows('approval_queue'), [])
        self.assertIsNotNone(tablet.active_timer(self.session)['downtime'])

    def test_failed_end_is_atomic_and_does_not_resume(self):
        start = self.begin()
        end = self.event('downtime_end', 25, timer_id=self.timer, downtime_id=start['id'])
        with patch.object(tablet.backend, '_attach_acumatica_payload', side_effect=RuntimeError('failure')):
            with self.assertRaises(RuntimeError): tablet.apply_event(self.session, end)
        self.assertIsNotNone(tablet.active_timer(self.session)['downtime'])
        self.assertEqual(self.rows('approval_queue'), [])
        tablet.apply_event(self.session, end)
        self.assertEqual(len(self.rows('approval_queue')), 1)


if __name__ == '__main__':
    unittest.main()
