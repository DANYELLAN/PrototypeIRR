import json
import uuid
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from time_entry_app import local_store
from time_entry_app.bridge import cnc_time_backend as backend


class ActiveDowntimeTests(unittest.TestCase):
    def setUp(self):
        temp_dir = Path(__file__).resolve().parent / ".tmp"
        temp_dir.mkdir(exist_ok=True)
        db_path = temp_dir / f"active_{uuid.uuid4().hex}.db"
        machines_path = temp_dir / f"machines_{uuid.uuid4().hex}.json"
        self.addCleanup(self.cleanup_files, db_path, machines_path)
        for target, name, value in (
            (local_store, "DB_PATH", db_path),
            (local_store, "LOCAL_MACHINE_CONFIG", machines_path),
            (backend, "list_active_direct_downtime", local_store.list_active_direct_downtime),
        ):
            patcher = patch.object(target, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.entry = {
            "id": 10, "sp_id": "local:10", "emp_id": "747", "employee_id": "000747",
            "production_number": "WO1", "operation_id": "0010", "status": "In Progress",
            "start": "2026-10-06T08:00:00+00:00", "lunch_start": None, "lunch_stop": None,
            "break_minutes": 30, "labor_date": "2026-10-06", "operators_name": "Operator",
            "shift": "40", "machine_no": "1", "details_type": "Machining",
            "inventory_id": "PART", "description": "Part", "operation_description": "Turn",
        }

    def cleanup_files(self, db_path, machines_path):
        for candidate in (db_path, db_path.with_name(f"{db_path.name}-wal"),
                          db_path.with_name(f"{db_path.name}-shm"), machines_path):
            candidate.unlink(missing_ok=True)

    def queue_downtime(self, minutes, entry_id="local:10", status="pending", **updates):
        fields = {"DetailsType": "DT", "EmpID": 747, "ProductionNo": "WO1",
                  "OperationID": "0010", "TotalMinutes": minutes, **updates}
        record_id = local_store.queue_approval("misc_time", {
            "active_direct_entry_id": entry_id, "fields": fields,
        })
        if status != "pending":
            local_store.mark_approval_reviewed(record_id, status)
        return record_id

    def test_matching_requires_employee_order_operation_and_running_entry(self):
        with patch.object(backend, "_read_list", return_value=[]), patch.object(
            backend, "_local_startstop_entries", return_value=[self.entry]
        ):
            self.assertEqual(backend._matching_active_direct_entry({"emp_id": "000747"}, "WO1", "0010"), self.entry)
            self.assertIsNone(backend._matching_active_direct_entry({"emp_id": "748"}, "WO1", "0010"))
            self.assertIsNone(backend._matching_active_direct_entry({"emp_id": "747"}, "WO2", "0010"))
            self.assertIsNone(backend._matching_active_direct_entry({"emp_id": "747"}, "WO1", "0020"))
            self.entry["status"] = "Submitted"
            self.assertIsNone(backend._matching_active_direct_entry({"emp_id": "747"}, "WO1", "0010"))

    def test_linked_downtime_survives_review_and_excludes_other_records(self):
        self.queue_downtime(15)
        self.queue_downtime(20, status="approved")
        self.queue_downtime(90, status="rejected")
        self.queue_downtime(90, entry_id="local:11")
        self.queue_downtime(90, ProductionNo="WO2")
        self.queue_downtime(90, OperationID="0020")
        self.queue_downtime(90, EmpID=748)
        local_store.queue_approval("manual_time", {
            "active_direct_entry_id": "local:10", "fields": {"TotalMinutes": 90},
        })
        self.assertEqual(backend._active_downtime_minutes(self.entry), 35)

    def test_misc_links_only_matching_active_entry_and_keeps_entered_duration(self):
        operation = {"order_type": "EN", "inventory_id": "PART", "description": "Part",
                     "operation_description": "Turn"}
        with patch.object(backend, "_get_operation_for_employee", return_value=operation), patch.object(
            backend, "_matching_active_direct_entry", return_value=self.entry
        ), patch.object(backend, "queue_approval", side_effect=local_store.queue_approval), patch.object(backend, "ensure_db"):
            result = backend.submit_misc_time({"emp_id": "747", "full_name": "Operator", "machinist": True},
                                             40, "1", "M1", "WO1", "0010", 0, 15, "Stopped")
        payload = json.loads(local_store.get_approval_record(result["approval_id"])["payload"])
        self.assertEqual(payload["active_direct_entry_id"], "local:10")
        self.assertEqual(payload["fields"]["TotalMinutes"], 15)
        self.assertEqual([row["labor_minutes"] for row in payload["acumatica_labor_transactions"]], [16, -1])
        self.assertEqual(backend._active_downtime_minutes(self.entry), 15)

    def test_stop_subtracts_breaks_and_downtime_and_rebuilds_net_acumatica_labor(self):
        self.queue_downtime(45)
        with patch.object(backend, "_get_startstop_by_id", return_value=self.entry), patch.object(
            backend, "_utc_now", return_value=datetime(2026, 10, 6, 16, tzinfo=timezone.utc)
        ), patch.object(backend, "queue_record", return_value=1), patch.object(backend, "_update_item"), patch.object(
            backend, "mark_record_synced"
        ), patch.object(backend, "lookup_employee_by_adp", return_value={"machinist": True}), patch.object(
            backend, "ensure_db"
        ), patch.object(backend, "queue_approval", side_effect=local_store.queue_approval):
            result = backend.stop_time_entry("local:10", quantity=10)
        payload = json.loads(local_store.get_approval_record(result["approval_id"])["payload"])
        self.assertEqual(result["worked_minutes"], 405)
        self.assertEqual(payload["fields"]["Total"], "06:45")
        self.assertEqual(payload["fields"]["BreakMinutes"], 30)
        self.assertEqual(payload["active_downtime_minutes"], 45)
        self.assertEqual(payload["acumatica_labor_transaction"]["labor_minutes"], 405)

    def test_correction_preserves_downtime_deduction_and_clamps_at_zero(self):
        entry = dict(self.entry, end="2026-10-06T16:00:00+00:00", downtime_minutes=45)
        self.assertEqual(backend._worked_minutes_from_entry(entry, 30), 405)
        self.assertEqual(backend._worked_minutes_from_entry(entry, 500), 0)

    def test_manual_submission_is_not_linked_to_active_timer(self):
        operation = {"order_type": "EN", "inventory_id": "PART", "description": "Part",
                     "operation_description": "Turn"}
        with patch.object(backend, "_get_operation_for_employee", return_value=operation), patch.object(
            backend, "_matching_active_direct_entry"
        ) as match, patch.object(backend, "queue_approval", return_value=1) as queue, patch.object(backend, "ensure_db"):
            backend.submit_manual_time({"emp_id": "747", "full_name": "Operator", "machinist": True},
                                       40, "1", "WO1", "0010", "Machining", 1, 0, 15, "")
        match.assert_not_called()
        payload = queue.call_args.args[1]
        self.assertNotIn("active_direct_entry_id", payload)
        self.assertEqual(payload["fields"]["TotalMinutes"], 15)

    def test_approval_payload_rebuild_does_not_add_balancing_minute_twice(self):
        fields = {"DetailsType": "DT", "DetailsTypeII": "M1", "TotalMinutes": 15, "MachineNo": "1"}
        payload = {"employee": {"machinist": True}, "fields": fields}
        backend._refresh_approval_acumatica_payload(payload, fields)
        backend._refresh_approval_acumatica_payload(payload, fields)
        self.assertEqual([row["labor_minutes"] for row in payload["acumatica_labor_transactions"]], [16, -1])
        self.assertEqual(fields["TotalMinutes"], 15)


if __name__ == "__main__":
    unittest.main()
