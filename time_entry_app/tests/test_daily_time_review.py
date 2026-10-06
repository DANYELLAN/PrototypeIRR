import json
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from time_entry_app import local_store
from time_entry_app.bridge import cnc_time_backend as backend


class DailyTimeReviewTests(unittest.TestCase):
    def setUp(self):
        temp_dir = Path(__file__).resolve().parent / ".tmp"
        temp_dir.mkdir(exist_ok=True)
        self.db_path = temp_dir / f"daily_{uuid.uuid4().hex}.db"
        self.machine_path = temp_dir / f"machines_{uuid.uuid4().hex}.json"
        self.addCleanup(self.cleanup_files)
        for target, name, value in (
            (local_store, "DB_PATH", self.db_path),
            (local_store, "LOCAL_MACHINE_CONFIG", self.machine_path),
            (backend, "list_employee_approvals", local_store.list_employee_approvals),
            (backend, "get_daily_time_confirmation", local_store.get_daily_time_confirmation),
            (backend, "save_daily_time_confirmation", local_store.save_daily_time_confirmation),
            (backend, "get_approval_record", local_store.get_approval_record),
            (backend, "patch_approval_record_payload", local_store.patch_approval_record_payload),
        ):
            patcher = patch.object(target, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.remote = []
        patcher = patch.object(backend, "_read_list", side_effect=lambda *args, **kwargs: self.remote)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.employee = {"emp_id": "000747", "full_name": "Operator", "machinist": True}
        self.day = "2026-10-06"

    def cleanup_files(self):
        for candidate in (self.db_path, self.db_path.with_name(f"{self.db_path.name}-wal"),
                          self.db_path.with_name(f"{self.db_path.name}-shm"), self.machine_path):
            candidate.unlink(missing_ok=True)

    def queue(self, minutes, status="pending", **updates):
        fields = {"LaborDate": self.day, "EmpID": 747, "Shift": "40", "ProductionNo": "WO1",
                  "OperationID": "0010", "MachineNo": "1", "DetailsType": "Machining",
                  "TotalMinutes": minutes, "Total": backend._hhmm_from_minutes(minutes), "Quantity": 1,
                  **updates}
        record_id = local_store.queue_approval("manual_time", {"fields": fields, "employee": self.employee}, emp_id=fields["EmpID"])
        if status != "pending":
            local_store.mark_approval_reviewed(record_id, status)
        return record_id

    def review(self):
        return backend.get_daily_time_review(self.employee, self.day)

    def test_limit_is_strictly_greater_than_630_minutes(self):
        self.queue(630, BreakMinutes=30)
        self.assertFalse(self.review()["required"])
        self.queue(1)
        review = self.review()
        self.assertTrue(review["required"])
        self.assertEqual(review["total_minutes"], 631)
        self.assertEqual(review["total"], "10:31")

    def test_daily_total_includes_all_shifts_excludes_rejected_other_days_and_people(self):
        self.queue(600)
        self.queue(45, DetailsType="DT", Shift="50")
        self.queue(100, status="rejected")
        self.queue(100, LaborDate="2026-10-05")
        self.queue(100, EmpID=748)
        review = self.review()
        self.assertEqual(review["total_minutes"], 645)
        self.assertEqual(len(review["entries"]), 2)

    def test_approved_downstream_copies_are_counted_only_once(self):
        record_id = self.queue(640, status="approved")
        fields = json.loads(local_store.get_approval_record(record_id)["payload"])["fields"]
        self.remote.append({"id": "sp:5", "fields": fields})
        self.assertEqual(self.review()["total_minutes"], 640)
        self.assertEqual(len(self.review()["entries"]), 1)

    def test_identical_separate_entries_are_not_deduplicated(self):
        self.queue(320)
        self.queue(320)
        self.assertEqual(self.review()["total_minutes"], 640)
        self.assertEqual(len(self.review()["entries"]), 2)

    def test_non_machinist_does_not_need_review(self):
        self.queue(700)
        result = backend.get_daily_time_review(dict(self.employee, machinist=False), self.day)
        self.assertFalse(result["required"])

    def test_confirmation_requires_nonblank_reason_and_current_snapshot(self):
        self.queue(640)
        review = self.review()
        for reason in ("", "  ", None):
            with self.subTest(reason=reason), self.assertRaisesRegex(ValueError, "reason is required"):
                backend.confirm_daily_time_review(self.employee, self.day, review["snapshot_hash"], reason)
        with self.assertRaisesRegex(ValueError, "changed"):
            backend.confirm_daily_time_review(self.employee, self.day, "outdated", "Overtime")
        self.assertTrue(self.review()["required"])

    def test_reason_is_persisted_for_admin_and_changes_require_new_review(self):
        record_id = self.queue(640)
        review = self.review()
        backend.confirm_daily_time_review(self.employee, self.day, review["snapshot_hash"], "  Approved overtime  ")
        self.assertFalse(self.review()["required"])
        self.assertEqual(self.review()["reason"], "Approved overtime")
        payload = json.loads(local_store.get_approval_record(record_id)["payload"])
        self.assertEqual(payload["daily_time_exception"]["reason"], "Approved overtime")
        local_store.patch_approval_record_fields(record_id, {"TotalMinutes": 650, "Total": "10:50"})
        self.assertTrue(self.review()["required"])

    def test_approval_status_change_does_not_invalidate_confirmation(self):
        record_id = self.queue(640)
        review = self.review()
        backend.confirm_daily_time_review(self.employee, self.day, review["snapshot_hash"], "Overtime")
        local_store.mark_approval_reviewed(record_id, "approved")
        self.assertTrue(self.review()["confirmed"])

    def test_other_operator_cannot_review_an_entry(self):
        record_id = self.queue(640, EmpID=748)
        with self.assertRaisesRegex(ValueError, "does not belong"):
            backend.get_daily_time_review(self.employee, approval_id=record_id)

    def test_approval_blocks_unconfirmed_over_limit_time(self):
        record_id = self.queue(640)
        with self.assertRaisesRegex(ValueError, "provide a reason"):
            backend.approve_time_entry(record_id)
        self.assertEqual(local_store.get_approval_record(record_id)["approval_status"], "pending")

    def test_approval_allows_confirmed_over_limit_time(self):
        record_id = self.queue(640)
        review = self.review()
        backend.confirm_daily_time_review(self.employee, self.day, review["snapshot_hash"], "Overtime")
        with patch.object(backend, "approve_approval_record", return_value={"queued_id": 10}), patch.object(
            backend, "_create_item"
        ), patch.object(backend, "mark_record_synced"):
            self.assertTrue(backend.approve_time_entry(record_id)["approved"])


if __name__ == "__main__":
    unittest.main()
