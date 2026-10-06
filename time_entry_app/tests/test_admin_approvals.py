import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from time_entry_app import local_store
from time_entry_app.bridge import cnc_time_backend as backend


class AdminApprovalStoreTests(unittest.TestCase):
    def test_approval_handoff_creates_one_downstream_record(self):
        temp_dir = Path(__file__).resolve().parent / ".tmp"
        temp_dir.mkdir(exist_ok=True)
        db_path = temp_dir / f"cnc_time_test_{uuid.uuid4().hex}.db"
        machines_path = temp_dir / f"machines_{uuid.uuid4().hex}.json"
        try:
            with (
                patch.object(local_store, "DB_PATH", db_path),
                patch.object(local_store, "LOCAL_MACHINE_CONFIG", machines_path),
            ):
                approval_id = local_store.queue_approval(
                    "manual_time",
                    {"fields": {"Title": "EWO26-00001", "TotalMinutes": 75}},
                    machine_no="1",
                    emp_id="747",
                    employee_name="Alex Simoneaux",
                )

                result = local_store.approve_approval_record(
                    approval_id,
                    reviewer={"emp_id": "101", "full_name": "Production Manager"},
                    note="Looks good",
                )
                duplicate_result = local_store.approve_approval_record(
                    approval_id,
                    reviewer={"emp_id": "101", "full_name": "Production Manager"},
                )

                pending_records = local_store.list_pending_records(limit=10)
                approval = local_store.get_approval_record(approval_id)
        finally:
            for candidate in (db_path, db_path.with_name(f"{db_path.name}-wal"), db_path.with_name(f"{db_path.name}-shm"), machines_path):
                try:
                    candidate.unlink(missing_ok=True)
                except PermissionError:
                    pass

        self.assertIsNotNone(result)
        self.assertEqual(result["queued_id"], pending_records[0]["id"])
        self.assertIsNone(duplicate_result)
        self.assertEqual(len(pending_records), 1)
        self.assertEqual(pending_records[0]["source"], "approved")
        self.assertEqual(approval["approval_status"], "approved")
        self.assertEqual(approval["reviewed_by_emp_id"], "101")
        self.assertEqual(approval["review_note"], "Looks good")


class AdminApprovalBackendTests(unittest.TestCase):
    def test_bulk_approval_deduplicates_and_continues_after_failure(self):
        reviewer = {"emp_id": "100"}
        with patch.object(backend, "approve_time_entry", side_effect=[
            {"approved": True, "queued": True}, ValueError("Already reviewed"),
            {"approved": True, "synced": True},
        ]) as approve:
            result = backend.approve_all_time_entries(["1", "1", "2", "3"], reviewer=reviewer)
        self.assertEqual(result["approved"], 2)
        self.assertEqual(result["queued"], 1)
        self.assertEqual(result["synced"], 1)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(result["errors"][0]["approval_id"], 2)
        self.assertEqual([call.args[0] for call in approve.call_args_list], [1, 2, 3])
        self.assertTrue(all(call.kwargs["reviewer"] == reviewer for call in approve.call_args_list))

    def test_bulk_approval_validates_all_ids_before_approving(self):
        with patch.object(backend, "approve_time_entry") as approve:
            for ids in (None, "12", [], [1, "invalid"], [0], [True], [1.5], [1] * 501):
                with self.subTest(ids=ids), self.assertRaises(ValueError):
                    backend.approve_all_time_entries(ids)
            approve.assert_not_called()

    def test_retry_all_uses_one_failed_only_sync(self):
        with patch.object(backend, "sync_approved_entries", return_value={"sent": 2}) as sync:
            self.assertEqual(backend.retry_all_acumatica_entries(actor={"emp_id": "100"}), {"sent": 2})
        sync.assert_called_once_with(trigger_name="manual_retry_all", actor={"emp_id": "100"}, failed_only=True)

    def test_retry_acumatica_entry_targets_only_failed_approval(self):
        with (
            patch.object(
                backend,
                "get_approval_record",
                return_value={"id": 45, "approval_status": "approved"},
            ),
            patch.object(
                backend,
                "get_acumatica_sync_item",
                return_value={"status": "failed"},
            ),
            patch.object(
                backend,
                "sync_approved_entries",
                return_value={"status": "completed", "sent": 1, "failed": 0},
            ) as mock_sync,
        ):
            result = backend.retry_acumatica_entry(45, actor={"emp_id": "100"})

        self.assertEqual(result["sent"], 1)
        mock_sync.assert_called_once_with(
            trigger_name="manual_retry",
            actor={"emp_id": "100"},
            approval_id=45,
        )

    def test_retry_acumatica_entry_rejects_non_failed_approval(self):
        with (
            patch.object(
                backend,
                "get_approval_record",
                return_value={"id": 45, "approval_status": "approved"},
            ),
            patch.object(
                backend,
                "get_acumatica_sync_item",
                return_value={"status": "sent"},
            ),
        ):
            with self.assertRaisesRegex(ValueError, "Only a failed"):
                backend.retry_acumatica_entry(45)

    def test_approval_rebuilds_acumatica_payload_from_reviewed_fields(self):
        captured = {}
        record = {
            "id": 45,
            "record_type": "stop_time_entry",
            "approval_status": "pending",
            "payload": (
                '{"fields":{"ProductionNo":"EWO26-00090","OperationID":"0050",'
                '"DetailsType":"Machining","Quantity":1,"TotalMinutes":15,"Total":"00:15",'
                '"EmployeeID":"001005","MachineNo":"1","LaborType":"Direct","Shift":"40"},'
                '"employee":{"machinist":true},'
                '"acumatica_labor_transaction":{"labor_minutes":0}}'
            ),
        }

        def capture_payload(record_id, payload):
            captured["record_id"] = record_id
            captured["payload"] = payload
            return True

        with (
            patch.object(backend, "get_approval_record", return_value=record),
            patch.object(backend, "patch_approval_record_payload", side_effect=capture_payload),
            patch.object(backend, "approve_approval_record", return_value={"queued_id": 91}),
            patch.object(backend, "_create_item"),
            patch.object(backend, "mark_record_synced"),
        ):
            result = backend.approve_time_entry(45)

        transaction = captured["payload"]["acumatica_labor_transaction"]
        self.assertTrue(result["approved"])
        self.assertEqual(captured["record_id"], 45)
        self.assertEqual(transaction["labor_minutes"], 15)
        self.assertEqual(transaction["labor_time"], "00:15")
        self.assertEqual(transaction["labor_amount"], 30.83)

    def test_admin_update_refreshes_pending_payload_and_acumatica(self):
        captured = {}

        def capture_payload(record_id, payload, editor=None, reason=None):
            captured["record_id"] = record_id
            captured["payload"] = payload
            return {"approval_status": "pending", "revision_no": 2, "had_sync_record": False}

        record = {
            "id": 44,
            "record_type": "manual_time",
            "approval_status": "pending",
            "payload": (
                '{"fields":{"ProductionNo":"EWO26-00001","Title":"EWO26-00001","OperationID":"0010",'
                '"DetailsType":"Machining","Quantity":1,"TotalMinutes":60,"Total":"01:00","EmpID":747,'
                '"EmployeeID":"747","OperatorsName":"Alex Simoneaux","MachineNo":"1","LaborType":"Direct"}}'
            ),
        }
        operation = {
            "order_type": "EN",
            "inventory_id": "EN00049",
            "description": "Edited Part",
            "operation_description": "Edited Operation",
        }

        with (
            patch.object(backend, "get_approval_record", return_value=record),
            patch.object(backend, "_get_operation", return_value=operation),
            patch.object(backend, "update_approval_with_revision", side_effect=capture_payload),
        ):
            result = backend.update_approval_time_entry(
                44,
                {
                    "production_number": "EWO26-00002",
                    "operation_id": "0020",
                    "detail_type": "Machining",
                    "quantity": "3",
                    "total_hours": "2",
                    "total_minutes_remainder": "15",
                    "machine_no": "2",
                    "operator_name": "Edited Operator",
                    "emp_id": "888",
                    "comments": "Manager corrected",
                },
            )

        fields = captured["payload"]["fields"]
        acumatica = captured["payload"]["acumatica_labor_transaction"]
        self.assertTrue(result["updated"])
        self.assertTrue(result["pending_approval"])
        self.assertEqual(result["revision_no"], 2)
        self.assertEqual(captured["record_id"], 44)
        self.assertEqual(fields["ProductionNo"], "EWO26-00002")
        self.assertEqual(fields["OperationID"], "0020")
        self.assertEqual(fields["Quantity"], 3)
        self.assertEqual(fields["TotalMinutes"], 135)
        self.assertEqual(fields["Total"], "02:15")
        self.assertEqual(fields["MachineNo"], "2")
        self.assertEqual(fields["OperatorsName"], "Edited Operator")
        self.assertEqual(fields["EmpID"], 888)
        self.assertEqual(fields["TranDescription"], "Manager corrected")
        self.assertEqual(acumatica["production_number"], "EWO26-00002")
        self.assertEqual(acumatica["operation_id"], "0020")
        self.assertEqual(acumatica["quantity"], 3)
        self.assertEqual(acumatica["labor_minutes"], 135)


if __name__ == "__main__":
    unittest.main()
