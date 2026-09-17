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
    def test_admin_update_refreshes_pending_payload_and_acumatica(self):
        captured = {}

        def capture_payload(record_id, payload):
            captured["record_id"] = record_id
            captured["payload"] = payload
            return True

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
            patch.object(backend, "patch_approval_record_payload", side_effect=capture_payload),
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
        self.assertEqual(result, {"updated": True, "pending_approval": True})
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
