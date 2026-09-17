import unittest
from unittest.mock import patch

from time_entry_app.bridge import cnc_time_backend as backend


class DowntimeReasonTests(unittest.TestCase):
    def test_downtime_reason_code_accepts_display_label(self):
        self.assertEqual(backend._downtime_reason_code("DT- OPS (OPERATIONS)"), "OPS")

    @patch.object(backend, "queue_approval", return_value=123)
    @patch.object(
        backend,
        "_get_operation_for_employee",
        return_value={
            "order_type": "EN",
            "inventory_id": "EN00049",
            "description": "E-5",
            "operation_description": "TURN ...",
        },
    )
    @patch.object(backend, "ensure_db")
    def test_submit_misc_time_uses_acumatica_downtime_fields(
        self,
        _mock_ensure_db,
        _mock_get_operation,
        mock_queue_approval,
    ):
        employee = {"emp_id": "747", "full_name": "Alex Simoneaux", "machinist": True}

        result = backend.submit_misc_time(
            employee,
            shift_id=40,
            machine_no="1",
            detail_type_ii="M1",
            production_number="EWO26-00094",
            operation_id="0010",
            hours=1,
            minutes=0,
            comments="",
        )

        self.assertEqual(result, {"submitted": False, "pending_approval": True, "approval_id": 123})
        approval_payload = mock_queue_approval.call_args.args[1]
        fields = approval_payload["fields"]
        self.assertEqual(fields["DetailsType"], "DT")
        self.assertEqual(fields["DetailsTypeII"], "M1")
        self.assertEqual(fields["TranDescription"], "M1")
        self.assertEqual(fields["LaborType"], "Direct")
        self.assertEqual(fields["ProductionNo"], "EWO26-00094")
        acumatica_row = approval_payload["acumatica_labor_transaction"]
        self.assertEqual(acumatica_row["detail_type"], "DT")
        self.assertEqual(acumatica_row["reason_code"], "M1")
        self.assertEqual(acumatica_row["warehouse"], "EN-FG SSOT")
        self.assertEqual(acumatica_row["location"], "CUST REC")
        acumatica_rows = approval_payload["acumatica_labor_transactions"]
        self.assertEqual(len(acumatica_rows), 2)
        self.assertEqual(acumatica_rows[0]["quantity"], 1)
        self.assertEqual(acumatica_rows[0]["labor_time"], "01:00")
        self.assertEqual(acumatica_rows[1]["quantity"], -1)
        self.assertEqual(acumatica_rows[1]["labor_time"], "-00:01")
        self.assertEqual(acumatica_rows[1]["labor_minutes"], -1)
        self.assertEqual(acumatica_rows[1]["reason_code"], "M1")

    @patch.object(backend, "queue_approval", return_value=123)
    @patch.object(
        backend,
        "_get_operation_for_employee",
        return_value={
            "order_type": "EN",
            "inventory_id": "EN00049",
            "description": "E-5",
            "operation_description": "TURN ...",
        },
    )
    @patch.object(backend, "ensure_db")
    def test_non_cnc_station_time_does_not_include_acumatica_payload(
        self,
        _mock_ensure_db,
        _mock_get_operation,
        mock_queue_approval,
    ):
        employee = {"emp_id": "747", "full_name": "Alex Simoneaux", "machinist": True}

        backend.submit_misc_time(
            employee,
            shift_id=40,
            machine_no="Line 1 - Inspection 1",
            detail_type_ii="M1",
            production_number="EWO26-00094",
            operation_id="0010",
            hours=1,
            minutes=0,
            comments="",
        )

        approval_payload = mock_queue_approval.call_args.args[1]
        self.assertNotIn("acumatica_labor_transaction", approval_payload)
        self.assertNotIn("acumatica_labor_transactions", approval_payload)

    @patch.object(backend, "list_pending_records", return_value=[{"id": 1, "record_type": "misc_time"}])
    @patch.object(backend, "ensure_db")
    def test_pending_write_sync_skips_when_sharepoint_writes_are_disabled(
        self, _mock_ensure_db, _mock_list_pending_records
    ):
        original_value = backend.SHAREPOINT_WRITES_ENABLED
        backend.SHAREPOINT_WRITES_ENABLED = False
        try:
            result = backend.sync_pending_writes_to_sharepoint()
        finally:
            backend.SHAREPOINT_WRITES_ENABLED = original_value

        self.assertEqual(result["synced"], 0)
        self.assertEqual(result["skipped"], 1)
        self.assertTrue(result["dry_run"])


if __name__ == "__main__":
    unittest.main()
