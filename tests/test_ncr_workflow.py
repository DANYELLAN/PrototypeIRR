import unittest
from unittest.mock import MagicMock, patch

import ncr_workflow as ncr


def capture():
    return {
        "ncr_date": "2026-11-09", "ncr_type": "Internal", "area": "CNC Operation",
        "tier1_id": "4", "tier2_id": "1", "end_type": "Box", "grade": "P110",
        "manufacturer": "Mill", "material": "Steel", "thread": "BTS-6",
        "nonconformance": "Seal out of tolerance", "immediate_containment": "Segregated for rework",
        "notes": "Review needed", "quantity_affected": "1", "size": "2.875", "weight": "7.9",
        "engineering_report": "false", "attachments": [],
    }


class CaptureTests(unittest.TestCase):
    def test_valid_capture(self):
        result = ncr.validate_capture(capture())
        self.assertEqual(result["quantity_affected"], 1)
        self.assertFalse(result["engineering_report"])

    def test_invalid_tier_pair(self):
        data = capture()
        data["tier1_id"] = "3"
        with self.assertRaisesRegex(ValueError, "belonging"):
            ncr.validate_capture(data)

    def test_category_without_children(self):
        data = capture()
        data.update(tier1_id="8", tier2_id="")
        self.assertEqual(ncr.validate_capture(data)["tier2_id"], "")

    def test_supplier_type_does_not_require_fields_absent_from_ncr_info(self):
        data = capture()
        data["ncr_type"] = "Supplier"
        self.assertEqual(ncr.validate_capture(data)["ncr_type"], "Supplier")

    def test_only_ncr_info_fields_are_required(self):
        data = {key: value for key, value in capture().items() if key in (
            "ncr_date", "ncr_type", "area", "tier1_id", "tier2_id", "end_type", "engineering_report"
        )}
        data["ncr_details"] = "Seal failed; pipe segregated for rework."
        saved = ncr.validate_capture(data)
        self.assertEqual(saved["ncr_details"], data["ncr_details"])
        self.assertNotIn("size", saved)
        self.assertEqual(saved["attachments"], [])
        row = {"capture_data": saved, "status": "open", "production_number": "WO-1", "pipe_number": "1"}
        fields = ncr.sharepoint_fields(row, "NCR-4875", "[test]")
        self.assertEqual(fields["NCR_x0020_Details"], data["ncr_details"])
        self.assertNotIn("QuantityAffected", fields)

    def test_required_data_and_finite_positive_numbers(self):
        for key, value in [("tier1_id", ""), ("ncr_date", "2026-13-31"), ("quantity_affected", "1.5"),
                           ("size", "nan"), ("weight", "0"), ("engineering_report", ""), ("area", "Invented")]:
            with self.subTest(key=key):
                data = capture()
                data[key] = value
                with self.assertRaises(ValueError):
                    ncr.validate_capture(data)

    def test_attachments_are_bounded_and_validated(self):
        data = capture()
        data["attachments"] = [{"name": "photo.png", "content": "aGVsbG8="}]
        self.assertEqual(ncr.validate_capture(data)["attachments"][0]["name"], "photo.png")
        for files in [[{"name": "../bad", "content": "aGVsbG8="}], [{"name": "x", "content": "!!"}], data["attachments"] * 2]:
            data["attachments"] = files
            with self.assertRaises(ValueError):
                ncr.validate_capture(data)

    def test_number_uses_numeric_max_not_string_order(self):
        items = [{"fields": {"NCR_x0020_Number": x}} for x in ("NCR-999", "NCR-1000", "NCR-9999", "", "Other")]
        self.assertEqual(ncr.next_ncr_number(items), "NCR-10000")
        with self.assertRaises(ValueError):
            ncr.next_ncr_number([])

    def test_sharepoint_mapping_preserves_context(self):
        data = capture()
        data["context"] = {"inspector": "Original Inspector", "operator": "Original Operator", "shift": "Night"}
        row = {"capture_data": data, "status": "open", "disposition": "rework", "production_number": "WO-1", "pipe_number": "7", "reviewed_by": "Manager"}
        fields = ncr.sharepoint_fields(row, "NCR-10000", "[marker]")
        self.assertEqual(fields["Inspector"], "Original Inspector")
        self.assertEqual(fields["Tier_x0020_1_x0020_Defect_x0020_"], "Machining")
        self.assertEqual(fields["Disposition"], "Rework")
        self.assertIn("[marker]", fields["Reason_x0020_for_x0020_NCR"])
        self.assertIn("Immediate containment", fields["NCR_x0020_Details"])


class ManagerTests(unittest.TestCase):
    def test_it_admin_can_verify_but_cannot_send(self):
        employee = {"name": "IT Admin", "department": "IT", "role_title": "Technician", "employee_status": "Active"}
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = {"review_version": 1, "sharepoint_item_id": None, "review_status": "pending_review", "capture_data": capture()}
        with patch("workflow_db.get_employee_by_adp", return_value=employee), patch.object(ncr, "get_db_connection", return_value=connection):
            result = ncr.save_ncr_review(1, capture(), "IT", 1, approve=True)
            self.assertEqual(result["review_status"], "reviewed")
            with self.assertRaisesRegex(ValueError, "Only managers"):
                ncr.submit_ncr(1, "IT", 1)

    def test_non_manager_cannot_review_or_submit(self):
        with patch("workflow_db.get_employee_by_adp", return_value={}), patch("workflow_db.is_manager_or_supervisor", return_value=False), patch.object(ncr, "get_db_connection") as db:
            with self.assertRaises(ValueError):
                ncr.save_ncr_review(1, capture(), "1", 1)
            with self.assertRaises(ValueError):
                ncr.submit_ncr(1, "1", 1)
            db.assert_not_called()

    def test_review_uses_optimistic_version_and_preserves_context(self):
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = {"review_version": 2, "sharepoint_item_id": None, "review_status": "pending_review", "capture_data": {"context": {"inspector": "Original"}}}
        with patch("workflow_db.get_employee_by_adp", return_value={"name": "Manager"}), patch("workflow_db.is_manager_or_supervisor", return_value=True), patch.object(ncr, "get_db_connection", return_value=connection):
            with self.assertRaisesRegex(ValueError, "changed"):
                ncr.save_ncr_review(1, capture(), "1", 1)
            result = ncr.save_ncr_review(1, capture(), "1", 2, approve=True)
        self.assertEqual(result["review_status"], "reviewed")
        args = cursor.execute.call_args.args[1]
        self.assertEqual(args[0].adapted["context"]["inspector"], "Original")

    def test_simplified_review_preserves_previous_optional_details(self):
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        previous = capture()
        previous["attachments"] = [{"name": "original.txt", "content": "aGk="}]
        cursor.fetchone.return_value = {"review_version": 1, "sharepoint_item_id": None, "review_status": "pending_review", "capture_data": previous}
        with patch("workflow_db.get_employee_by_adp", return_value={"name": "Manager"}), patch("workflow_db.is_manager_or_supervisor", return_value=True), patch.object(ncr, "get_db_connection", return_value=connection):
            ncr.save_ncr_review(1, {"ncr_details":"Updated combined details"}, "1", 1)
        data = cursor.execute.call_args.args[1][0].adapted
        self.assertEqual(data["grade"], "P110")
        self.assertEqual(data["attachments"][0]["name"], "original.txt")
        self.assertEqual(data["ncr_details"], "Updated combined details")


class SubmissionTests(unittest.TestCase):
    def setUp(self):
        self.connection = MagicMock()
        self.cursor = self.connection.cursor.return_value.__enter__.return_value
        self.row = {"id": 1, "review_status": "reviewed", "review_version": 1, "sharepoint_item_id": None,
                    "submission_key": "stable-key", "capture_data": capture()}
        self.cursor.fetchone.return_value = self.row
        self.detail = {**self.row, "status": "open", "disposition": "rework", "production_number": "WO-1", "pipe_number": "7", "reviewed_by": "Manager"}
        self.patchers = [
            patch("workflow_db.get_employee_by_adp", return_value={"name": "Manager"}),
            patch("workflow_db.is_manager_or_supervisor", return_value=True),
            patch.object(ncr, "get_db_connection", return_value=self.connection),
            patch.object(ncr, "get_ncr_detail", return_value=self.detail),
            patch("sharepoint_client.get_access_token", return_value="test"),
            patch("sharepoint_client.get_site_id", return_value="site"),
            patch("sharepoint_client.graph_get", return_value={"value": [{"fields": {"NCR_x0020_Number": "NCR-4875"}}]}),
            patch("sharepoint_client.create_list_item", return_value={"id": "50"}),
        ]
        self.mocks = [p.start() for p in self.patchers]
        self.addCleanup(lambda: [p.stop() for p in self.patchers])

    def test_each_send_creates_exactly_one_item(self):
        result = ncr.submit_ncr(1, "1", 1)
        self.assertEqual(result["number"], "NCR-4876")
        self.mocks[-1].assert_called_once()
        self.assertIn("pg_advisory_lock", self.cursor.execute.call_args_list[0].args[0])

    def test_multiple_ncrs_are_sent_individually_in_sequence(self):
        self.assertEqual(ncr.submit_ncr(1, "1", 1)["number"], "NCR-4876")
        self.row.update(id=2, submission_key="second-key")
        self.mocks[-2].return_value = {"value": [{"fields": {"NCR_x0020_Number": "NCR-4876"}}]}
        self.assertEqual(ncr.submit_ncr(2, "1", 1)["number"], "NCR-4877")
        self.assertEqual(self.mocks[-1].call_count, 2)

    def test_stale_send_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "changed"):
            ncr.submit_ncr(1, "1", 0)
        self.mocks[-1].assert_not_called()

    def test_requires_verification(self):
        self.row["review_status"] = "pending_review"
        with self.assertRaisesRegex(ValueError, "verify"):
            ncr.submit_ncr(1, "1", 1)
        self.mocks[-1].assert_not_called()

    def test_sent_record_is_not_resent(self):
        self.row.update(sharepoint_item_id="50", sharepoint_ncr_number="NCR-4876", review_status="sent")
        self.assertTrue(ncr.submit_ncr(1, "1", 1)["already_sent"])
        self.mocks[-1].assert_not_called()

    def test_unknown_send_is_reconciled_without_creating(self):
        self.row["review_status"] = "submission_unknown"
        self.mocks[-2].return_value = {"value": [{"id": "50", "fields": {"NCR_x0020_Number": "NCR-4876", "Reason_x0020_for_x0020_NCR": "[InspectionApp NCR stable-key]"}}]}
        self.assertEqual(ncr.submit_ncr(1, "1", 1)["item_id"], "50")
        self.mocks[-1].assert_not_called()

    def test_missing_unknown_send_is_not_retried(self):
        self.row["review_status"] = "submission_unknown"
        with self.assertRaisesRegex(ValueError, "uncertain"):
            ncr.submit_ncr(1, "1", 1)
        self.mocks[-1].assert_not_called()

    def test_timeout_marks_uncertain(self):
        self.mocks[-1].side_effect = TimeoutError("timeout")
        with self.assertRaisesRegex(ValueError, "could not be confirmed"):
            ncr.submit_ncr(1, "1", 1)
        self.assertTrue(any("submission_unknown" in call.args[0] for call in self.cursor.execute.call_args_list))

    def test_attachment_access_checked_before_create(self):
        self.row["capture_data"]["attachments"] = [{"name": "x.txt", "content": "aGk="}]
        with patch.object(ncr, "attachment_headers", side_effect=ValueError("Not connected")):
            with self.assertRaises(ValueError):
                ncr.submit_ncr(1, "1", 1)
        self.mocks[-1].assert_not_called()

    def test_partial_attachment_delivery_uses_existing_item(self):
        self.row.update(review_status="attachments_pending", sharepoint_item_id="50", sharepoint_ncr_number="NCR-4876")
        self.row["capture_data"]["attachments"] = [{"name": "x.txt", "content": "aGk="}]
        self.mocks[-2].return_value = {"value": [{"id": "50", "fields": {"NCR_x0020_Number": "NCR-4876", "Reason_x0020_for_x0020_NCR": "[InspectionApp NCR stable-key]"}}]}
        with patch.object(ncr, "attachment_headers", return_value={}), patch.object(ncr, "upload_attachments") as upload:
            self.assertEqual(ncr.submit_ncr(1, "1", 1)["item_id"], "50")
            upload.assert_called_once()
        self.mocks[-1].assert_not_called()

    def test_rejected_create_stays_reviewed(self):
        self.mocks[-1].side_effect = ValueError("Graph request failed (400) for test: invalid field")
        with self.assertRaisesRegex(ValueError, "rejected"):
            ncr.submit_ncr(1, "1", 1)
        self.assertTrue(any("review_status = 'reviewed'" in call.args[0] for call in self.cursor.execute.call_args_list))


if __name__ == "__main__":
    unittest.main()
