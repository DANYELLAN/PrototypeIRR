import unittest
from unittest.mock import MagicMock, patch

from workflow_db import evaluate_measurements, _normalize_employee_row, complete_inspection_attempt


class InspectionEntryTests(unittest.TestCase):
    def test_history_title_comes_only_from_adp_job_title_field(self):
        employee = _normalize_employee_row({"sharepoint_item_id": 1, "fields_json": {
            "ADPJobTitleDesc": "Quality Manager", "PositionTitle": "Inspector",
        }})
        self.assertEqual(employee["adp_job_title_desc"], "Quality Manager")
        employee = _normalize_employee_row({"sharepoint_item_id": 1, "fields_json": {"PositionTitle": "Quality Manager"}})
        self.assertEqual(employee["adp_job_title_desc"], "")

    def test_normal_submission_cannot_overwrite_saved_or_other_pipe_attempts(self):
        for row in [("passed", 1), ("approved", 1), ("rework", 1), ("scrapped", 1), ("in_progress", 2), None]:
            with self.subTest(row=row):
                connection = MagicMock()
                cursor = connection.cursor.return_value.__enter__.return_value
                cursor.fetchone.return_value = row
                with patch("workflow_db.get_db_connection", return_value=connection):
                    with self.assertRaisesRegex(ValueError, "in-progress"):
                        complete_inspection_attempt(1, 1, [], "pass")
                self.assertEqual(cursor.execute.call_count, 1)
                connection.commit.assert_not_called()

    def numeric(self, value, nominal=1.234, minimum=1.23, maximum=1.24):
        return evaluate_measurements([{
            "capture_type": "numeric", "nominal": nominal, "min": minimum,
            "max": maximum, "measured_value": value,
        }])

    def test_complete_values_are_literal_without_nominal_offset(self):
        for entered, expected in [("1.234", 1.234), ("5", 5), (".234", .234), ("-1.234", -1.234), ("+1.234", 1.234)]:
            with self.subTest(entered=entered):
                self.assertEqual(self.numeric(entered)["measurements"][0]["measured_value"], expected)
        self.assertFalse(self.numeric("1.234")["has_failures"])
        self.assertTrue(self.numeric("5")["has_failures"])
        self.assertTrue(self.numeric(".234")["has_failures"])

    def test_boundaries_and_invalid_numbers(self):
        for value in ("1.23", "1.24"):
            self.assertFalse(self.numeric(value)["has_failures"])
        for value in ("", None, "NaN", "Infinity", "-Infinity", "abc", "1.25"):
            with self.subTest(value=value):
                self.assertTrue(self.numeric(value)["has_failures"])

    def test_visual_requires_an_explicit_pass(self):
        for value in (None, "", " ", "No", False):
            with self.subTest(value=value):
                result = evaluate_measurements([{"capture_type": "boolean", "measured_value": value}],
                    [{"condition": "visual_fail", "requiresApproval": True}])
                self.assertTrue(result["has_failures"])
                self.assertTrue(result["requires_approval"])
        self.assertFalse(evaluate_measurements([{"capture_type": "boolean", "measured_value": "Yes"}])["has_failures"])


if __name__ == "__main__":
    unittest.main()
