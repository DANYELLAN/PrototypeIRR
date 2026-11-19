import unittest
from unittest.mock import patch
from admin_inspections import require_admin, save_live_draft, get_admin_inspection_data


class AdminInspectionTests(unittest.TestCase):
    def test_it_admin_is_authorized(self):
        with patch("workflow_db.get_employee_by_adp", return_value={"name":"IT", "department":"IT", "employee_status":"Active"}):
            require_admin("IT")

    def test_non_admin_and_inactive_admin_cannot_read_monitor(self):
        for employee in ({"name":"Inspector"}, {"name":"IT", "department":"IT", "employee_status":"Inactive"}, None):
            with patch("workflow_db.get_employee_by_adp", return_value=employee), patch("admin_inspections.get_db_connection") as db:
                with self.assertRaisesRegex(ValueError, "Admin access"):
                    get_admin_inspection_data("TEST")
                db.assert_not_called()

    def test_invalid_draft_is_rejected_before_database_access(self):
        for readings in ([], {"x":"1"}, {"1":"1"*65}, {str(i):"1" for i in range(101)}):
            with patch("admin_inspections.get_db_connection") as db:
                with self.assertRaises(ValueError):
                    save_live_draft(1, 1, readings)
                db.assert_not_called()
