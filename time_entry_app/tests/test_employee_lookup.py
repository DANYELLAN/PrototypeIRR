import unittest

from time_entry_app.bridge.cnc_time_backend import lookup_employee_by_adp


class EmployeeLookupTests(unittest.TestCase):
    def test_lookup_employee_by_adp_returns_active_ennis_machinist(self):
        employees = [
            {
                "id": 1,
                "fields": {
                    "ADPEmpNumber": "12345",
                    "Full_x0020_Name": "Robert Thompson",
                    "Branches": "Ennis",
                    "Status": "Active",
                    "Machinist": True,
                },
            }
        ]

        result = lookup_employee_by_adp("12345", employees)

        self.assertIsNotNone(result)
        self.assertEqual(result["full_name"], "Robert Thompson")
        self.assertEqual(result["emp_id"], "12345")


if __name__ == "__main__":
    unittest.main()
