import unittest

from time_entry_app.bridge.cnc_time_backend import lookup_employee_by_adp


class EmployeeLookupTests(unittest.TestCase):
    def test_lookup_employee_by_adp_allows_any_active_ennis_employee(self):
        employees = [
            {
                "id": 2,
                "fields": {
                    "ADPEmpNumber": "4567",
                    "Full_x0020_Name": "Taylor Employee",
                    "Branches": "Ennis",
                    "Status": "Active",
                    "Machinist": False,
                    "DepartmentName": "Quality",
                },
            }
        ]

        result = lookup_employee_by_adp("4567", employees)

        self.assertIsNotNone(result)
        self.assertEqual(result["full_name"], "Taylor Employee")
        self.assertEqual(result["roles"], ["operator"])

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

    def test_lookup_employee_by_adp_ignores_leading_zeroes(self):
        employees = [
            {
                "id": 1,
                "fields": {
                    "ADPEmpNumber": "747",
                    "Full_x0020_Name": "Alex Simoneaux",
                    "Branches": "Ennis",
                    "Status": "Active",
                    "Machinist": True,
                },
            }
        ]

        result = lookup_employee_by_adp("000747", employees)

        self.assertIsNotNone(result)
        self.assertEqual(result["emp_id"], "747")

    def test_lookup_employee_by_adp_allows_active_ennis_logistics_exporter(self):
        employees = [
            {
                "id": 603,
                "fields": {
                    "field_0": "883",
                    "Title": "Martha Y Medrano",
                    "field_2": "Martha",
                    "field_4": "Medrano",
                    "field_6": "Logistics 1 - Administrative Asst.",
                    "field_19": "Active",
                    "field_20": "Tubular",
                    "field_22": "Ennis",
                    "Machinist": False,
                    "DepartmentName": "Logistics 1 - Ennis Days",
                    "PositionTitle": "Administrative Assistant Shipping & Receiving",
                },
            }
        ]

        result = lookup_employee_by_adp("883", employees)

        self.assertIsNotNone(result)
        self.assertEqual(result["full_name"], "Martha Y Medrano")
        self.assertEqual(result["roles"], ["operator", "exporter", "approver"])

    def test_lookup_employee_by_adp_allows_manager_to_approve_and_operate(self):
        employees = [
            {
                "id": 701,
                "fields": {
                    "ADPEmpNumber": "990",
                    "Full_x0020_Name": "Chris Manager",
                    "Branches": "Ennis",
                    "Status": "Active",
                    "Machinist": False,
                    "DepartmentName": "Production Manager",
                    "PositionTitle": "CNC Supervisor",
                },
            }
        ]

        result = lookup_employee_by_adp("990", employees)

        self.assertIsNotNone(result)
        self.assertEqual(result["full_name"], "Chris Manager")
        self.assertEqual(result["roles"], ["operator", "approver"])

    def test_lookup_employee_by_adp_grants_it_all_permissions(self):
        employees = [
            {
                "id": 702,
                "fields": {
                    "ADPEmpNumber": "991",
                    "Full_x0020_Name": "IT Employee",
                    "Branches": "Ennis",
                    "Status": "Active",
                    "DepartmentName": "IT",
                    "PositionTitle": "Systems Administrator",
                },
            }
        ]

        result = lookup_employee_by_adp("991", employees)

        self.assertIsNotNone(result)
        self.assertEqual(result["roles"], ["operator", "exporter", "approver"])

    def test_it_keyword_does_not_match_inside_quality(self):
        employees = [
            {
                "id": 703,
                "fields": {
                    "ADPEmpNumber": "992",
                    "Full_x0020_Name": "Quality Employee",
                    "Branches": "Ennis",
                    "Status": "Active",
                    "DepartmentName": "Quality",
                    "PositionTitle": "Inspector",
                },
            }
        ]

        result = lookup_employee_by_adp("992", employees)

        self.assertIsNotNone(result)
        self.assertEqual(result["roles"], ["operator"])

    def test_lookup_employee_by_adp_rejects_inactive_non_ennis_and_missing_adp_records(self):
        employees = [
            {
                "id": 1,
                "fields": {
                    "ADPEmpNumber": "100",
                    "Full_x0020_Name": "Inactive Employee",
                    "Branches": "Ennis",
                    "Status": "Inactive",
                },
            },
            {
                "id": 2,
                "fields": {
                    "ADPEmpNumber": "200",
                    "Full_x0020_Name": "Houston Employee",
                    "Branches": "Houston",
                    "Status": "Active",
                },
            },
            {
                "id": 3,
                "fields": {
                    "ADPEmpNumber": "",
                    "Full_x0020_Name": "Missing ADP",
                    "Branches": "Ennis",
                    "Status": "Active",
                },
            },
        ]

        self.assertIsNone(lookup_employee_by_adp("100", employees))
        self.assertIsNone(lookup_employee_by_adp("200", employees))
        self.assertIsNone(lookup_employee_by_adp("", employees))


if __name__ == "__main__":
    unittest.main()
