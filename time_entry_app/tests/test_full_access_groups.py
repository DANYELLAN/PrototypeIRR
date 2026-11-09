import unittest
from unittest.mock import patch
from time_entry_app.bridge import cnc_time_backend as b

class FullAccessGroupTests(unittest.TestCase):
    def setUp(self):
        for key,value in [('FULL_ACCESS_ADP_NUMBERS',set()),('FULL_ACCESS_DEPARTMENTS',{'integration technologist 1'}),('FULL_ACCESS_JOB_TITLES',{'integration technologist i','integration technologist-ennis'}),('FULL_ACCESS_DEPARTMENT_KEYWORDS',())]:
            p=patch.object(b,key,value);p.start();self.addCleanup(p.stop)
    def lookup(self,**updates):
        fields={'ADPEmpNumber':'98765','Full_x0020_Name':'Test employee','Status':'Active','Branches':'Ennis','DepartmentName':'Quality','PositionTitle':'Inspector',**updates}
        return b.lookup_employee_by_adp('98765',[{'id':'1','fields':fields}])
    def test_exact_department_grants_full_access_with_a_different_title(self):
        self.assertEqual(self.lookup(DepartmentName='Integration Technologist 1')['roles'],['operator','exporter','approver','it'])
    def test_either_position_or_adp_job_title_grants_same_access(self):
        for fields in [{'PositionTitle':'Integration Technologist-Ennis'},{'ADPJobTitleDesc':'Integration Technologist I'}]:
            with self.subTest(fields=fields):self.assertEqual(self.lookup(**fields)['roles'],['operator','exporter','approver','it'])
    def test_case_and_spacing_are_normalized(self):
        self.assertIn('it',self.lookup(DepartmentName='  INTEGRATION   TECHNOLOGIST 1 ')['roles'])
    def test_partial_titles_and_other_departments_do_not_match(self):
        for fields in [{'DepartmentName':'Office'},{'DepartmentName':'Ennis Office'},{'PositionTitle':'Integration Technologist II'},{'PositionTitle':'Senior Integration Technologist I'},{'DepartmentName':'Integration Technologist 10'}]:
            with self.subTest(fields=fields):self.assertEqual(self.lookup(**fields)['roles'],['operator'])
    def test_inactive_and_non_ennis_records_stay_ineligible(self):
        for fields in [{'Status':'Inactive'},{'Branches':'Houston'}]:
            self.assertIsNone(self.lookup(DepartmentName='Integration Technologist 1',**fields))

class IntegrationTechnologistKeywordTests(FullAccessGroupTests):
    def test_keyword_includes_all_levels_and_location_suffixes(self):
        with patch.object(b,'FULL_ACCESS_DEPARTMENT_KEYWORDS',('integration technologist',)):
            for fields in [{'PositionTitle':'Integration Technologist II'},{'ADPJobTitleDesc':'Integration Technologist III'},{'PositionTitle':'Senior Integration Technologist I'},{'DepartmentName':'Integration Technologist 2'},{'PositionTitle':'Integration Technologist - Ennis'}]:
                with self.subTest(fields=fields):self.assertEqual(self.lookup(**fields)['roles'],['operator','exporter','approver','it'])
    def test_keyword_does_not_grant_other_office_or_quality_employees_access(self):
        with patch.object(b,'FULL_ACCESS_DEPARTMENT_KEYWORDS',('integration technologist',)):
            self.assertEqual(self.lookup(DepartmentName='Office')['roles'],['operator'])
            self.assertEqual(self.lookup(ADPJobTitleDesc='Quality Inspector')['roles'],['operator'])
            self.assertIsNone(self.lookup(ADPJobTitleDesc='Integration Technologist II',Status='Inactive'))


if __name__=='__main__':unittest.main()
