import unittest
from time_entry_app.bridge import cnc_time_backend as b

class CNCStationEligibilityTests(unittest.TestCase):
    def fields(self,station,detail='Machining'):
        return {'MachineNo':station,'DetailsType':detail,'DetailsTypeII':'M1','TotalMinutes':15,'Quantity':2,'EmpID':'1058','LaborDate':'2026-11-05'}
    def test_cnc_1_through_6_accept_employees_without_machinist_flag(self):
        for employee in [{},{'machinist':False},{'machinist':None},{'machinist':True}]:
            for station in ['1','2','3','4','5','6','CNC 1','E-6']:
                with self.subTest(employee=employee,station=station):
                    payload=b._attach_acumatica_payload({},employee,self.fields(station))
                    self.assertIn('acumatica_labor_transaction',payload)
    def test_other_stations_and_unmapped_machine_numbers_remain_excluded(self):
        for station in ['',None,'0','7','CNC 7','E-7','Line 1 - Inspection 1','Line 1 - Sandblast 1','Line 1 - Drift','Line 2 - Stenciling']:
            with self.subTest(station=station):
                payload={'acumatica_labor_transaction':{'machine_no':'1'},'acumatica_labor_transactions':[{'machine_no':'1'}]}
                b._attach_acumatica_payload(payload,{'machinist':True},self.fields(station))
                self.assertNotIn('acumatica_labor_transaction',payload)
                self.assertNotIn('acumatica_labor_transactions',payload)
    def test_manual_and_downtime_payloads_preserve_existing_calculations(self):
        direct=b._attach_acumatica_payload({}, {'machinist':False}, self.fields('1'))
        self.assertEqual(direct['acumatica_labor_transaction']['labor_minutes'],15)
        downtime=b._attach_acumatica_payload({}, {}, self.fields('1','DT'),'M1')
        self.assertEqual(len(downtime['acumatica_labor_transactions']),2)
        self.assertEqual(sum(t['labor_minutes'] for t in downtime['acumatica_labor_transactions']),15)
    def test_approval_refresh_adds_payload_for_unflagged_cnc_employee_and_removes_it_after_station_change(self):
        payload={'employee':{'machinist':False}}
        b._refresh_approval_acumatica_payload(payload,self.fields('1'))
        self.assertIn('acumatica_labor_transaction',payload)
        b._refresh_approval_acumatica_payload(payload,self.fields('Line 1 - Inspection 1'))
        self.assertNotIn('acumatica_labor_transaction',payload)

if __name__=='__main__':unittest.main()
