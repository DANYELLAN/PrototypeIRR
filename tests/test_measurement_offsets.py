import unittest
from workflow_db import evaluate_measurements, _prepare_local_recipe_payload
from node_app.bridge.lunch_inspections import evaluate_full

class MeasurementOffsetTests(unittest.TestCase):
    def row(self,value,nominal=2.881):
        return {'element_sequence':1,'element_description':'First thread diameter','capture_type':'numeric','nominal':nominal,'min':2.878,'max':2.886,'measured_value':value}
    def test_full_values_and_user_example(self):
        for entered,actual in [('2.879',2.879),('-.002',2.879),('-.003',2.878),('+.005',2.886),('+0.001',2.882),('-0',2.881)]:
            with self.subTest(entered=entered):
                result=evaluate_measurements([self.row(entered)])
                self.assertEqual(result['measurements'][0]['measured_value'],actual)
                self.assertFalse(result['has_failures'])
    def test_offset_outside_tolerance_and_invalid_inputs_fail(self):
        for entered in ['-.004','.006','+.006','+','-','.','NaN','Infinity','abc']:
            with self.subTest(entered=entered):self.assertTrue(evaluate_measurements([self.row(entered)])['has_failures'])
    def test_unsigned_zero_means_nominal_including_lunch(self):
        for entered in ['0', '.000', '0.000', ' 0 ', '+0', '-0']:
            with self.subTest(entered=entered):
                result=evaluate_measurements([self.row(entered)])
                self.assertEqual(result['measurements'][0]['measured_value'],2.881)
                self.assertFalse(result['has_failures'])
                self.assertEqual(evaluate_measurements(result['measurements'])['measurements'][0]['measured_value'],2.881)
                lunch=evaluate_full([self.row('')],{'1':entered},require_explicit_visual=True)
                self.assertEqual(lunch['measurements'][0]['measured_value'],2.881)
                self.assertFalse(lunch['has_failures'])
    def test_zero_without_nominal_and_normalized_numeric_zero_remain_actual_zero(self):
        for nominal in [None, '']:
            self.assertEqual(evaluate_measurements([self.row('.000',nominal)])['measurements'][0]['measured_value'],0)
        self.assertEqual(evaluate_measurements([self.row(0)])['measurements'][0]['measured_value'],0)
        first=evaluate_measurements([self.row('-2.881')])['measurements']
        self.assertEqual(evaluate_measurements(first)['measurements'][0]['measured_value'],0)
    def test_range_offsets_require_an_explicit_sign(self):
        for entered, actual, passes in [('+.002',2.883,True),('-.002',2.879,True),('+.005',2.886,True),('+.006',2.887,False),('.002',.002,False),('.005',.005,False)]:
            with self.subTest(entered=entered):
                row={**self.row(entered),'min':2.876,'max':2.886}
                result=evaluate_measurements([row])
                self.assertEqual(result['measurements'][0]['measured_value'],actual)
                self.assertEqual(not result['has_failures'],passes)
    def test_legacy_float_limits_are_inclusive_without_accepting_outside_values(self):
        for entered, passes in [('+.005',True),('2.886',True),('-.003',True),('2.878',True),('+.005001',False),('-.003001',False),('.005',False),('.005001',False)]:
            with self.subTest(entered=entered):
                row={**self.row(entered),'min':2.8779999999999997,'max':2.8859999999999997}
                result=evaluate_measurements([row])
                self.assertEqual(not result['has_failures'],passes)
                lunch=evaluate_full([row],{'1':entered},require_explicit_visual=True)
                self.assertEqual(not lunch['has_failures'],passes)
    def test_new_recipe_limits_use_decimal_arithmetic(self):
        for mode, extra, expected in [
            ('asymmetric_tolerance',{'plus_tolerance':'.005','minus_tolerance':'.003'},(2.878,2.886)),
            ('nominal_tolerance',{'tolerance_digits':'005','tolerance_decimal_places':3},(2.876,2.886)),
        ]:
            with self.subTest(mode=mode):
                payload={'size_label':'2.875','rows':[{'element_description':'Diameter','measurement_mode':mode,'gauge':'MRP','nominal_value':'2.881',**extra}]}
                row=_prepare_local_recipe_payload(payload)['processed_rows'][0]
                self.assertEqual((row['min_value'],row['max_value']),expected)
    def test_normalized_actual_values_are_never_offset_twice(self):
        first=evaluate_measurements([self.row('+.002',.1)])['measurements']
        self.assertEqual(evaluate_measurements(first)['measurements'][0]['measured_value'],.102)
        first=evaluate_measurements([self.row('-.002')])['measurements']
        self.assertEqual(evaluate_measurements(first)['measurements'][0]['measured_value'],2.879)
        first=evaluate_measurements([{**self.row('-2',1),'min':-2,'max':2}])['measurements']
        self.assertEqual(evaluate_measurements(first)['measurements'][0]['measured_value'],-1)
    def test_signed_values_without_a_nominal_remain_full_values(self):
        self.assertEqual(evaluate_measurements([self.row('-.002',None)])['measurements'][0]['measured_value'],-.002)
        self.assertEqual(evaluate_measurements([self.row('.002',None)])['measurements'][0]['measured_value'],.002)
    def test_lunch_uses_the_same_nominal_calculation(self):
        plan=[self.row('')]
        result=evaluate_full(plan,{'1':'-.002'},require_explicit_visual=True)
        self.assertFalse(result['has_failures'])
        self.assertEqual(result['measurements'][0]['measured_value'],2.879)

if __name__=='__main__':unittest.main()
