import unittest

from workflow_db import evaluate_measurements


class MeasurementEntryTests(unittest.TestCase):
    def evaluate(self, value, minimum=0.05, maximum=0.053, nominal=0.05):
        return evaluate_measurements([{
            "capture_type": "numeric", "measured_value": value,
            "nominal": nominal, "min": minimum, "max": maximum,
        }])["measurements"][0]

    def test_leading_decimal_implies_zero(self):
        for value in (".050", "0.050", ".05", "0.05"):
            with self.subTest(value=value):
                row = self.evaluate(value)
                self.assertEqual(row["measured_value"], 0.05)
                self.assertEqual(row["pass_fail"], "Pass")

    def test_thread_height_boundaries(self):
        for value, expected in ((".049", "Fail"), (".050", "Pass"), (".053", "Pass"), (".054", "Fail")):
            with self.subTest(value=value):
                self.assertEqual(self.evaluate(value)["pass_fail"], expected)

    def test_full_readings_are_never_offsets(self):
        self.assertEqual(self.evaluate("50")["measured_value"], 50)
        self.assertEqual(self.evaluate("+.001")["measured_value"], 0.051)
        self.assertEqual(self.evaluate("2.590", 2.59, 2.6, None)["pass_fail"], "Pass")
        self.assertEqual(self.evaluate("60", 2.59, 2.6, None)["pass_fail"], "Fail")

    def test_signed_and_zero_readings(self):
        for value in ("-.001", "-0.001", ".001", "0"):
            with self.subTest(value=value):
                self.assertEqual(self.evaluate(value, -0.002, 0.002, 0)["pass_fail"], "Pass")

    def test_explicit_signed_deviations_from_nominal(self):
        for value, expected in (("-.003", "Pass"), ("-.004", "Pass"), ("+.001", "Fail"), ("-.005", "Fail"), ("3.0294", "Pass")):
            self.assertEqual(self.evaluate(value, 3.0254, 3.0294, 3.0294)["pass_fail"], expected)
        self.assertEqual(self.evaluate("-.003", 3.0254, 3.0294, 3.0294)["measured_value"], 3.0264)

    def test_unsigned_zero_is_nominal_and_nonzero_offsets_still_need_sign(self):
        for value in ('0', '.000', '0.000', '+0', '-0'):
            with self.subTest(value=value):
                row = self.evaluate(value, 2.876, 2.886, 2.881)
                self.assertEqual(row['measured_value'], 2.881)
                self.assertEqual(row['pass_fail'], 'Pass')
                again = evaluate_measurements([{'capture_type': 'numeric', 'nominal': 2.881, 'measured_value': row['measured_value']}])
                self.assertEqual(again['measurements'][0]['measured_value'], 2.881)
        self.assertEqual(self.evaluate('.002', 2.876, 2.886, 2.881)['measured_value'], .002)
        self.assertEqual(self.evaluate('+.002', 2.876, 2.886, 2.881)['measured_value'], 2.883)
        self.assertEqual(self.evaluate('.000', -1, 1, None)['measured_value'], 0)
        self.assertEqual(self.evaluate(0, -1, 1, 2.881)['measured_value'], 0)

    def test_invalid_readings_cannot_pass(self):
        for value in ("", None, ".", "NaN", "Infinity", "-Infinity"):
            with self.subTest(value=value):
                self.assertEqual(self.evaluate(value)["pass_fail"], "Fail")


if __name__ == "__main__":
    unittest.main()
