import unittest
from lunch_workflow import evaluate_lunch_measurements


class LunchMeasurementTests(unittest.TestCase):
    elements = [
        {"element_sequence": 1, "capture_type": "numeric", "nominal": 3.0294, "min": 3.0254, "max": 3.0294},
        {"element_sequence": 2, "capture_type": "numeric", "nominal": .05, "min": .05, "max": .053},
        {"element_sequence": 3, "capture_type": "boolean"},
    ]

    def test_full_plan_including_visual_fields_is_required(self):
        for values in ({}, {"1": "3.0294"}, {"1": "3.0294", "2": ".050", "3": ""}):
            with self.assertRaises(ValueError):
                evaluate_lunch_measurements(self.elements, values)

    def test_signed_deviations_and_leading_decimal(self):
        result = evaluate_lunch_measurements(self.elements, {"1": "-.003", "2": ".050", "3": "Yes"})
        self.assertFalse(result["has_failures"])
        self.assertEqual(result["measurements"][0]["measured_value"], 3.0264)

    def test_out_of_tolerance_is_documented_as_failure(self):
        result = evaluate_lunch_measurements(self.elements, {"1": "3.0294", "2": ".054", "3": "No"})
        self.assertTrue(result["has_failures"])
        self.assertEqual(result["measurements"][1]["pass_fail"], "Fail")

    def test_invalid_numeric_and_visual_input_is_rejected(self):
        for value in ("NaN", "Infinity", "abc"):
            with self.assertRaises(ValueError):
                evaluate_lunch_measurements(self.elements, {"1": value, "2": ".050", "3": "Yes"})
        with self.assertRaises(ValueError):
            evaluate_lunch_measurements(self.elements, {"1": "3.0294", "2": ".050", "3": "maybe"})
