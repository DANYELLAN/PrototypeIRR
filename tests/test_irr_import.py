import base64
import io
import unittest
import zipfile
from unittest.mock import patch

from irr_import import (
    _draft_from_sheets,
    _parse_dimension,
    _pdf_page_to_rows,
    _xlsx_rows,
    parse_irr_upload,
)


class IrrImportTests(unittest.TestCase):
    def test_dimension_parser_handles_report_formats(self):
        self.assertEqual(
            _parse_dimension("2.308 +.001/-.003")["measurement_mode"],
            "asymmetric_tolerance",
        )
        self.assertEqual(
            _parse_dimension("2.215 - 2.217"),
            {
                "measurement_mode": "range",
                "range_min": "2.215",
                "range_max": "2.217",
            },
        )
        self.assertEqual(
            _parse_dimension("<= .005"),
            {"measurement_mode": "max_limit", "limit_max": ".005"},
        )
        self.assertEqual(_parse_dimension(".033 +/-.0005")["tolerance_digits"], "0005")
        self.assertEqual(
            _parse_dimension("Thread Profile", "THD Profile")["measurement_mode"],
            "visual",
        )

    def test_report_table_becomes_reviewable_draft(self):
        sheets = [
            {
                "name": "Sheet1",
                "rows": [
                    {"number": 1, "values": {"A": "2.375 4.70# BEN-EUI (BOX) INSPECTION REPORT"}, "styles": {}},
                    {"number": 2, "values": {"A": "Drawing #:", "B": "EUI2375470B Rev 10"}, "styles": {}},
                    {"number": 3, "values": {"A": "First Article #: 45-EUI2375470B Rev 11"}, "styles": {}},
                    {"number": 4, "values": {"A": "#", "B": "ELEMENT", "C": "DWG DIM", "D": "GAUGE"}, "styles": {}},
                    {"number": 5, "values": {"A": "1", "B": "Thread Crest Diameter", "C": "2.308 +.001/-.003", "D": "MRP"}, "styles": {"B": "7"}},
                    {"number": 6, "values": {"B": "Thread Ovality", "C": "<= .005"}, "styles": {"B": "7"}},
                    {"number": 7, "values": {"A": "2", "B": "Thread Profile", "C": "Thread Profile", "D": "THD Profile"}, "styles": {}},
                ],
            }
        ]

        draft = _draft_from_sheets("sample.xlsx", sheets)

        self.assertEqual(draft["size_label"], "2.375")
        self.assertEqual(draft["weight_label"], "4.70#")
        self.assertEqual(draft["grade_label"], "BEN-EUI")
        self.assertEqual(draft["connector_type"], "BOX")
        self.assertEqual(draft["drawing"], "EUI2375470B Rev 10")
        self.assertEqual(draft["first_article_label"], "45-EUI2375470B Rev 11")
        self.assertEqual(len(draft["rows"]), 3)
        self.assertEqual(draft["rows"][1]["gauge"], "MRP")
        self.assertEqual(draft["rows"][2]["measurement_mode"], "visual")
        self.assertEqual(draft["rows"][2]["frequency"], "rotating")
    def test_first_article_value_can_be_in_an_adjacent_cell(self):
        sheets = [
            {
                "name": "Sheet1",
                "rows": [
                    {"number": 1, "values": {"A": "First Article #:", "B": "45-EUI2375470B Rev 11"}, "styles": {}},
                    {"number": 2, "values": {"A": "#", "B": "ELEMENT", "C": "DWG DIM", "D": "GAUGE"}, "styles": {}},
                    {"number": 3, "values": {"B": "Overall Length", "C": "2.215 - 2.217", "D": "Depth Gauge"}, "styles": {}},
                ],
            }
        ]

        draft = _draft_from_sheets("sample.xlsx", sheets)

        self.assertEqual(draft["first_article_label"], "45-EUI2375470B Rev 11")

    def test_csv_upload_uses_same_table_mapping(self):
        report = (
            "#,ELEMENT,DWG DIM,GAUGE\n"
            "1,Overall Length,2.215 - 2.217,Depth Gauge\n"
        ).encode()

        draft = parse_irr_upload("report.csv", base64.b64encode(report).decode())

        self.assertEqual(draft["size_label"], "")
        self.assertEqual(draft["rows"][0]["element_description"], "Overall Length")
        self.assertEqual(draft["rows"][0]["measurement_mode"], "range")
    def test_pdf_coordinates_join_wrapped_dimensions_and_highlights(self):
        fragments = [
            {"x": 275, "y": 582, "text": "2.875 6.50# BEN-EUI (BOX) INSPECTION REPORT"},
            {"x": 236, "y": 539, "text": "Drawing #:"},
            {"x": 333, "y": 539, "text": "EUI2875650B Rev. 12"},
            {"x": 42, "y": 498, "text": "First Article #: 45-EUI2875650B Rev. 12"},
            {"x": 28, "y": 462, "text": "#"},
            {"x": 73, "y": 462, "text": "ELEMENT"},
            {"x": 156, "y": 462, "text": "DWG DIM"},
            {"x": 223, "y": 462, "text": "GAUGE"},
            {"x": 28, "y": 430, "text": "1"},
            {"x": 42, "y": 437, "text": "Thread Crest Diameter"},
            {"x": 167, "y": 448, "text": "2.802"},
            {"x": 156, "y": 436, "text": "+.001/-.003"},
            {"x": 228, "y": 431, "text": "MRP"},
            {"x": 42, "y": 414, "text": "Thread Ovality"},
            {"x": 166, "y": 413, "text": "<= .006"},
            {"x": 25, "y": 85, "text": "2"},
            {"x": 42, "y": 86, "text": "MRP Verification"},
            {"x": 145, "y": 96, "text": "Standards, Resting Point, Visual"},
            {"x": 145, "y": 85, "text": "Check, Caliper Repeatability"},
            {"x": 42, "y": 63, "text": "45-EUI2875650BIR-15 Check orange measurements"},
        ]

        rows = _pdf_page_to_rows(fragments, [(313, 458)])
        draft = _draft_from_sheets("sample.pdf", [{"name": "PDF Page 1", "rows": rows}])

        self.assertEqual(len(draft["rows"]), 3)
        self.assertEqual(draft["rows"][0]["measurement_mode"], "asymmetric_tolerance")
        self.assertEqual(draft["rows"][0]["frequency"], "every_pipe")
        self.assertEqual(draft["rows"][1]["gauge"], "MRP")
        self.assertEqual(draft["rows"][2]["visual_spec"], "Standards, Resting Point, Visual Check, Caliper Repeatability")
        self.assertEqual(draft["rows"][2]["frequency"], "rotating")

    def test_server_rejects_uploads_larger_than_eight_megabytes(self):
        encoded = base64.b64encode(b"x" * (8 * 1024 * 1024 + 1)).decode()

        with self.assertRaisesRegex(ValueError, "smaller than 8 MB"):
            parse_irr_upload("report.csv", encoded)

    def test_excel_import_rejects_oversized_expanded_members(self):
        workbook_bytes = io.BytesIO()
        with zipfile.ZipFile(workbook_bytes, "w", zipfile.ZIP_DEFLATED) as workbook:
            workbook.writestr("xl/workbook.xml", "x" * 32)

        with patch("irr_import._MAX_XLSX_MEMBER_BYTES", 16):
            with self.assertRaisesRegex(ValueError, "unexpectedly large internal file"):
                _xlsx_rows(workbook_bytes.getvalue())


if __name__ == "__main__":
    unittest.main()
