"""Extract reviewable Digital IRR drafts from existing report files."""

from __future__ import annotations

import base64
import csv
import io
import json
import re
import sys
import zipfile
from pathlib import Path
from xml.etree import ElementTree


_SHEET_NS = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
_REL_NS = {"r": "http://schemas.openxmlformats.org/package/2006/relationships"}
_WORKBOOK_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"


def _cell_text(cell, shared_strings):
    cell_type = cell.get("t")
    if cell_type == "inlineStr":
        return "".join(node.text or "" for node in cell.findall(".//x:t", _SHEET_NS)).strip()
    value = cell.find("x:v", _SHEET_NS)
    if value is None or value.text is None:
        return ""
    if cell_type == "s":
        try:
            return shared_strings[int(value.text)].strip()
        except (IndexError, ValueError):
            return ""
    return value.text.strip()


def _xlsx_rows(file_bytes):
    with zipfile.ZipFile(io.BytesIO(file_bytes)) as workbook:
        shared_strings = []
        if "xl/sharedStrings.xml" in workbook.namelist():
            root = ElementTree.fromstring(workbook.read("xl/sharedStrings.xml"))
            for item in root.findall("x:si", _SHEET_NS):
                shared_strings.append("".join(node.text or "" for node in item.findall(".//x:t", _SHEET_NS)))

        style_fills = {}
        if "xl/styles.xml" in workbook.namelist():
            styles_root = ElementTree.fromstring(workbook.read("xl/styles.xml"))
            fills = styles_root.findall("x:fills/x:fill", _SHEET_NS)
            for style_index, style in enumerate(styles_root.findall("x:cellXfs/x:xf", _SHEET_NS)):
                fill_id = int(style.get("fillId", "0"))
                if fill_id >= len(fills):
                    continue
                pattern = fills[fill_id].find("x:patternFill", _SHEET_NS)
                color = pattern.find("x:fgColor", _SHEET_NS) if pattern is not None else None
                if color is not None:
                    style_fills[style_index] = color.get("rgb") or color.get("indexed") or color.get("theme") or ""

        relationships = ElementTree.fromstring(workbook.read("xl/_rels/workbook.xml.rels"))
        targets = {
            relation.get("Id"): relation.get("Target")
            for relation in relationships.findall("r:Relationship", _REL_NS)
        }
        workbook_root = ElementTree.fromstring(workbook.read("xl/workbook.xml"))
        sheets = []
        for sheet in workbook_root.findall("x:sheets/x:sheet", _SHEET_NS):
            target = targets.get(sheet.get(f"{{{_WORKBOOK_REL_NS}}}id"), "")
            if not target:
                continue
            target = target.lstrip("/")
            if not target.startswith("xl/"):
                target = f"xl/{target}"
            sheet_root = ElementTree.fromstring(workbook.read(target))
            rows = []
            for row in sheet_root.findall(".//x:sheetData/x:row", _SHEET_NS):
                values = {}
                cell_styles = {}
                for cell in row.findall("x:c", _SHEET_NS):
                    reference = cell.get("r", "")
                    column = re.sub(r"\d", "", reference)
                    text = _cell_text(cell, shared_strings)
                    if text:
                        values[column] = text
                        style_index = int(cell.get("s", "0"))
                        if style_index in style_fills:
                            cell_styles[column] = style_fills[style_index]
                if values:
                    rows.append({"number": int(row.get("r", "0")), "values": values, "styles": cell_styles})
            sheets.append({"name": sheet.get("name", "Sheet"), "rows": rows})
        return sheets


def inspect_xlsx(path):
    """Return populated cells for diagnostic use while tailoring import layouts."""
    return _xlsx_rows(Path(path).read_bytes())


def parse_irr_upload(filename, encoded_content):
    """Parse an uploaded report into an editable recipe-builder draft."""
    name = Path(str(filename or "")).name
    if not name:
        raise ValueError("Choose a report file to import.")
    try:
        content = base64.b64decode(encoded_content or "", validate=True)
    except (ValueError, TypeError) as error:
        raise ValueError("The uploaded report could not be read.") from error
    if not content:
        raise ValueError("The uploaded report is empty.")

    suffix = Path(name).suffix.lower()
    if suffix == ".xlsx":
        sheets = _xlsx_rows(content)
    elif suffix == ".csv":
        text = content.decode("utf-8-sig", errors="replace")
        reader = csv.reader(io.StringIO(text))
        rows = [
            {"number": index, "values": {chr(65 + column): value.strip() for column, value in enumerate(row) if value.strip()}}
            for index, row in enumerate(reader, start=1)
        ]
        sheets = [{"name": "CSV", "rows": rows}]
    elif suffix == ".pdf":
        sheets = _pdf_rows(content)
    else:
        raise ValueError("Upload an .xlsx, .csv, or text-readable .pdf report.")

    return _draft_from_sheets(name, sheets)


def _pdf_rows(file_bytes):
    try:
        from pypdf import PdfReader
    except ImportError as error:
        raise ValueError("PDF import requires pypdf. Run: python -m pip install pypdf") from error

    try:
        reader = PdfReader(io.BytesIO(file_bytes))
    except Exception as error:
        raise ValueError("The PDF could not be opened.") from error

    sheets = []
    for page_number, page in enumerate(reader.pages, start=1):
        fragments = []
        highlight_regions = []
        paint_state = {"highlight": False, "rectangle": None}

        def capture_text(text, _cm, text_matrix, _font, _font_size):
            cleaned = re.sub(r"\s+", " ", str(text or "")).strip()
            if cleaned:
                fragments.append(
                    {
                        "x": float(text_matrix[4]),
                        "y": float(text_matrix[5]),
                        "text": cleaned,
                    }
                )

        def capture_shapes(operator, operands, _cm, _tm):
            if operator == b"rg" and len(operands) >= 3:
                red, green, blue = (float(operands[index]) for index in range(3))
                paint_state["highlight"] = red >= 0.8 and green >= 0.7 and blue <= 0.9
                paint_state["rectangle"] = None
            elif operator == b"re" and paint_state["highlight"] and len(operands) >= 4:
                paint_state["rectangle"] = tuple(float(value) for value in operands[:4])
            elif operator in {b"f", b"f*"} and paint_state["highlight"]:
                rectangle = paint_state.get("rectangle")
                if rectangle and rectangle[2] > float(page.mediabox.width) * 0.6 and rectangle[3] > 20:
                    highlight_regions.append((rectangle[1], rectangle[1] + rectangle[3]))
                paint_state["highlight"] = False
                paint_state["rectangle"] = None

        page.extract_text(visitor_text=capture_text, visitor_operand_before=capture_shapes)
        page_rows = _pdf_page_to_rows(fragments, highlight_regions)
        if page_rows:
            sheets.append({"name": f"PDF Page {page_number}", "rows": page_rows})

    if not sheets:
        raise ValueError("No readable text was found in the PDF. A scanned PDF must be OCR-processed before importing.")
    return sheets


def _pdf_page_to_rows(fragments, highlight_regions):
    if not fragments:
        return []

    def find_fragment(text):
        key = text.casefold()
        return next((item for item in fragments if item["text"].casefold() == key), None)

    element_header = find_fragment("ELEMENT")
    dimension_header = find_fragment("DWG DIM")
    gauge_header = find_fragment("GAUGE")
    number_header = find_fragment("#")
    if not element_header or not dimension_header:
        return []

    header_y = element_header["y"]
    element_x = element_header["x"]
    dimension_x = dimension_header["x"]
    gauge_x = gauge_header["x"] if gauge_header else dimension_x + 65
    number_x = number_header["x"] if number_header else max(0, element_x - 15)
    element_left = number_x + 8
    element_right = (element_x + dimension_x) / 2
    dimension_left = element_right
    gauge_left = (dimension_x + gauge_x) / 2
    footer_fragments = [
        item
        for item in fragments
        if item["y"] < header_y and re.search(r"\b\d{2}-[A-Z0-9-]+IR-\d+\b", item["text"], re.I)
    ]
    footer_y = max((item["y"] for item in footer_fragments), default=-1)

    title = next(
        (item["text"] for item in fragments if "INSPECTION REPORT" in item["text"].upper()),
        "",
    )
    rows = []
    row_number = 1
    if title:
        rows.append({"number": row_number, "values": {"A": title}, "styles": {}})
        row_number += 1

    drawing_label = next((item for item in fragments if "drawing" in item["text"].casefold()), None)
    if drawing_label:
        same_line = [
            item
            for item in fragments
            if abs(item["y"] - drawing_label["y"]) <= 2 and item["x"] > drawing_label["x"] + 10
        ]
        drawing = min(same_line, key=lambda item: item["x"], default={"text": ""})["text"]
        rows.append(
            {
                "number": row_number,
                "values": {"E": "Drawing #:", "G": drawing},
                "styles": {},
            }
        )
        row_number += 1

    first_article = next((item["text"] for item in fragments if "first article" in item["text"].casefold()), "")
    if first_article:
        rows.append({"number": row_number, "values": {"B": first_article}, "styles": {}})
        row_number += 1

    rows.append(
        {
            "number": row_number,
            "values": {"A": "#", "B": "ELEMENT", "C": "DWG DIM", "D": "GAUGE"},
            "styles": {},
        }
    )
    row_number += 1

    element_fragments = [
        item
        for item in fragments
        if footer_y + 8 < item["y"] < header_y - 3
        and element_left <= item["x"] < element_right
        and not item["text"].casefold().startswith("check orange")
    ]
    element_fragments.sort(key=lambda item: item["y"], reverse=True)
    for index, element in enumerate(element_fragments):
        upper_y = header_y if index == 0 else (element_fragments[index - 1]["y"] + element["y"]) / 2
        lower_y = footer_y if index == len(element_fragments) - 1 else (element["y"] + element_fragments[index + 1]["y"]) / 2
        dimension_parts = sorted(
            (
                item
                for item in fragments
                if lower_y < item["y"] <= upper_y
                and dimension_left <= item["x"] < gauge_left
                and item is not dimension_header
            ),
            key=lambda item: item["y"],
            reverse=True,
        )
        gauge_parts = sorted(
            (
                item
                for item in fragments
                if lower_y < item["y"] <= upper_y
                and gauge_left <= item["x"] < gauge_x + 65
                and item is not gauge_header
            ),
            key=lambda item: item["y"],
            reverse=True,
        )
        number_parts = [
            item
            for item in fragments
            if lower_y < item["y"] <= upper_y
            and number_x - 8 <= item["x"] < element_left
            and item["text"].isdigit()
        ]
        dimension = " ".join(item["text"] for item in dimension_parts).strip()
        if not dimension:
            continue
        values = {
            "B": element["text"],
            "C": dimension,
        }
        if number_parts:
            values["A"] = number_parts[0]["text"]
        if gauge_parts:
            values["D"] = " ".join(item["text"] for item in gauge_parts).strip()
        highlighted = any(low <= element["y"] <= high for low, high in highlight_regions)
        rows.append(
            {
                "number": row_number,
                "values": values,
                "styles": {"B": "PDF_HIGHLIGHT"} if highlighted else {},
            }
        )
        row_number += 1
    return rows


def _draft_from_sheets(filename, sheets):
    populated_rows = [row for sheet in sheets for row in sheet["rows"] if row.get("values")]
    populated = [
        str(value).strip()
        for row in populated_rows
        for value in row["values"].values()
        if str(value).strip()
    ]
    if not populated_rows:
        raise ValueError("No readable report content was found.")

    title = next((value for value in populated if "INSPECTION REPORT" in value.upper()), "")
    identity_text = title or filename
    identity = re.search(
        r"(?P<size>\d+(?:\.\d+)?)\s+(?P<weight>\d+(?:\.\d+)?)\s*#?\s+"
        r"(?P<grade>.+?)\s+\(?(?P<connector>BOX|PIN)\)?\s+(?:INSP(?:ECTION)?\s+)?REPORT",
        identity_text,
        re.I,
    )
    if identity:
        size_label = identity.group("size")
        weight_label = f"{identity.group('weight')}#"
        grade_label = re.sub(r"\s+", " ", identity.group("grade")).strip()
        connector_type = identity.group("connector").upper()
    else:
        size_label = ""
        weight_label = ""
        grade_label = ""
        connector_type = "BOX" if re.search(r"\bBOX\b", filename, re.I) else "PIN" if re.search(r"\bPIN\b", filename, re.I) else ""

    drawing = _value_after_label(populated_rows, "drawing")
    first_article = ""
    for value in populated:
        match = re.search(r"First\s+Article\s*#?\s*:\s*(.+)", value, re.I)
        if match:
            first_article = match.group(1).strip(" _")
            break

    element_rows = _extract_element_rows(populated_rows)
    if not element_rows:
        raise ValueError("The report did not contain a readable ELEMENT / DWG DIM / GAUGE table.")
    if len(element_rows) > 25:
        raise ValueError(f"The report contains {len(element_rows)} elements; the Digital IRR builder supports up to 25.")

    return {
        "source_report": title or filename,
        "imported_file_name": filename,
        "size_label": size_label,
        "weight_label": weight_label,
        "grade_label": grade_label,
        "connector_type": connector_type,
        "drawing": drawing,
        "first_article_label": first_article,
        "rows": element_rows,
    }


def _column_number(column):
    result = 0
    for character in str(column).upper():
        if not character.isalpha():
            continue
        result = result * 26 + ord(character) - 64
    return result


def _value_after_label(rows, label):
    label_key = label.casefold()
    for row in rows:
        ordered = sorted(row["values"].items(), key=lambda item: _column_number(item[0]))
        for index, (_, value) in enumerate(ordered):
            text = str(value).strip()
            if label_key not in text.casefold():
                continue
            inline = text.split(":", 1)[1].strip(" _") if ":" in text else ""
            if inline:
                return inline
            if index + 1 < len(ordered):
                return str(ordered[index + 1][1]).strip(" _")
    return ""


def _find_table_columns(row):
    columns = {}
    aliases = {
        "element": {"element", "inspection element", "characteristic"},
        "dimension": {"dwg dim", "drawing dimension", "dimension", "specification", "spec"},
        "gauge": {"gauge", "gage", "instrument"},
    }
    for column, value in row["values"].items():
        normalized = re.sub(r"\s+", " ", str(value).strip()).casefold()
        for field, names in aliases.items():
            if normalized in names:
                columns[field] = column
    return columns if "element" in columns and "dimension" in columns else None


def _extract_element_rows(rows):
    header_index = None
    columns = None
    for index, row in enumerate(rows):
        columns = _find_table_columns(row)
        if columns:
            header_index = index
            break
    if header_index is None:
        return []

    result = []
    previous_gauge = ""
    for row in rows[header_index + 1 :]:
        values = row["values"]
        element = str(values.get(columns["element"], "")).strip()
        dimension = str(values.get(columns["dimension"], "")).strip()
        gauge = str(values.get(columns.get("gauge", ""), "")).strip()
        if not element:
            continue
        if element.casefold().startswith("check orange") or re.fullmatch(r"\d{2}-[A-Z0-9-]+", element, re.I):
            break
        if not dimension:
            continue
        if not gauge and ("oval" in element.casefold()):
            gauge = previous_gauge
        parsed = _parse_dimension(dimension, gauge)
        parsed.update(
            {
                "element_sequence": len(result) + 1,
                "element_description": re.sub(r"\s+", " ", element),
                "gauge": gauge or ("Visual" if parsed["measurement_mode"] == "visual" else ""),
                "frequency": _frequency_from_row(row),
                "notes": f"Imported DWG DIM: {re.sub(r'\s+', ' ', dimension)}",
            }
        )
        result.append(parsed)
        if gauge:
            previous_gauge = gauge
    return result


def _frequency_from_row(row):
    colors = {str(row.get("styles", {}).get(column, "")).upper() for column in ("B", "C", "D")}
    highlighted = any(color and color not in {"0", "FFD9D9D9"} for color in colors)
    return "every_pipe" if highlighted or "styles" not in row else "rotating"


def _decimal_places(value):
    text = str(value).strip()
    return len(text.split(".", 1)[1]) if "." in text else 0


def _decimal_tail(value):
    text = str(value).strip()
    return text.split(".", 1)[1] if "." in text else text


def _parse_dimension(dimension, gauge=""):
    text = re.sub(r"\s+", " ", str(dimension).strip())
    normalized = text.replace("±", "+/-").replace("≤", "<=").replace("≥", ">=")
    number = r"(?:\d+(?:\.\d+)?|\.\d+)"

    asymmetric = re.fullmatch(
        rf"({number})\s*\+\s*({number})\s*/\s*-\s*({number})",
        normalized,
        re.I,
    )
    if asymmetric:
        nominal, plus_value, minus_value = asymmetric.groups()
        return {
            "measurement_mode": "asymmetric_tolerance",
            "nominal_value": nominal,
            "plus_tolerance": _decimal_tail(plus_value),
            "minus_tolerance": _decimal_tail(minus_value),
        }

    symmetric = re.fullmatch(rf"({number})?\s*\+\s*/\s*-\s*({number})", normalized, re.I)
    if symmetric:
        nominal, tolerance = symmetric.groups()
        places = _decimal_places(tolerance)
        return {
            "measurement_mode": "nominal_tolerance" if nominal else "deviation",
            "nominal_value": nominal or "",
            "tolerance_decimal_places": str(max(3, min(4, places))),
            "tolerance_digits": _decimal_tail(tolerance),
        }

    range_match = re.fullmatch(rf"({number})\s*-\s*({number})", normalized, re.I)
    if range_match:
        return {
            "measurement_mode": "range",
            "range_min": range_match.group(1),
            "range_max": range_match.group(2),
        }

    max_match = re.fullmatch(rf"(?:<=?\s*)?({number})\s*(?:MAX(?:IMUM)?)?", normalized, re.I)
    if max_match and (normalized.startswith("<") or re.search(r"MAX", normalized, re.I)):
        return {"measurement_mode": "max_limit", "limit_max": max_match.group(1)}

    min_match = re.fullmatch(rf"(?:>=?\s*)?({number})\s*(?:MIN(?:IMUM)?)?", normalized, re.I)
    if min_match and (normalized.startswith(">") or re.search(r"MIN", normalized, re.I)):
        return {"measurement_mode": "min_limit", "limit_min": min_match.group(1)}

    return {
        "measurement_mode": "visual",
        "visual_spec": text,
        "gauge": gauge or "Visual",
    }


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "--inspect":
        print(json.dumps(inspect_xlsx(sys.argv[2]), indent=2))
        return
    raise SystemExit("Usage: irr_import.py --inspect report.xlsx")


if __name__ == "__main__":
    main()
