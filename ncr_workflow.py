"""Local NCR capture and manager review, independent of pipe disposition."""

import base64
import json
import math
import re
import uuid
from urllib.parse import quote

import requests
from datetime import date
from pathlib import Path

from psycopg2.extras import Json, RealDictCursor
from postgres_sync import get_db_connection

CATALOG = json.loads((Path(__file__).parent / "node_app/config/ncr_catalog.json").read_text(encoding="utf-8-sig"))
AREAS = ("Bucking", "CNC Operation", "End Finishing", "Pre-Processing", "Shipping/Receiving")
TEXT_FIELDS = ("ncr_date", "ncr_type", "supplier", "area", "tier1_id", "tier2_id", "end_type", "grade", "manufacturer", "material", "thread", "nonconformance", "immediate_containment", "ncr_details", "notes")
SCHEMA_SQL = """
ALTER TABLE ncr_reports ADD COLUMN IF NOT EXISTS capture_data JSONB NOT NULL DEFAULT '{}';
ALTER TABLE ncr_reports ADD COLUMN IF NOT EXISTS review_status TEXT NOT NULL DEFAULT 'pending_review';
ALTER TABLE ncr_reports ADD COLUMN IF NOT EXISTS review_version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE ncr_reports ADD COLUMN IF NOT EXISTS reviewed_by TEXT;
ALTER TABLE ncr_reports ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMPTZ;
ALTER TABLE ncr_reports ADD COLUMN IF NOT EXISTS review_notes TEXT;
ALTER TABLE ncr_reports ADD COLUMN IF NOT EXISTS sharepoint_item_id TEXT;
ALTER TABLE ncr_reports ADD COLUMN IF NOT EXISTS sharepoint_ncr_number TEXT;
ALTER TABLE ncr_reports ADD COLUMN IF NOT EXISTS sent_at TIMESTAMPTZ;
ALTER TABLE ncr_reports ADD COLUMN IF NOT EXISTS submission_key TEXT;
ALTER TABLE ncr_reports ADD COLUMN IF NOT EXISTS submission_error TEXT;
"""


def validate_capture(values):
    values = values or {}
    data = {key: str(values.get(key) or "").strip() for key in TEXT_FIELDS}
    if "ncr_details" in values:
        data["nonconformance"] = data["ncr_details"]
        data["immediate_containment"] = ""
    else:
        data["ncr_details"] = "\n\n".join(filter(None, [data["nonconformance"], "Immediate containment: " + data["immediate_containment"] if data["immediate_containment"] else ""]))
    for key in ("ncr_date", "ncr_type", "area", "tier1_id", "end_type", "ncr_details"):
        if not data[key]:
            raise ValueError("Complete the NCR field: " + key.replace("_", " ") + ".")
    try:
        date.fromisoformat(data["ncr_date"])
    except ValueError as error:
        raise ValueError("Enter a valid NCR date.") from error
    if data["ncr_type"] not in CATALOG["choices"]["NCR Type"]["choices"]:
        raise ValueError("Choose a valid NCR type.")
    if data["area"] not in AREAS or data["end_type"] not in ("Box", "Pin"):
        raise ValueError("Choose a valid area and end type.")
    if data["tier1_id"] not in {row["id"] for row in CATALOG["tier1"]}:
        raise ValueError("Choose a valid Tier 1 defect code.")
    children = [row for row in CATALOG["tier2"] if str(row["tier1_id"]) == data["tier1_id"]]
    if children and data["tier2_id"] not in {row["id"] for row in children}:
        raise ValueError("Choose a Tier 2 defect code belonging to the selected Tier 1 category.")
    if not children and data["tier2_id"]:
        raise ValueError("The selected Tier 1 category has no Tier 2 codes.")
    for key in ("quantity_affected", "size", "weight"):
        if values.get(key) in (None, ""):
            continue
        try:
            value = float(values.get(key, ""))
        except (ValueError, TypeError) as error:
            raise ValueError("Enter a number for " + key.replace("_", " ") + ".") from error
        if not math.isfinite(value) or value <= 0 or (key == "quantity_affected" and not value.is_integer()):
            raise ValueError("Enter a positive " + ("whole " if key == "quantity_affected" else "") + "number for " + key.replace("_", " ") + ".")
        data[key] = int(value) if key == "quantity_affected" else value
    if str(values.get("engineering_report", "")).lower() not in ("true", "false", "yes", "no"):
        raise ValueError("Choose whether an engineering report is required.")
    data["engineering_report"] = str(values["engineering_report"]).lower() in ("true", "yes")
    data["sharepoint_disposition"] = str(values.get("sharepoint_disposition") or "Rework")
    if data["sharepoint_disposition"] not in CATALOG["choices"]["Disposition"]["choices"]:
        raise ValueError("Choose a valid SharePoint disposition.")
    attachments = values.get("attachments", [])
    if not isinstance(attachments, list) or len(attachments) > 5:
        raise ValueError("Attach no more than five files.")
    data["attachments"] = []
    total_size = 0
    for item in attachments:
        try:
            name = str(item["name"])
            content = base64.b64decode(item["content"], validate=True)
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("An attachment is invalid.") from error
        if not name or len(name) > 200 or Path(name).name != name or any(c in name for c in ('/', '\\', '\r', '\n', '"', ':', '*', '?', '<', '>', '|')):
            raise ValueError("Invalid attachment filename.")
        total_size += len(content)
        data["attachments"].append({"name": name, "content": base64.b64encode(content).decode("ascii")})
    if total_size > 4 * 1024 * 1024:
        raise ValueError("Attachments must total 4 MB or less.")
    if len({item["name"].casefold() for item in data["attachments"]}) != len(data["attachments"]):
        raise ValueError("Attachment filenames must be unique.")
    return data


def get_ncr_detail(ncr_id, branch=None):
    connection = get_db_connection()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT nr.*, pu.production_number, pu.operation_description, pu.pipe_number, pu.branch,
                       ia.inspector_name, ia.cnc_operator_name, sess.shift, sess.location_name
                FROM ncr_reports nr JOIN pipe_units pu ON pu.id = nr.pipe_unit_id
                JOIN inspection_attempts ia ON ia.id = nr.attempt_id
                LEFT JOIN inspector_sessions sess ON sess.id = ia.session_id
                WHERE nr.id = %s AND (%s IS NULL OR pu.branch = %s)
            """, (ncr_id, branch, branch))
            row = cursor.fetchone()
            if not row:
                raise ValueError("NCR not found.")
            return dict(row)
    finally:
        connection.close()


def save_ncr_review(ncr_id, values, actor_adp, expected_version, approve=False, review_notes=""):
    from workflow_db import get_employee_by_adp, is_admin_user

    actor = get_employee_by_adp(actor_adp)
    if not is_admin_user(actor):
        raise ValueError("Only admins can review NCRs.")
    connection = get_db_connection()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT * FROM ncr_reports WHERE id = %s FOR UPDATE", (ncr_id,))
            row = cursor.fetchone()
            if not row or row["review_version"] != int(expected_version):
                raise ValueError("This NCR changed. Reload it before saving your review.")
            if row["sharepoint_item_id"] or row["review_status"] in ("submitting", "submission_unknown"):
                raise ValueError("This NCR has already been submitted to SharePoint.")
            data = validate_capture({**(row["capture_data"] or {}), **values})
            data["context"] = (row["capture_data"] or {}).get("context", {})
            cursor.execute("""
                UPDATE ncr_reports SET capture_data = %s, nonconformance = %s,
                    immediate_containment = %s, tier_code = %s, review_status = %s,
                    reviewed_by = %s, reviewed_at = CASE WHEN %s THEN NOW() ELSE NULL END,
                    review_notes = %s, review_version = review_version + 1 WHERE id = %s
            """, (Json(data), data["nonconformance"], data["immediate_containment"], data["tier2_id"] or data["tier1_id"],
                  "reviewed" if approve else "pending_review", actor["name"] if approve else None, approve, review_notes, ncr_id))
        connection.commit()
        return {"review_status": "reviewed" if approve else "pending_review"}
    finally:
        connection.close()


SITE_URL = "https://benoitinc.sharepoint.com/sites/QMS1061"
LIST_ID = "34805c0d-325f-4118-896b-a5388f683c3f"


def next_ncr_number(items):
    numbers = []
    for item in items:
        match = re.fullmatch(r"NCR-(\d+)", str(item.get("fields", {}).get("NCR_x0020_Number", "")).strip(), re.IGNORECASE)
        if match:
            numbers.append(int(match.group(1)))
    if not numbers:
        raise ValueError("No existing SharePoint NCR number could be verified. Nothing was sent.")
    return "NCR-" + str(max(numbers) + 1)


def sharepoint_fields(row, number, marker):
    data = validate_capture(row["capture_data"])
    context = data.get("context", {})
    # Context is immutable inspection metadata, not editable form input.
    context = (row.get("capture_data") or {}).get("context", context)
    tier1 = next(item["label"] for item in CATALOG["tier1"] if item["id"] == data["tier1_id"])
    tier2 = next((item["label"] for item in CATALOG["tier2"] if item["id"] == data["tier2_id"]), "")
    context_notes = "\n".join(f"{label}: {context.get(key) or row.get(fallback) or ''}" for key, label, fallback in (
        ("inspector", "Inspector", "inspector_name"), ("operator", "Operator", "cnc_operator_name"),
        ("shift", "Shift", "shift"), ("machine", "Machine", "location_name")))
    fields = {
        "NCR_x0020_Number": number, "NCR_x0020_Type": data["ncr_type"],
        "Supplier": data["supplier"],
        "NCR_x0020_Date": data["ncr_date"] + "T12:00:00Z",
        "Status": "Closed" if row["status"] == "closed" else "Open",
        "Job_x0020_Number": row["production_number"], "PipeID": row["pipe_number"],
        "Work_x0020_Shift": context.get("shift") or row.get("shift") or "",
        "Inspector": context.get("inspector") or row.get("inspector_name") or "",
        "Operator": context.get("operator") or row.get("cnc_operator_name") or "",
        "Area": data["area"], "Tier_x0020_1_x0020_Defect_x0020_": tier1,
        "Tier_x0020_2_x0020_Defect_x0020_": tier2, "EndType": data["end_type"],
        "NCR_x0020_Details": data["ncr_details"],
        "QuantityAffected": data.get("quantity_affected"), "Size": data.get("size"), "Weight": data.get("weight"),
        "Grade": data["grade"], "MFG": data["manufacturer"], "Material": data["material"], "Thread": data["thread"],
        "EngineeringReport": data["engineering_report"],
        "Reason_x0020_for_x0020_NCR": "\n\n".join(filter(None, [data["notes"], context_notes,
            "Manager review: " + str(row.get("reviewed_by") or ""), str(row.get("review_notes") or ""), marker])),
        "Disposition": data["sharepoint_disposition"],
        "QA_x002f__x0020_Management_x0020": str(row.get("reviewed_by") or ""),
    }
    if row.get("closed_at"):
        fields["Actual_x0020_Close_x0020_Date"] = row["closed_at"].isoformat()
    return {key: value for key, value in fields.items() if value is not None}


def attachment_headers():
    from sharepoint_client import get_msal_app

    app = get_msal_app()
    accounts = app.get_accounts()
    token = app.acquire_token_silent(["https://benoitinc.sharepoint.com/.default"], account=accounts[0]) if accounts else None
    if not token or "access_token" not in token:
        raise ValueError("SharePoint attachment access is not connected. Attachments remain local; no new NCR was sent.")
    return {"Authorization": "Bearer " + token["access_token"], "Accept": "application/json;odata=nometadata"}


def upload_attachments(item_id, attachments, headers):
    if not attachments:
        return
    if not str(item_id).isdigit():
        raise ValueError("Invalid SharePoint item ID.")
    url = f"{SITE_URL}/_api/web/lists(guid'{LIST_ID}')/items({item_id})/AttachmentFiles"
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    existing = {item["FileName"].casefold() for item in response.json().get("value", [])}
    for item in attachments:
        if item["name"].casefold() in existing:
            continue
        filename = quote(item["name"].replace("'", "''"), safe="")
        response = requests.post(url + f"/add(FileName='{filename}')", headers={**headers, "Content-Type": "application/octet-stream"},
                                 data=base64.b64decode(item["content"]), timeout=30)
        response.raise_for_status()


def submit_ncr(ncr_id, actor_adp, expected_version):
    from workflow_db import get_employee_by_adp, is_manager_or_supervisor
    from sharepoint_client import get_access_token, get_site_id, graph_get, graph_get_absolute, create_list_item

    if not is_manager_or_supervisor(get_employee_by_adp(actor_adp)):
        raise ValueError("Only managers and supervisors can send NCRs.")
    connection = get_db_connection()
    attempted_create = False
    known_item_id = None
    try:
        # A session advisory lock serializes all numbering and sends from this app.
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT pg_advisory_lock(20261109, 4875)")
            cursor.execute("SELECT * FROM ncr_reports WHERE id = %s FOR UPDATE", (ncr_id,))
            row = cursor.fetchone()
            if not row:
                raise ValueError("NCR not found.")
            if row["sharepoint_item_id"] and row["review_status"] == "sent":
                return {"number": row["sharepoint_ncr_number"], "already_sent": True}
            if row["review_status"] not in ("reviewed", "submitting", "submission_unknown", "attachments_pending"):
                raise ValueError("A manager must verify this NCR before sending it.")
            if row["review_version"] != int(expected_version):
                raise ValueError("This NCR changed. Reload it before sending.")
            key = row.get("submission_key") or str(uuid.uuid4())
            marker = "[InspectionApp NCR " + key + "]"
            files = (row["capture_data"] or {}).get("attachments", [])
            file_headers = attachment_headers() if files else None
            headers = {"Authorization": "Bearer " + get_access_token(allow_interactive=False)}
            site_id = get_site_id(SITE_URL, headers)
            endpoint = f"/sites/{site_id}/lists/{LIST_ID}/items"
            page = graph_get(endpoint, headers, params={"$expand": "fields($select=NCR_x0020_Number,Reason_x0020_for_x0020_NCR)", "$top": 999})
            items = page.get("value", [])
            while page.get("@odata.nextLink"):
                page = graph_get_absolute(page["@odata.nextLink"], headers)
                items.extend(page.get("value", []))
            existing = next((item for item in items if marker in str(item.get("fields", {}).get("Reason_x0020_for_x0020_NCR", ""))), None)
            if existing:
                item_id, number = existing["id"], existing["fields"]["NCR_x0020_Number"]
            else:
                if row["review_status"] != "reviewed":
                    raise ValueError("The previous send has an uncertain result. No matching item was found; verify SharePoint before retrying. Nothing new was sent.")
                detail = get_ncr_detail(ncr_id)
                number = next_ncr_number(items)
                fields = sharepoint_fields(detail, number, marker)
                cursor.execute("UPDATE ncr_reports SET review_status = 'submitting', submission_key = %s, submission_error = NULL WHERE id = %s", (key, ncr_id))
                connection.commit()
                attempted_create = True
                created = create_list_item(SITE_URL, LIST_ID, fields, headers, site_id=site_id)
                item_id = created.get("id")
                if not item_id:
                    raise ValueError("SharePoint did not return an item ID.")
            known_item_id = str(item_id)
            cursor.execute("""UPDATE ncr_reports SET sharepoint_item_id = %s, sharepoint_ncr_number = %s,
                review_status = 'attachments_pending' WHERE id = %s""", (known_item_id, number, ncr_id))
            connection.commit()
            upload_attachments(item_id, files, file_headers)
            cursor.execute("""UPDATE ncr_reports SET review_status = 'sent', sharepoint_item_id = %s,
                sharepoint_ncr_number = %s, sent_at = NOW(), submission_error = NULL,
                review_version = review_version + 1 WHERE id = %s""", (str(item_id), number, ncr_id))
            connection.commit()
            return {"number": number, "item_id": str(item_id)}
    except Exception as error:
        connection.rollback()
        if known_item_id:
            with connection.cursor() as cursor:
                cursor.execute("UPDATE ncr_reports SET review_status = 'attachments_pending', submission_error = %s WHERE id = %s", ("The NCR exists in SharePoint, but attachment delivery needs retrying.", ncr_id))
            connection.commit()
            raise ValueError("The NCR was created in SharePoint. Retry attachment delivery on this record; do not create another NCR.") from error
        if attempted_create and re.match(r"Graph request failed \((400|401|403|404|409|412|422|429)\)", str(error)):
            with connection.cursor() as cursor:
                cursor.execute("UPDATE ncr_reports SET review_status = 'reviewed', submission_error = %s WHERE id = %s", (str(error)[:1000], ncr_id))
            connection.commit()
            raise ValueError("SharePoint rejected the submission; the NCR remains local and reviewed. " + str(error)[:500]) from error
        if attempted_create:
            with connection.cursor() as cursor:
                cursor.execute("UPDATE ncr_reports SET review_status = 'submission_unknown', submission_error = %s WHERE id = %s", (str(error)[:1000], ncr_id))
            connection.commit()
            raise ValueError("SharePoint submission could not be confirmed. Use Check Submission to reconcile it; do not create another NCR manually until checked.") from error
        raise
    finally:
        connection.close()
