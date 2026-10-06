import json
import os
import re
from collections import defaultdict
from datetime import datetime

from time_entry_app.acumatica_client import AcumaticaClient, AcumaticaError, entity_value, wrapped
from time_entry_app.local_store import (
    begin_acumatica_sync_run,
    find_latest_acumatica_batch,
    finish_acumatica_sync_run,
    get_acumatica_sync_item,
    list_acumatica_candidates,
    save_acumatica_batch,
    update_acumatica_batch_status,
    upsert_acumatica_sync_item,
)


MACHINE_AREAS = {
    "E-1": "L1",
    "E-2": "L1",
    "E-3": "L2",
    "E-4": "L2",
    "E-5": "T&B",
    "E-6": "T&B",
}


def acumatica_enabled():
    return os.getenv("ACUMATICA_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}


def acumatica_cutover_at():
    return os.getenv("ACUMATICA_CUTOVER_AT", "").strip()


def normalize_machine(value):
    text = str(value or "").strip().upper()
    match = re.fullmatch(r"(?:CNC\s*|E-?)?(\d+)", text)
    return f"E-{int(match.group(1))}" if match else text


def machine_area(value):
    machine = normalize_machine(value)
    if machine not in MACHINE_AREAS:
        raise ValueError(f"Machine {value!r} is not mapped to an Acumatica labor area.")
    return MACHINE_AREAS[machine]


def normalize_labor_date(value):
    text = str(value or "").strip()
    if not text:
        raise ValueError("Labor date is required for Acumatica sync.")
    return datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()


def normalize_detail_type(value):
    text = re.sub(r"[\s_-]+", " ", str(value or "").strip()).upper()
    aliases = {
        "MACHINING": "MACHINING",
        "SET UP": "SET-UP",
        "SETUP": "SET-UP",
        "DT": "DOWN TIME",
        "DOWNTIME": "DOWN TIME",
        "DOWN TIME": "DOWN TIME",
    }
    return aliases.get(text, str(value or "").strip())


def normalize_shift(value):
    text = str(value or "").strip()
    aliases = {
        "40": "40",
        "40.0": "40",
        "DAY": "40",
        "DAY SHIFT": "40",
        "41": "41",
        "41.0": "41",
        "NIGHT": "41",
        "NIGHT SHIFT": "41",
    }
    return aliases.get(text.upper(), text)


def batch_description(area, labor_date, supplemental=False):
    formatted = datetime.fromisoformat(labor_date).strftime("%m/%d/%Y")
    suffix = " SUPPLEMENTAL" if supplemental else ""
    return f"{area} CNC LABOR {formatted}{suffix}"


def _payload(record):
    try:
        return json.loads(record.get("payload") or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}


def _transactions(record):
    payload = _payload(record)
    transactions = payload.get("acumatica_labor_transactions")
    if not transactions:
        transaction = payload.get("acumatica_labor_transaction")
        transactions = [transaction] if transaction else []
    transactions = [dict(transaction) for transaction in transactions if transaction]
    if len(transactions) == 1 and normalize_detail_type(transactions[0].get("detail_type")) == "DOWN TIME":
        positive = transactions[0]
        positive["quantity"] = 1
        positive["labor_minutes"] = int(positive.get("labor_minutes") or 0) + 1
        positive["labor_hours"] = round(positive["labor_minutes"] / 60, 4)
        positive["labor_time"] = f"{positive['labor_minutes'] // 60:02d}:{positive['labor_minutes'] % 60:02d}"
        positive["labor_amount"] = round(positive["labor_minutes"] / 60 * float(positive.get("labor_rate") or 0), 2)
        negative = dict(positive)
        negative["quantity"] = -1
        negative["labor_time"] = "-00:01"
        negative["labor_minutes"] = -1
        negative["labor_hours"] = round(-1 / 60, 4)
        negative["labor_amount"] = round(
            negative["labor_hours"] * float(negative.get("labor_rate") or 0),
            2,
        )
        transactions.append(negative)
    return transactions


def _marker(approval_id, revision_no, index):
    return f"[CNCAPP:{approval_id}:v{revision_no}:{index}]"


def _comment(transaction, marker):
    comment = str(transaction.get("tran_description") or "").strip()
    return f"{comment} {marker}".strip()


def _detail_payload(transaction, marker, line_nbr=None):
    detail = {
        "DetailType": wrapped(normalize_detail_type(transaction.get("detail_type"))),
        "EmployeeID": wrapped(str(transaction.get("employee_id") or "").zfill(6)),
        "Machine": wrapped(normalize_machine(transaction.get("machine_no"))),
        "LaborType": wrapped(str(transaction.get("labor_type") or "Direct")),
        "OrderType": wrapped(str(transaction.get("order_type") or "EN")),
        "ProductionNbr": wrapped(str(transaction.get("production_number") or "")),
        "OperationNbr": wrapped(str(transaction.get("operation_id") or "")),
    }
    inventory_id = str(transaction.get("inventory_id") or "").strip()
    if inventory_id:
        detail["InventoryID"] = wrapped(inventory_id)
    detail.update(
        {
            "LaborCode": wrapped("ENNIS"),
            "Shift": wrapped(normalize_shift(transaction.get("shift"))),
            "LaborTime": wrapped(int(transaction.get("labor_minutes") or 0)),
            "Quantity": wrapped(float(transaction.get("quantity") or 0)),
            "Location": wrapped(str(transaction.get("location") or "CUST REC")),
            "LaborComments": wrapped(_comment(transaction, marker)),
            "TranDescription": wrapped(str(transaction.get("tran_description") or "")),
        }
    )
    if line_nbr is not None:
        detail["LineNbr"] = wrapped(int(line_nbr))
    return detail


def _entity_details(entity):
    details = (entity or {}).get("Details") or []
    return details if isinstance(details, list) else []


def _entity_errors(value, path=""):
    errors = []
    if isinstance(value, dict):
        error = str(value.get("error") or "").strip()
        if error:
            errors.append(f"{path or 'entity'}: {error}")
        for key, child in value.items():
            if key != "error":
                errors.extend(_entity_errors(child, f"{path}.{key}" if path else key))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            errors.extend(_entity_errors(child, f"{path}[{index}]"))
    return errors


def _entity_status(entity):
    status = str(entity_value(entity, "Status", "") or "").strip()
    hold = entity_value(entity, "Hold", None)
    if status:
        return status
    return "On Hold" if hold is True else "Unknown"


def _entity_batch_nbr(entity):
    return str(entity_value(entity, "BatchNbr", "") or "").strip()


def _line_numbers_for_markers(entity, markers):
    found = {}
    for detail in _entity_details(entity):
        comments = " ".join(
            str(entity_value(detail, key, "") or "") for key in ("LaborComments", "TranDescription")
        )
        for marker in markers:
            if marker in comments:
                found[marker] = entity_value(detail, "LineNbr", None)
    return [found.get(marker) for marker in markers]


def _load_remote_batch(client, batch):
    entity = client.get_batch(batch["batch_nbr"])
    status = _entity_status(entity)
    update_acumatica_batch_status(batch["batch_nbr"], status)
    batch = dict(batch)
    batch["status"] = status
    return batch, entity


def _record_group(record):
    transactions = _transactions(record)
    if not transactions:
        return None
    labor_date = normalize_labor_date((_payload(record).get("fields") or {}).get("LaborDate"))
    area = machine_area(transactions[0].get("machine_no"))
    return labor_date, area


def _prepare_record_for_sync(client, record):
    payload = _payload(record)
    transactions = payload.get("acumatica_labor_transactions")
    if not transactions:
        transaction = payload.get("acumatica_labor_transaction")
        transactions = [transaction] if transaction else []
    checked = set()
    orders = {}
    for transaction in transactions:
        order_type = str(transaction.get("order_type") or "EN").strip()
        production_nbr = str(transaction.get("production_number") or "").strip()
        key = (order_type, production_nbr)
        if key not in checked:
            checked.add(key)
            orders[key] = client.get_production_order(order_type, production_nbr) if production_nbr else None
        order = orders[key]
        if not order:
            raise AcumaticaError(
                f"Production order {order_type} {production_nbr or '(blank)'} does not exist in the Acumatica tenant."
            )
        if not str(transaction.get("inventory_id") or "").strip():
            transaction["inventory_id"] = str(entity_value(order, "InventoryID", "") or "").strip()

    prepared = dict(record)
    prepared["payload"] = json.dumps(payload)
    return prepared


def _put_record(client, record, batch, remote_entity=None, description=None, is_supplemental=False):
    approval_id = int(record["id"])
    revision_no = int(record.get("revision_no") or 1)
    transactions = _transactions(record)
    markers = [_marker(approval_id, revision_no, index) for index in range(len(transactions))]
    existing_sync = get_acumatica_sync_item(approval_id)
    previous_lines = json.loads((existing_sync or {}).get("line_nbrs") or "[]")
    is_update = bool(
        existing_sync
        and existing_sync.get("sent_at")
        and previous_lines
        and int(existing_sync.get("revision_no") or 1) < revision_no
    )

    if remote_entity is None and batch:
        remote_entity = client.get_batch(batch["batch_nbr"])

    existing_lines = _line_numbers_for_markers(remote_entity or {}, markers)
    if markers and all(line is not None for line in existing_lines):
        upsert_acumatica_sync_item(
            approval_id,
            revision_no,
            "sent",
            batch_nbr=batch["batch_nbr"],
            line_nbrs=existing_lines,
            sent_payload=transactions,
        )
        return remote_entity, False

    if is_update:
        old_lines = previous_lines
        if len(old_lines) != len(transactions) or any(line is None for line in old_lines):
            raise AcumaticaError("The existing Acumatica line mapping is incomplete; Logistics review is required.")
        details = [
            _detail_payload(transaction, marker, old_lines[index])
            for index, (transaction, marker) in enumerate(zip(transactions, markers))
        ]
        request_payload = {
            "BatchNbr": wrapped(batch["batch_nbr"]),
            "Hold": wrapped(True),
            "Details": details,
        }
    elif batch:
        details = [
            _detail_payload(transaction, marker)
            for transaction, marker in zip(transactions, markers)
        ]
        request_payload = {
            "BatchNbr": wrapped(batch["batch_nbr"]),
            "Hold": wrapped(True),
            "Details": details,
        }
    else:
        labor_date, area = _record_group(record)
        description = description or batch_description(area, labor_date)
        details = [
            _detail_payload(transaction, marker)
            for transaction, marker in zip(transactions, markers)
        ]
        request_payload = {
            "Date": wrapped(f"{labor_date}T00:00:00"),
            "Description": wrapped(description),
            "Hold": wrapped(True),
            "Details": details,
        }

    if batch and (remote_entity or {}).get("id"):
        request_payload["id"] = remote_entity["id"]

    response = client.put_batch(request_payload)
    response_errors = _entity_errors(response)
    if response_errors:
        raise AcumaticaError("Acumatica rejected labor data: " + "; ".join(response_errors))
    batch_nbr = _entity_batch_nbr(response) or (batch or {}).get("batch_nbr")
    if not batch_nbr:
        raise AcumaticaError("Acumatica created the labor entry but did not return a batch number.")
    if not batch:
        labor_date, area = _record_group(record)
        description = description or batch_description(area, labor_date)
        save_acumatica_batch(
            labor_date,
            area,
            batch_nbr,
            description,
            status=_entity_status(response),
            is_supplemental=is_supplemental,
        )
        batch = find_latest_acumatica_batch(labor_date, area)
    refreshed = client.get_batch(batch_nbr)
    line_nbrs = _line_numbers_for_markers(refreshed, markers)
    if any(line is None for line in line_nbrs):
        raise AcumaticaError("Acumatica saved the entry, but its line numbers could not be reconciled.")

    update_acumatica_batch_status(batch_nbr, _entity_status(refreshed))

    upsert_acumatica_sync_item(
        approval_id,
        revision_no,
        "sent",
        batch_nbr=batch_nbr,
        line_nbrs=line_nbrs,
        sent_payload=transactions,
    )
    return refreshed, True


def sync_approved_entries(trigger_name="manual", actor=None, client_factory=AcumaticaClient, approval_id=None, failed_only=False):
    run_id = begin_acumatica_sync_run(trigger_name, actor=actor)
    if run_id is None:
        return {"status": "already_running", "sent": 0, "skipped": 0, "failed": 0}

    summary = {"status": "disabled", "sent": 0, "skipped": 0, "failed": 0, "batches": []}
    if not acumatica_enabled():
        finish_acumatica_sync_run(run_id, "disabled", summary)
        return summary

    cutover_at = acumatica_cutover_at()
    if not cutover_at:
        summary["status"] = "configuration_error"
        summary["error"] = "ACUMATICA_CUTOVER_AT is required before Acumatica sync can be enabled."
        finish_acumatica_sync_run(run_id, "configuration_error", summary)
        return summary

    candidates = []
    for record in list_acumatica_candidates(reviewed_after=cutover_at):
        if approval_id is not None and int(record["id"]) != int(approval_id):
            continue
        if failed_only and (get_acumatica_sync_item(record["id"]) or {}).get("status") != "failed":
            continue
        try:
            group = _record_group(record)
        except (TypeError, ValueError) as exc:
            upsert_acumatica_sync_item(
                record["id"], record.get("revision_no") or 1, "failed", error=str(exc)
            )
            summary["failed"] += 1
            continue
        if not group:
            upsert_acumatica_sync_item(
                record["id"], record.get("revision_no") or 1, "not_applicable"
            )
            summary["skipped"] += 1
            continue
        candidates.append((group, record))

    grouped = defaultdict(list)
    for group, record in candidates:
        grouped[group].append(record)

    if not candidates:
        summary["status"] = "completed"
        finish_acumatica_sync_run(run_id, "completed", summary)
        return summary

    try:
        with client_factory() as client:
            for (labor_date, area), records in grouped.items():
                batch = find_latest_acumatica_batch(labor_date, area)
                remote_entity = None
                if batch:
                    try:
                        batch, remote_entity = _load_remote_batch(client, batch)
                    except AcumaticaError:
                        remote_entity = None

                for record in records:
                    description = batch_description(area, labor_date)
                    is_supplemental = False
                    existing_sync = get_acumatica_sync_item(record["id"])
                    was_sent = bool(
                        existing_sync
                        and existing_sync.get("batch_nbr")
                        and existing_sync.get("sent_at")
                        and json.loads(existing_sync.get("line_nbrs") or "[]")
                    )
                    if was_sent:
                        correction_batch = {
                            "batch_nbr": existing_sync["batch_nbr"],
                            "status": batch.get("status") if batch and batch["batch_nbr"] == existing_sync["batch_nbr"] else "Unknown",
                        }
                        correction_entity = remote_entity
                        if correction_batch["status"] == "Unknown":
                            correction_entity = client.get_batch(correction_batch["batch_nbr"])
                            correction_batch["status"] = _entity_status(correction_entity)
                        if correction_batch["status"].lower() == "released":
                            upsert_acumatica_sync_item(
                                record["id"],
                                record.get("revision_no") or 1,
                                "released_attention",
                                batch_nbr=correction_batch["batch_nbr"],
                                error="Released batch changed; notify Logistics.",
                            )
                            summary["skipped"] += 1
                            continue
                        target_batch = correction_batch
                        target_entity = correction_entity
                    else:
                        if batch and batch.get("status", "").lower() == "released":
                            description = batch_description(area, labor_date, supplemental=True)
                            batch = None
                            remote_entity = None
                            is_supplemental = True
                        else:
                            description = batch_description(area, labor_date)
                            is_supplemental = False
                        target_batch = batch
                        target_entity = remote_entity

                    try:
                        record = _prepare_record_for_sync(client, record)
                        target_entity, changed = _put_record(
                            client,
                            record,
                            target_batch,
                            target_entity,
                            description=description,
                            is_supplemental=is_supplemental,
                        )
                        if not target_batch:
                            batch = find_latest_acumatica_batch(labor_date, area)
                            remote_entity = target_entity
                        summary["sent"] += int(changed)
                        summary["skipped"] += int(not changed)
                    except Exception as exc:
                        upsert_acumatica_sync_item(
                            record["id"],
                            record.get("revision_no") or 1,
                            "failed",
                            batch_nbr=(target_batch or {}).get("batch_nbr"),
                            error=str(exc),
                        )
                        summary["failed"] += 1

                if batch and batch["batch_nbr"] not in summary["batches"]:
                    summary["batches"].append(batch["batch_nbr"])

        summary["status"] = "completed" if summary["failed"] == 0 else "completed_with_errors"
        finish_acumatica_sync_run(run_id, summary["status"], summary)
        return summary
    except Exception as exc:
        summary["status"] = "failed"
        summary["error"] = str(exc)
        finish_acumatica_sync_run(run_id, "failed", summary)
        return summary


def get_approval_batch_status(approval_id, client_factory=AcumaticaClient):
    sync_item = get_acumatica_sync_item(approval_id)
    if not sync_item or not sync_item.get("batch_nbr"):
        return {"status": "Not Sent", "released": False}
    if not acumatica_enabled():
        return {"status": "Unknown", "released": False, "disabled": True}

    with client_factory() as client:
        entity = client.get_batch(sync_item["batch_nbr"])
    status = _entity_status(entity)
    update_acumatica_batch_status(sync_item["batch_nbr"], status)
    released = status.lower() == "released"
    if released:
        upsert_acumatica_sync_item(
            approval_id,
            sync_item.get("revision_no") or 1,
            "released_attention",
            batch_nbr=sync_item["batch_nbr"],
            line_nbrs=json.loads(sync_item.get("line_nbrs") or "[]"),
            error="Released batch changed; notify Logistics.",
        )
    return {"status": status, "released": released, "batch_nbr": sync_item["batch_nbr"]}
