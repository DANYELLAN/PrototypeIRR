import json
import os
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from time_entry_app import local_store
from time_entry_app.acumatica_client import entity_value, wrapped
from time_entry_app.acumatica_sync import (
    _detail_payload,
    _entity_errors,
    normalize_detail_type,
    normalize_machine,
    sync_approved_entries,
)


class FakeAcumaticaClient:
    batches = {}
    put_calls = 0
    production_orders = {("EN", "EWO26-00009")}

    @classmethod
    def reset(cls):
        cls.batches = {}
        cls.put_calls = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def get_batch(self, batch_nbr):
        return self.batches[batch_nbr]

    def get_production_order(self, order_type, production_nbr):
        if (order_type, production_nbr) not in self.production_orders:
            return None
        return {
            "OrderType": wrapped(order_type),
            "ProductionNbr": wrapped(production_nbr),
            "InventoryID": wrapped("EN00088"),
        }

    def put_batch(self, payload):
        type(self).put_calls += 1
        batch_nbr = entity_value(payload, "BatchNbr", "")
        if batch_nbr:
            batch = self.batches[batch_nbr]
        else:
            batch_nbr = str(140000 + len(self.batches) + 1)
            batch = {
                "BatchNbr": wrapped(batch_nbr),
                "Date": payload["Date"],
                "Description": payload["Description"],
                "Hold": wrapped(True),
                "Status": wrapped("On Hold"),
                "Details": [],
            }
            self.batches[batch_nbr] = batch

        for incoming in payload.get("Details") or []:
            line_nbr = entity_value(incoming, "LineNbr", None)
            if line_nbr is None:
                line_nbr = len(batch["Details"]) + 1
                incoming = dict(incoming)
                incoming["LineNbr"] = wrapped(line_nbr)
                batch["Details"].append(incoming)
            else:
                index = next(
                    index
                    for index, detail in enumerate(batch["Details"])
                    if entity_value(detail, "LineNbr") == line_nbr
                )
                batch["Details"][index] = incoming
        return batch

    def patch_batch(self, payload):
        return self.put_batch(payload)


def approval_payload(machine, employee_id, quantity=1):
    transaction = {
        "tran_description": "Machining",
        "detail_type": "Machining",
        "employee_id": employee_id,
        "machine_no": machine,
        "labor_type": "Direct",
        "order_type": "EN",
        "production_number": "EWO26-00009",
        "operation_id": "0005",
        "inventory_id": "EN00088",
        "shift": "40",
        "labor_minutes": 15,
        "labor_rate": 123.30,
        "labor_amount": 30.83,
        "quantity": quantity,
        "uom": "JOINT",
        "warehouse": "EN-FG SSOT",
        "location": "CUST REC",
        "qty_scrapped": 0,
        "reason_code": "",
    }
    return {
        "fields": {"LaborDate": "2026-09-29", "MachineNo": machine},
        "acumatica_labor_transaction": transaction,
    }


class AcumaticaSyncTests(unittest.TestCase):
    def setUp(self):
        temp_dir = Path(__file__).resolve().parent / ".tmp"
        temp_dir.mkdir(exist_ok=True)
        self.db_path = temp_dir / f"acumatica_{uuid.uuid4().hex}.db"
        self.machine_path = temp_dir / f"machines_{uuid.uuid4().hex}.json"
        self.db_patch = patch.object(local_store, "DB_PATH", self.db_path)
        self.machine_patch = patch.object(local_store, "LOCAL_MACHINE_CONFIG", self.machine_path)
        self.db_patch.start()
        self.machine_patch.start()
        FakeAcumaticaClient.reset()

    def tearDown(self):
        self.machine_patch.stop()
        self.db_patch.stop()
        for candidate in (
            self.db_path,
            self.db_path.with_name(f"{self.db_path.name}-wal"),
            self.db_path.with_name(f"{self.db_path.name}-shm"),
            self.machine_path,
        ):
            try:
                candidate.unlink(missing_ok=True)
            except PermissionError:
                pass

    def approve(self, machine, employee_id):
        approval_id = local_store.queue_approval(
            "manual_time",
            approval_payload(machine, employee_id),
            machine_no=machine,
            emp_id=employee_id,
            employee_name=f"Employee {employee_id}",
        )
        local_store.approve_approval_record(approval_id, reviewer={"emp_id": "100", "full_name": "Manager"})
        return approval_id

    def test_groups_by_date_and_area_into_on_hold_batches(self):
        first_id = self.approve("1", "1005")
        second_id = self.approve("E-2", "845")
        third_id = self.approve("3", "778")

        sync_env = {"ACUMATICA_ENABLED": "true", "ACUMATICA_CUTOVER_AT": "2026-01-01T00:00:00+00:00"}
        with patch.dict(os.environ, sync_env):
            result = sync_approved_entries(client_factory=FakeAcumaticaClient)

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["sent"], 3)
        self.assertEqual(len(FakeAcumaticaClient.batches), 2)
        batches_by_description = {
            entity_value(batch, "Description"): batch for batch in FakeAcumaticaClient.batches.values()
        }
        batch = batches_by_description["L1 CNC LABOR 09/29/2026"]
        self.assertTrue(entity_value(batch, "Hold"))
        self.assertEqual(len(batch["Details"]), 2)
        self.assertEqual(entity_value(batch["Details"][0], "Machine"), "E-1")
        self.assertEqual(entity_value(batch["Details"][1], "Machine"), "E-2")
        self.assertEqual(
            entity_value(batches_by_description["L2 CNC LABOR 09/29/2026"]["Details"][0], "Machine"),
            "E-3",
        )
        self.assertEqual(local_store.get_acumatica_sync_item(first_id)["status"], "sent")
        self.assertEqual(local_store.get_acumatica_sync_item(second_id)["status"], "sent")
        self.assertEqual(local_store.get_acumatica_sync_item(third_id)["status"], "sent")

        with patch.dict(os.environ, sync_env):
            retry = sync_approved_entries(client_factory=FakeAcumaticaClient)
        self.assertEqual(retry["sent"], 0)
        self.assertEqual(len(batch["Details"]), 2)

    def test_released_batch_correction_is_not_written(self):
        approval_id = self.approve("3", "778")
        sync_env = {"ACUMATICA_ENABLED": "true", "ACUMATICA_CUTOVER_AT": "2026-01-01T00:00:00+00:00"}
        with patch.dict(os.environ, sync_env):
            sync_approved_entries(client_factory=FakeAcumaticaClient)

        batch = next(iter(FakeAcumaticaClient.batches.values()))
        batch["Status"] = wrapped("Released")
        payload = approval_payload("3", "778", quantity=2)
        local_store.update_approval_with_revision(
            approval_id,
            payload,
            editor={"emp_id": "100", "full_name": "Manager"},
            reason="Correct quantity",
        )
        calls_before = FakeAcumaticaClient.put_calls

        with patch.dict(os.environ, sync_env):
            result = sync_approved_entries(client_factory=FakeAcumaticaClient)

        self.assertEqual(result["sent"], 0)
        self.assertEqual(FakeAcumaticaClient.put_calls, calls_before)
        self.assertEqual(local_store.get_acumatica_sync_item(approval_id)["status"], "released_attention")
        revisions = local_store.list_approval_revisions(approval_id)
        self.assertEqual(revisions[0]["change_reason"], "Correct quantity")

    def test_normalizes_machine_number(self):
        self.assertEqual(normalize_machine("1"), "E-1")
        self.assertEqual(normalize_machine("CNC 4"), "E-4")
        self.assertEqual(normalize_machine("E-6"), "E-6")

    def test_normalizes_acumatica_detail_type(self):
        self.assertEqual(normalize_detail_type("Machining"), "MACHINING")
        self.assertEqual(normalize_detail_type("Set-Up"), "SET-UP")
        self.assertEqual(normalize_detail_type("DT"), "DOWN TIME")

    def test_blank_inventory_is_left_for_acumatica_to_default(self):
        transaction = approval_payload("1", "1005")["acumatica_labor_transaction"]
        transaction["inventory_id"] = ""

        detail = _detail_payload(transaction, "[CNCAPP:1:v1:1]")

        self.assertNotIn("InventoryID", detail)

    def test_detail_payload_leaves_calculated_fields_to_acumatica(self):
        transaction = approval_payload("1", "1005")["acumatica_labor_transaction"]

        detail = _detail_payload(transaction, "[CNCAPP:1:v1:1]")

        for field in ("LaborRate", "LaborAmount", "UOM", "Warehouse", "QtyScrapped"):
            self.assertNotIn(field, detail)
        self.assertEqual(entity_value(detail, "Location"), "CUST REC")

    def test_extracts_nested_acumatica_field_errors(self):
        entity = {"Details": [{"OperationNbr": {"value": "0050", "error": "Invalid operation."}}]}

        errors = _entity_errors(entity)

        self.assertEqual(errors, ["Details[0].OperationNbr: Invalid operation."])

    def test_missing_production_order_fails_before_creating_batch(self):
        payload = approval_payload("1", "1005")
        payload["acumatica_labor_transaction"]["production_number"] = "EWO26-99999"
        approval_id = local_store.queue_approval(
            "manual_time",
            payload,
            machine_no="1",
            emp_id="1005",
            employee_name="Employee 1005",
        )
        local_store.approve_approval_record(
            approval_id,
            reviewer={"emp_id": "100", "full_name": "Manager"},
        )
        sync_env = {"ACUMATICA_ENABLED": "true", "ACUMATICA_CUTOVER_AT": "2026-01-01T00:00:00+00:00"}

        with patch.dict(os.environ, sync_env):
            result = sync_approved_entries(client_factory=FakeAcumaticaClient)

        self.assertEqual(result["failed"], 1)
        self.assertEqual(FakeAcumaticaClient.put_calls, 0)
        sync_item = local_store.get_acumatica_sync_item(approval_id)
        self.assertIn("does not exist", sync_item["last_error"])


if __name__ == "__main__":
    unittest.main()
