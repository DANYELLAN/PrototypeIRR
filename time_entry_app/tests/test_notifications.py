import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from time_entry_app import local_store
from time_entry_app.bridge import cnc_time_backend as backend


class NotificationStoreTests(unittest.TestCase):
    def setUp(self):
        self.original_db_path = local_store.DB_PATH
        temp_dir = Path(__file__).resolve().parent / ".tmp"
        temp_dir.mkdir(exist_ok=True)
        self.db_path = temp_dir / f"notifications_{uuid.uuid4().hex}.db"
        local_store.DB_PATH = self.db_path

    def tearDown(self):
        local_store.DB_PATH = self.original_db_path
        for suffix in ("", "-shm", "-wal"):
            candidate = Path(f"{self.db_path}{suffix}")
            if candidate.exists():
                candidate.unlink()

    def test_notification_request_lifecycle_is_persisted(self):
        request = local_store.create_notification_request(
            "mold_approval",
            "1005",
            "Test Operator",
            "E-1",
            "EWO26-00009",
            {},
            callback_token="test-token",
        )

        self.assertEqual(request["status"], "pending")
        self.assertEqual(request["delivery_status"], "queued")
        self.assertTrue(
            local_store.decide_notification_request(
                request["id"],
                "approved_to_run",
                "Quality Reviewer",
                "Mold verified",
            )
        )

        updated = local_store.get_notification_request(request["id"])
        self.assertEqual(updated["status"], "approved_to_run")
        self.assertEqual(updated["responder_name"], "Quality Reviewer")
        self.assertEqual(updated["response_note"], "Mold verified")


class NotificationBackendTests(unittest.TestCase):
    def setUp(self):
        self.employee = {"emp_id": "1005", "full_name": "Test Operator"}

    @patch.object(backend, "_work_orders")
    def test_notification_work_orders_are_descending_and_exclude_completed(self, work_orders):
        work_orders.return_value = [
            {"production_number": "EWO26-00008", "order_type": "EN", "status": "Released"},
            {"production_number": "EWO26-00010", "order_type": "EN", "status": "In Process"},
            {"production_number": "EWO26-00009", "order_type": "EN", "status": "Planned"},
            {"production_number": "EWO26-00011", "order_type": "EN", "status": "Completed"},
        ]

        result = backend._notification_work_orders()

        self.assertEqual(result, ["EWO26-00010", "EWO26-00009", "EWO26-00008"])

    @patch.dict(
        backend.os.environ,
        {
            "CNC_TIME_CHANGE_MATERIAL_WEBHOOK_URL": "",
            "CNC_TIME_NOTIFICATION_CALLBACK_BASE_URL": "",
        },
        clear=False,
    )
    @patch.object(backend, "_queue_notification")
    @patch.object(backend, "create_notification_request")
    @patch.object(backend, "_notification_work_orders", return_value=["EWO26-00009"])
    def test_change_material_uses_confirmed_message(self, _work_orders, create_request, queue_notification):
        create_request.return_value = {
            "id": 12,
            "notification_type": "change_material",
            "status": "notified",
            "delivery_status": "queued",
            "created_at": "2026-10-05T12:00:00+00:00",
            "details": {},
        }

        result = backend.submit_notification_request(
            self.employee,
            "E-1",
            "change_material",
            "EWO26-00009",
            "Re-Bore",
            "Use revised stock.",
        )

        self.assertTrue(result["queued"])
        payload = queue_notification.call_args.args[1]
        self.assertIn("Machinist Test Operator at E-1", payload["message"])
        self.assertIn("Re-Bore material change", payload["message"])
        self.assertIn("EWO26-00009", payload["message"])

    @patch.object(backend, "get_notification_request")
    @patch.object(backend, "decide_notification_request", return_value=True)
    def test_mold_response_requires_token_and_updates_decision(self, decide_request, get_request):
        pending = {
            "id": 21,
            "notification_type": "mold_approval",
            "status": "pending",
            "callback_token": "correct-token",
        }
        approved = {
            **pending,
            "status": "approved_to_run",
            "responder_name": "Quality Reviewer",
        }
        get_request.side_effect = [pending, approved]

        result = backend.respond_to_mold_approval(
            21,
            "correct-token",
            "approved",
            "Quality Reviewer",
            "Approved sample",
        )

        self.assertEqual(result["status"], "approved_to_run")
        self.assertNotIn("callback_token", result)
        decide_request.assert_called_once_with(21, "approved_to_run", "Quality Reviewer", "Approved sample")

    @patch.object(backend, "get_notification_request")
    def test_mold_response_rejects_invalid_token(self, get_request):
        get_request.return_value = {
            "id": 22,
            "notification_type": "mold_approval",
            "status": "pending",
            "callback_token": "correct-token",
        }

        with self.assertRaisesRegex(ValueError, "Invalid mold approval response token"):
            backend.respond_to_mold_approval(22, "wrong-token", "approved")


if __name__ == "__main__":
    unittest.main()
