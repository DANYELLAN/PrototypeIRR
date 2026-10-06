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

    def test_sharepoint_reference_survives_reload(self):
        request = local_store.create_notification_request(
            "mold_approval", "1005", "Operator", "E-1", "EWO26-00009", {},
        )
        local_store.mark_notification_submitted(request["id"], "https://example.com", "list-id", "42")
        updated = local_store.get_notification_request(request["id"])
        self.assertEqual(updated["delivery_status"], "submitted")
        self.assertEqual(updated["sharepoint_item_id"], "42")
        self.assertEqual(updated["sharepoint_list_id"], "list-id")
        self.assertEqual(updated["sharepoint_site_url"], "https://example.com")

    def test_assistance_resolution_is_persisted_and_first_response_wins(self):
        request = local_store.create_notification_request(
            "assistance_asap", "1005", "Operator", "E-1", "", {"message": "Need help"},
            status="assistance_requested",
        )
        resolved_at = "2026-10-06T18:00:00Z"
        self.assertTrue(local_store.resolve_assistance_request(request["id"], "First Responder", "Fixed", resolved_at))
        self.assertFalse(local_store.resolve_assistance_request(request["id"], "Second Responder", "Other note"))
        saved = local_store.get_notification_request(request["id"])
        self.assertEqual(saved["status"], "resolved")
        self.assertEqual(saved["responder_name"], "First Responder")
        self.assertEqual(saved["response_note"], "Fixed")
        self.assertEqual(saved["responded_at"], resolved_at)

    def test_assistance_resolution_cannot_close_mold_request(self):
        request = local_store.create_notification_request("mold_approval", "1005", "Operator", "E-1", "WO", {})
        self.assertFalse(local_store.resolve_assistance_request(request["id"], "Responder"))
        self.assertEqual(local_store.get_notification_request(request["id"])["status"], "pending")


class NotificationBackendTests(unittest.TestCase):
    def setUp(self):
        self.employee = {"emp_id": "1005", "full_name": "Test Operator"}
        environment = patch.dict(backend.os.environ, {
            "CNC_TIME_NOTIFICATION_TRANSPORT": "webhook", "CNC_TIME_ASSISTANCE_SHAREPOINT_LIST_ID": "",
        })
        environment.start()
        self.addCleanup(environment.stop)

    @patch.dict(backend.os.environ, {
        "CNC_TIME_NOTIFICATION_TRANSPORT": "sharepoint", "CNC_TIME_ASSISTANCE_SHAREPOINT_LIST_ID": "assistance-list",
    })
    def test_sharepoint_submission_matches_existing_flow_fields(self):
        for kind, list_id, expected in [
            ("change_material", "7a805b80-4edd-4409-bf0c-0d3b26938999", {
                "Title": "Time Entry Request 12", "Machinist": "Test Operator",
                "MachineNumber": "E-1", "WO": "EWO26-00009", "MaterialType": "Re-Bore",
            }),
            ("mold_approval", "c7c723b1-123d-4a1b-b8a4-6e1b8b2a865e", {
                "Title": "Time Entry Request 12", "MachinistName": "Test Operator",
                "MachineNumber": "E-1", "WONumber": "EWO26-00009",
            }),
            ("assistance_asap", "assistance-list", {
                "Title": "Time Entry Request 12", "RequesterName": "Test Operator",
                "MachineNumber": "E-1", "WONumber": "EWO26-00009",
                "AssistanceNeeded": "Need help", "RequestStatus": "Open",
            }),
        ]:
            with self.subTest(kind=kind):
                record = {"id": 12, "notification_type": kind, "requester_name": "Test Operator",
                          "machine_no": "E-1", "work_order": "EWO26-00009",
                          "details": {"material_change_type": "Re-Bore", "message": "Need help"}}
                with patch.object(backend, "_get_time_entry_access_token", return_value="test-token"), \
                        patch.object(backend, "_site_id", return_value="site-id"), \
                        patch.object(backend, "_request", return_value={"id": "42"}) as graph, \
                        patch.object(backend, "mark_notification_submitted") as saved, \
                        patch.object(backend, "get_notification_request", return_value=record):
                    result = backend._submit_sharepoint_notification(record)
                self.assertEqual(graph.call_args.args[1], f"/sites/site-id/lists/{list_id}/items")
                self.assertEqual(graph.call_args.args[3], {"fields": expected})
                saved.assert_called_once_with(12, backend.EMPLOYEE_SITE, list_id, "42")
                self.assertTrue(result["submitted"])
                self.assertFalse(result["delivered"])

    def test_sharepoint_decisions_match_qc_buttons(self):
        record = {"id": 21, "notification_type": "mold_approval", "status": "pending",
                  "sharepoint_item_id": "42", "sharepoint_site_url": backend.EMPLOYEE_SITE,
                  "sharepoint_list_id": "qc-list"}
        for decision, expected in [("Approved", "approved_to_run"), ("Not Approved", "do_not_run")]:
            with self.subTest(decision=decision), \
                    patch.object(backend, "_get_time_entry_access_token", return_value="test-token"), \
                    patch.object(backend, "_site_id", return_value="site-id"), \
                    patch.object(backend, "_request", return_value={"fields": {
                        "ApprovalStatus": decision, "QCResponder": "QC Reviewer"}}) as graph, \
                    patch.object(backend, "decide_notification_request") as decide, \
                    patch.object(backend, "get_notification_request", return_value={**record, "status": expected}):
                refreshed = backend._refresh_sharepoint_mold_decision(record)
                self.assertEqual(refreshed["status"], expected)
                decide.assert_called_once_with(21, expected, "QC Reviewer")
                self.assertIn("/items/42?$expand=fields", graph.call_args.args[1])

    def test_sharepoint_read_failure_keeps_mold_pending(self):
        record = {"id": 21, "notification_type": "mold_approval", "status": "pending",
                  "sharepoint_item_id": "42", "sharepoint_site_url": backend.EMPLOYEE_SITE}
        with patch.object(backend, "_get_time_entry_access_token", side_effect=RuntimeError("offline")), \
                patch.object(backend, "decide_notification_request") as decide:
            refreshed = backend._refresh_sharepoint_mold_decision(record)
        self.assertEqual(refreshed["status"], "pending")
        self.assertIn("refresh_error", refreshed)
        decide.assert_not_called()

    def test_unknown_qc_status_does_not_approve(self):
        record = {"id": 21, "notification_type": "mold_approval", "status": "pending",
                  "sharepoint_item_id": "42", "sharepoint_site_url": backend.EMPLOYEE_SITE,
                  "sharepoint_list_id": "qc-list"}
        with patch.object(backend, "_get_time_entry_access_token", return_value="test-token"), \
                patch.object(backend, "_site_id", return_value="site-id"), \
                patch.object(backend, "_request", return_value={"fields": {"ApprovalStatus": "Pending"}}), \
                patch.object(backend, "decide_notification_request") as decide:
            self.assertEqual(backend._refresh_sharepoint_mold_decision(record)["status"], "pending")
        decide.assert_not_called()

    @patch.dict(backend.os.environ, {"CNC_TIME_NOTIFICATION_TRANSPORT": "sharepoint"})
    def test_failed_sharepoint_submit_is_saved_without_webhook_fallback(self):
        record = {"id": 12}
        with patch.object(backend, "_notification_work_orders", return_value=["EWO26-00009"]), \
                patch.object(backend, "create_notification_request", return_value=record), \
                patch.object(backend, "_submit_sharepoint_notification", side_effect=RuntimeError("offline")), \
                patch.object(backend, "update_notification_delivery") as delivery, \
                patch.object(backend, "_queue_notification") as queue:
            with self.assertRaisesRegex(ValueError, "saved but could not be submitted"):
                backend.submit_notification_request(self.employee, "E-1", "mold_approval", "EWO26-00009")
        delivery.assert_called_once_with(12, "failed", "offline")
        queue.assert_not_called()

    def test_status_refresh_only_uses_requests_owned_by_employee(self):
        own_record = {"id": 12, "notification_type": "mold_approval", "status": "pending"}
        with patch.object(backend, "ensure_db"), \
                patch.object(backend, "list_notification_requests", return_value=[own_record]) as listing, \
                patch.object(backend, "_notification_work_orders", return_value=[]), \
                patch.object(backend, "_refresh_sharepoint_mold_decision") as refresh:
            backend.get_notification_context("1005", request_id="99")
        listing.assert_called_once_with("1005", limit=30)
        refresh.assert_not_called()

    @patch.dict(backend.os.environ, {"CNC_TIME_NOTIFICATION_TRANSPORT": "sharepoint"})
    def test_assistance_retains_webhook_route(self):
        self.assertIsNone(backend._notification_sharepoint_target("assistance_asap"))

    def test_assistance_refresh_records_responder_note_and_time(self):
        record = {"id": 21, "notification_type": "assistance_asap", "status": "assistance_requested",
                  "sharepoint_item_id": "42", "sharepoint_site_url": backend.EMPLOYEE_SITE,
                  "sharepoint_list_id": "assistance-list"}
        with patch.object(backend, "_get_time_entry_access_token", return_value="test-token"), \
                patch.object(backend, "_site_id", return_value="site-id"), \
                patch.object(backend, "_request", return_value={"fields": {
                    "RequestStatus": "Resolved", "ResolvedBy": "Responder", "ResolutionNote": "Fixed",
                    "ResolvedAt": "2026-10-06T18:00:00Z"}}), \
                patch.object(backend, "resolve_assistance_request") as resolve, \
                patch.object(backend, "get_notification_request", return_value={**record, "status": "resolved"}):
            self.assertEqual(backend._refresh_sharepoint_assistance(record)["status"], "resolved")
        resolve.assert_called_once_with(21, "Responder", "Fixed", "2026-10-06T18:00:00Z")

    def test_assistance_read_failure_keeps_request_open(self):
        record = {"id": 21, "notification_type": "assistance_asap", "status": "assistance_requested",
                  "sharepoint_item_id": "42", "sharepoint_site_url": backend.EMPLOYEE_SITE}
        with patch.object(backend, "_get_time_entry_access_token", side_effect=RuntimeError("offline")), \
                patch.object(backend, "resolve_assistance_request") as resolve:
            refreshed = backend._refresh_sharepoint_assistance(record)
        self.assertEqual(refreshed["status"], "assistance_requested")
        self.assertIn("refresh_error", refreshed)
        resolve.assert_not_called()

    def test_assistance_context_refreshes_watched_request(self):
        record = {"id": 21, "notification_type": "assistance_asap", "status": "assistance_requested"}
        with patch.object(backend, "ensure_db"), \
                patch.object(backend, "list_notification_requests", return_value=[record]), \
                patch.object(backend, "_notification_work_orders", return_value=[]), \
                patch.object(backend, "_refresh_sharepoint_assistance", return_value={**record, "status": "resolved"}) as refresh:
            context = backend.get_notification_context("1005", "21")
        refresh.assert_called_once_with(record)
        self.assertEqual(context["requests"][0]["status"], "resolved")

    @patch.dict(backend.os.environ, {"CNC_TIME_ASSISTANCE_WEBHOOK_URL": ""})
    def test_assistance_outbox_targets_all_nine_recipients(self):
        record = {"id": 21, "created_at": "2026-10-06T18:00:00Z"}
        with patch.object(backend, "create_notification_request", return_value=record) as create, \
                patch.object(backend, "_queue_notification") as queue:
            backend.submit_notification_request(self.employee, "E-1", "assistance_asap", message="Need help")
        recipients = queue.call_args.args[1]["recipient_emails"]
        self.assertEqual(recipients, ["nhess@benoit-inc.com", "mmarrs@benoit-inc.com", "dsaia@benoit-inc.com",
            "fmorales@benoit-inc.com", "gsertima@benoit-inc.com", "jalmanza@benoit-inc.com",
            "jsilos@benoit-inc.com", "skidd@benoit-inc.com", "rgreen@benoit-inc.com"])
        self.assertEqual(create.call_args.args[5]["recipient_emails"], recipients)
        self.assertEqual(queue.call_args.args[1]["chat_id"], "19:8fa29ff440e743f495bbd994832463ff@thread.v2")
        self.assertEqual(create.call_args.args[5]["chat_id"], queue.call_args.args[1]["chat_id"])

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
