import os
import unittest
from unittest.mock import patch

import sharepoint_client
from time_entry_app.bridge import cnc_time_backend as backend


class ListCacheTests(unittest.TestCase):
    def setUp(self):
        backend.LIST_CACHE.clear()
        self.postgres_read_enabled = backend.POSTGRES_READ_ENABLED
        self.allow_direct_sharepoint_reads = backend.ALLOW_DIRECT_SHAREPOINT_READS
        backend.POSTGRES_READ_ENABLED = False
        backend.ALLOW_DIRECT_SHAREPOINT_READS = True

    def tearDown(self):
        backend.POSTGRES_READ_ENABLED = self.postgres_read_enabled
        backend.ALLOW_DIRECT_SHAREPOINT_READS = self.allow_direct_sharepoint_reads

    @patch.object(backend, "_get_time_entry_access_token", return_value="token")
    @patch.object(backend, "build_headers", return_value={"Authorization": "Bearer token"})
    @patch.object(backend, "get_site_id", return_value="site-id")
    @patch.object(
        backend,
        "get_list_items",
        side_effect=[
            [{"id": 1, "fields": {"Title": "Alpha"}}],
            [{"id": 2, "fields": {"Title": "Beta"}}],
        ],
    )
    def test_read_list_reuses_cached_items(self, mock_get_list_items, mock_get_site_id, mock_build_headers, mock_get_access_token):
        first = backend._read_list("employees")
        second = backend._read_list("employees")

        self.assertEqual(first, [{"id": 1, "fields": {"Title": "Alpha"}}])
        self.assertEqual(second, first)
        self.assertEqual(mock_get_list_items.call_count, 1)

    @patch.object(backend, "_load_local_machine_config")
    @patch.object(backend, "_read_list", side_effect=TimeoutError("Graph request timed out"))
    def test_sign_in_context_uses_local_machines_when_graph_is_unavailable(
        self, mock_read_list, mock_load_local_machine_config
    ):
        machines = [{"machine_no": "1", "email": "cnc1@example.com", "label": "Machine 1"}]
        mock_load_local_machine_config.return_value = machines

        context = backend.get_sign_in_context()

        self.assertEqual(context["machine_options"], machines)
        self.assertEqual(context["station_emails"], ["cnc1@example.com"])
        mock_read_list.assert_not_called()

    @patch.object(sharepoint_client, "get_access_token", return_value="token")
    def test_time_entry_placeholder_credentials_fall_back_to_shared_public_login(self, mock_get_access_token):
        with patch.dict(
            os.environ,
            {
                "CNC_TIME_SHAREPOINT_CLIENT_ID": "your-time-entry-app-client-id",
                "CNC_TIME_SHAREPOINT_CLIENT_SECRET": "your-time-entry-app-client-secret",
            },
        ):
            token = sharepoint_client.get_access_token_from_env("CNC_TIME_SHAREPOINT", allow_interactive=True)

        self.assertEqual(token, "token")
        self.assertEqual(mock_get_access_token.call_args.kwargs["client_id"], sharepoint_client.CLIENT_ID)
        self.assertEqual(mock_get_access_token.call_args.kwargs["client_secret"], "")
        self.assertTrue(mock_get_access_token.call_args.kwargs["allow_interactive"])

    @patch.object(
        backend,
        "_read_list",
        return_value=[
            {
                "id": 1,
                "fields": {
                    "Title": "Machining",
                    "OptionStep": 2,
                    "Branch": "Ennis",
                },
            }
        ],
    )
    def test_station_detail_types_are_added_to_synced_details(self, _mock_read_list):
        titles = [item["title"] for item in backend._detail_types()]

        self.assertIn("Machining", titles)
        self.assertIn("Inspection", titles)
        self.assertIn("Sandblast", titles)
        self.assertIn("Drift", titles)
        self.assertIn("Stenciling", titles)

    @patch.object(sharepoint_client, "graph_get", return_value={"value": []})
    def test_sharepoint_list_names_with_slashes_are_url_encoded(self, mock_graph_get):
        sharepoint_client.get_list_items(
            "https://benoitinc.sharepoint.com/sites/BenoitMaintenance1",
            "CNC Maintenace Pre-Use/Daily Checklist",
            {"Authorization": "Bearer token"},
            site_id="site-id",
        )

        endpoint = mock_graph_get.call_args.args[0]
        self.assertIn("CNC%20Maintenace%20Pre-Use%2FDaily%20Checklist", endpoint)

    @patch.object(sharepoint_client, "get_site_id", return_value="site-id")
    @patch.object(sharepoint_client, "get_list_items", return_value=[])
    def test_multiple_list_sync_can_fetch_by_id_and_store_by_name(self, mock_get_list_items, mock_get_site_id):
        site_results, _site_ids = sharepoint_client.get_multiple_lists(
            {
                "https://benoitinc.sharepoint.com/sites/BenoitMaintenance1": [
                    {
                        "name": "CNC Maintenace Pre-Use/Daily Checklist",
                        "id": "e5d9d01b-fb27-4abb-8d93-eb285c98c228",
                    }
                ]
            },
            headers={"Authorization": "Bearer token"},
        )

        self.assertIn(
            "CNC Maintenace Pre-Use/Daily Checklist",
            site_results["https://benoitinc.sharepoint.com/sites/BenoitMaintenance1"],
        )
        self.assertEqual(mock_get_list_items.call_args.args[1], "e5d9d01b-fb27-4abb-8d93-eb285c98c228")


if __name__ == "__main__":
    unittest.main()
