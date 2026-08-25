import unittest
from unittest.mock import patch

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


if __name__ == "__main__":
    unittest.main()
