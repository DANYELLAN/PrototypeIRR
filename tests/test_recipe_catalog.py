import unittest

from workflow_db import _merge_recipe_catalog


class RecipeCatalogTests(unittest.TestCase):
    def test_merges_local_and_sharepoint_copy_without_duplicate(self):
        local = [
            {
                "id": 12,
                "recipe_name": "2.875 7.90# BTS-6 PIN Recipe",
                "branch": "Ennis",
                "recipe_version": 2,
                "drawing": "013 Rev 2",
                "updated_at": "2026-09-17T10:00:00",
            }
        ]
        sharepoint = [
            {
                "item_id": "44",
                "recipe_name": "  2.875 7.90# BTS-6 PIN Recipe ",
                "branch": "Ennis",
                "recipe_version": 2,
                "drawing": "013 Rev 2",
                "updated_at": "2026-09-17T09:00:00",
            }
        ]

        catalog = _merge_recipe_catalog(local, sharepoint, "Ennis")

        self.assertEqual(len(catalog), 1)
        self.assertEqual(catalog[0]["source"], "Local + SharePoint")
        self.assertEqual(catalog[0]["local_recipe_id"], 12)
        self.assertEqual(catalog[0]["sharepoint_item_id"], "44")

    def test_keeps_latest_revision_and_filters_other_branches(self):
        sharepoint = [
            {
                "item_id": "1",
                "recipe_name": "Premium Connection Recipe",
                "branch": "Ennis",
                "recipe_version": 1,
            },
            {
                "item_id": "2",
                "recipe_name": "premium connection recipe",
                "branch": "Ennis",
                "recipe_version": 3,
            },
            {
                "item_id": "3",
                "recipe_name": "Houston Recipe",
                "branch": "Houston",
                "recipe_version": 5,
            },
        ]

        catalog = _merge_recipe_catalog([], sharepoint, "Ennis")

        self.assertEqual(len(catalog), 1)
        self.assertEqual(catalog[0]["recipe_version"], 3)
        self.assertEqual(catalog[0]["sharepoint_item_id"], "2")
        self.assertEqual(catalog[0]["source"], "SharePoint")

    def test_unfiltered_catalog_preserves_same_name_in_different_branches(self):
        sharepoint = [
            {"item_id": "1", "recipe_name": "Common Recipe", "branch": "Ennis"},
            {"item_id": "2", "recipe_name": "Common Recipe", "branch": "Houston"},
        ]

        catalog = _merge_recipe_catalog([], sharepoint)

        self.assertEqual(len(catalog), 2)


if __name__ == "__main__":
    unittest.main()
