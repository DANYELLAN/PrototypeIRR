import unittest
from unittest.mock import patch

import workflow_db


class RecipeBuilderOptionTests(unittest.TestCase):
    @patch.object(workflow_db, "_fetch_all_dicts")
    def test_saved_local_elements_are_available_without_duplicates(self, fetch_all):
        fetch_all.side_effect = [
            [
                {
                    "sharepoint_item_id": "1",
                    "fields_json": {
                        "RecipeName": "SharePoint Recipe",
                        "Branch": "Ennis",
                        "RecipeJson": {
                            "elements": [
                                {"element": "Laser Scan", "gauge": "Laser Gauge"},
                                {"element": "standoff", "gauge": "Visual"},
                            ]
                        },
                    },
                }
            ],
            [
                {"element_description": "Custom Thread Root", "gauge": "Profile Projector"},
                {"element_description": "  custom   thread root  ", "gauge": "profile projector"},
            ],
        ]

        options = workflow_db.get_recipe_builder_options("Ennis")

        self.assertIn("Custom Thread Root", options["element_options"])
        self.assertIn("Laser Scan", options["element_options"])
        self.assertEqual(
            sum(item.casefold() == "custom thread root" for item in options["element_options"]),
            1,
        )
        self.assertEqual(
            sum(item.casefold() == "standoff" for item in options["element_options"]),
            1,
        )
        self.assertIn("Profile Projector", options["gauge_options"])


if __name__ == "__main__":
    unittest.main()
