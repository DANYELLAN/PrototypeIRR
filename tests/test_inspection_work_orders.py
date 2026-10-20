import unittest
from unittest.mock import patch

import workflow_db as db


BOX = 'Turn & Bore on BOX End with Size: 2 7/8", Weight: 7.90#, Connection: BTS-6 Box as Per Benoit Print# 014 - Rev. 2'
PIN = BOX.replace('BOX End', 'PIN End').replace('BTS-6 Box', 'BTS-6 Pin').replace('014', '013')


class InspectionWorkOrderTests(unittest.TestCase):
    def test_work_order_includes_description_operation_and_derived_specification(self):
        row = {'sharepoint_item_id': 1, 'fields_json': {'ProductionNumber': 'TEST-WO', 'OperationID': '0020', 'OperationDescription': BOX}}
        with patch.object(db, 'PRODUCTION_NUMBER_FIELDS', ['ProductionNumber']), patch.object(db, 'PRODUCTION_OPERATION_DESCRIPTION_FIELDS', ['OperationDescription']):
            result = db._normalize_production_order_row(row)
        self.assertEqual(result['operation_id'], '0020')
        self.assertEqual(result['inspection_entry'], {'size': '2 7/8"', 'weight': '7.90#', 'connection': 'BTS-6', 'end_type': 'BOX', 'drawing': '014 - Rev. 2'})

    def test_end_and_fraction_are_normalized_without_confusing_connection_variants(self):
        self.assertEqual(db._extract_inspection_entry_parts(PIN)['end_type'], 'PIN')
        self.assertEqual(db._inspection_dimension('2 7/8"'), db._inspection_dimension('2.875'))
        recipe = {'size_label': '2.875', 'weight_label': '7.90#', 'grade_label': 'BTS-6', 'connector_type': 'BOX', 'drawing': '014 Rev 2'}
        self.assertEqual(db._inspection_recipe_score(BOX, recipe), 110)
        for key, bad in [('size_label', '2.375'), ('weight_label', '6.50#'), ('grade_label', 'BTS-6 PR'), ('connector_type', 'PIN')]:
            with self.subTest(key=key):
                self.assertIsNone(db._inspection_recipe_score(BOX, {**recipe, key: bad}))

    def test_sharepoint_report_metadata_can_match_without_local_recipe_fields(self):
        recipe = {'recipe_name': '2.875 7.90# BTS-6 BOX Recipe', 'recipe_json': {'sourceReport': '2.875 7.90# BTS-6 (BOX) INSPECTION REPORT', 'drawing': '014 Rev 2'}}
        self.assertEqual(db._inspection_recipe_score(BOX, recipe), 110)
        self.assertIsNone(db._inspection_recipe_score(PIN, recipe))
        self.assertIsNone(db._inspection_recipe_score('Materials', recipe))

    def test_candidate_search_filters_wrong_specs_and_branches_and_ranks_drawing_match(self):
        correct = {'recipe_name': 'Correct', 'connection_type': '', 'size_label': '2.875', 'weight_label': '7.90#', 'grade_label': 'BTS-6', 'connector_type': 'BOX', 'drawing': '014 Rev 2', 'recipe_version': 1, 'branch': 'Ennis'}
        candidates = [correct, {**correct, 'recipe_name': 'Wrong end', 'connector_type': 'PIN'}, {**correct, 'recipe_name': 'Wrong size', 'size_label': '2.375'},
                      {**correct, 'recipe_name': 'Other branch', 'branch': 'Houston'}, {**correct, 'recipe_name': 'Other drawing', 'drawing': '014 Rev 1'},
                      {**correct, 'recipe_name': 'No drawing', 'drawing': None}]
        with patch.object(db, '_fetch_all_dicts', side_effect=[[], candidates, []]):
            result = db.find_recipe_candidates(BOX, 'Ennis')
        self.assertEqual([r['recipe_name'] for r in result], ['Correct', 'No drawing'])
        self.assertEqual(result[0]['match_type'], 'specification')
        self.assertGreater(result[0]['score'], result[1]['score'])

    def test_complete_wo_specs_do_not_fall_back_to_generic_word_overlap(self):
        with patch.object(db, '_fetch_all_dicts', side_effect=[[], [{'recipe_name': 'Unrelated BOX Recipe', 'branch': 'Ennis', 'recipe_version': 1}], []]):
            self.assertEqual(db.find_recipe_candidates(BOX, 'Ennis'), [])

    def test_explicit_description_alias_remains_authoritative(self):
        with patch.object(db, '_fetch_all_dicts', return_value=[{'recipe_name': 'Mapped IRR', 'confidence_rank': 1}]):
            result = db.find_recipe_candidates(BOX, 'Ennis')
        self.assertEqual(result[0]['recipe_name'], 'Mapped IRR')
        self.assertEqual(result[0]['match_type'], 'alias')

    def test_existing_decimal_connection_is_found_from_fractional_wo_details(self):
        stored = {'id': 10, 'production_number': 'TEST-WO', 'pipe_number': '001', 'operation_description': '2.875 7.90 BTS - 6 BOX', 'current_status': 'completed'}
        with patch.object(db, '_fetch_one_dict', return_value=None), patch.object(db, '_fetch_all_dicts', return_value=[stored]):
            self.assertEqual(db.get_pipe_unit('TEST-WO', '2 7/8" 7.90# BTS-6 BOX', '001'), stored)
        with patch.object(db, '_fetch_one_dict', return_value=None), patch.object(db, '_fetch_all_dicts', return_value=[stored, {**stored, 'id': 11}]):
            with self.assertRaisesRegex(ValueError, 'Multiple saved pipe records'):
                db.get_pipe_unit('TEST-WO', '2 7/8" 7.90# BTS-6 BOX', '001')

    def test_history_filter_recognizes_equivalent_connection_labels(self):
        stored = {'id': 10, 'operation_description': '2.875 7.90 BTS - 6 BOX'}
        with patch.object(db, '_fetch_all_dicts', return_value=[stored, {'id': 11, 'operation_description': '2.875 7.90# BTS-6 PIN'}]):
            rows = db.search_pipe_units(production_number='TEST-WO', operation_description='2 7/8" 7.90# BTS-6 BOX')
        self.assertEqual(rows, [stored])

    def test_start_uses_existing_connection_label_so_completed_pipe_cannot_be_reentered(self):
        from unittest.mock import MagicMock
        stored = {'id': 10, 'operation_description': '2.875 7.90 BTS - 6 BOX', 'current_status': 'completed', 'latest_attempt_no': 1}
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = stored
        with patch('node_app.bridge.inspection_breaks.inspection_break_state', return_value={'mode':None,'version':0}), patch.object(db, 'get_pipe_unit', return_value=stored), patch.object(db, 'get_db_connection', return_value=connection):
            with self.assertRaisesRegex(ValueError, 'can not be re-entered'):
                db.create_inspection_attempt('TEST-WO', '2 7/8" 7.90# BTS-6 BOX', '001', 'Ennis', 1, {}, {}, 'IRR', {})
        self.assertEqual(cursor.execute.call_args.args[1][1], stored['operation_description'])
        connection.commit.assert_not_called()


if __name__ == '__main__':
    unittest.main()
