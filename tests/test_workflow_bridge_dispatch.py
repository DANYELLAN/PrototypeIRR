import unittest
from unittest.mock import patch
from node_app.bridge import workflow_bridge as bridge

class WorkflowBridgeDispatchTests(unittest.TestCase):
    def test_transport_action_is_not_forwarded_to_handlers(self):
        cases = [
            ('inspection_break_state', {'session_id': 7}),
            ('start_inspection_break', {'session_id': 7, 'mode': 'without_relief', 'expected_version': 0}),
            ('end_inspection_break', {'session_id': 7, 'expected_version': 1}),
            ('get_break_report', {'reviewer_adp': 'TEST', 'page': 1}),
            ('lunch_target', {'station_id': 7, 'reviewer_adp': 'TEST'}),
            ('start_lunch', {'station_id': 7, 'reviewer_adp': 'TEST', 'source_attempt_id': 1, 'confirmed_pipe_number': '001'}),
            ('get_lunch', {'inspection_id': 1, 'reviewer_adp': 'TEST'}),
            ('complete_lunch', {'inspection_id': 1, 'reviewer_adp': 'TEST', 'values': {}}),
            ('lunch_history', {'reviewer_adp': 'TEST', 'page': 1}),
            ('mold_approval_context', {'inspector_adp': 'TEST'}),
        ]
        for action, args in cases:
            with self.subTest(action=action), patch.object(bridge, '_payload', return_value={'action': action, **args}), patch.object(bridge, action, autospec=True, return_value={'ok': True}) as handler, patch.object(bridge, '_result') as result:
                bridge.main()
                handler.assert_called_once_with(**args)
                result.assert_called_once_with({'ok': True})

if __name__ == '__main__': unittest.main()
