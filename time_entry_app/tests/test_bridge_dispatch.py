import io
import json
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from time_entry_app.bridge import cnc_time_bridge as bridge
from time_entry_app.bridge import timed_downtime
from time_entry_app.bridge import setup_transition
from time_entry_app.bridge import entry_completion
from time_entry_app.bridge import tablet_time


class BridgeDispatchTests(unittest.TestCase):
    def test_time_page_bundle_preserves_all_three_reads_and_review_date(self):
        session={'employee':{'emp_id':'1','full_name':'Operator'},'machine_no':'CNC 1','shift_id':'40','user_email':'station@example.com'}
        with patch.object(bridge,'get_dashboard_context',return_value={'work_orders':['WO1']}) as context, \
             patch.object(tablet_time,'active_timer',return_value={'id':'timer'}) as timer, \
             patch.object(bridge,'get_daily_time_review',return_value={'required':True}) as review:
            result=self.dispatch({'action':'get_time_page_context','session':session,'labor_date':'2026-11-10'})
        context.assert_called_once_with('1','station@example.com')
        timer.assert_called_once_with(session)
        review.assert_called_once_with(session['employee'],'2026-11-10')
        self.assertEqual(result['data'],{'context':{'work_orders':['WO1']},'timer':{'id':'timer'},'review':{'required':True}})

    def test_starting_pipe_uses_signed_in_session_and_source_entry(self):
        session={'employee':{'emp_id':'1'}}
        with patch.object(entry_completion,'save_starting_pipe',return_value={'starting_pipe':'10'}) as handler:
            self.dispatch({'action':'save_starting_pipe','session':session,'entry_id':'entry','starting_pipe':'10'})
        handler.assert_called_once_with(session,'entry','10')

    def test_stop_dispatch_keeps_ending_pipe_and_owner_for_server_quantity_calculation(self):
        with patch.object(bridge,'stop_time_entry',return_value={'approval_id':1}) as handler:
            self.dispatch({'action':'stop_time_entry','entry_id':'entry','quantity':'999','ending_pipe':'20','original_adp':'1','machine_no':'Line 1 - Drift'})
        handler.assert_called_once_with('entry','999','20','1','Line 1 - Drift')

    def test_setup_handoff_uses_the_parsed_signed_in_session(self):
        session = {'employee': {'emp_id': '1'}}
        with patch.object(setup_transition, 'end_setup', return_value={'approval_id': 8}) as handler:
            result = self.dispatch({'action': 'end_setup', 'session': session, 'entry_id': 'setup', 'detail_type': 'Machining', 'quantity': '7'})
        self.assertEqual(result['data']['approval_id'], 8)
        handler.assert_called_once_with(session, 'setup', 'Machining', '7')

    def dispatch(self, payload):
        output = io.StringIO()
        with patch.object(bridge.sys, 'argv', ['bridge.py', json.dumps(payload)]), redirect_stdout(output):
            bridge.main()
        return json.loads(output.getvalue())

    def test_sign_in_context_still_parses_and_dispatches(self):
        with patch.object(bridge, 'get_sign_in_context', return_value={'stations': []}) as handler:
            self.assertEqual(self.dispatch({'action': 'get_sign_in_context'}), {'ok': True, 'data': {'stations': []}})
        handler.assert_called_once_with()

    def test_start_downtime_dispatches_only_after_parsing_payload(self):
        session = {'employee': {'emp_id': '1'}}
        with patch.object(timed_downtime, 'start_downtime', return_value={'id': 'test'}) as handler:
            result = self.dispatch({'action': 'start_timed_downtime', 'session': session,
                                    'entry_id': 'entry', 'reason': 'M1', 'comments': 'Repair'})
        self.assertTrue(result['ok'])
        handler.assert_called_once_with(session, 'entry', 'M1', 'Repair')

    def test_end_downtime_dispatches_to_idempotent_handler(self):
        session = {'employee': {'emp_id': '1'}}
        with patch.object(timed_downtime, 'end_downtime', return_value={'approval_id': 7}) as handler:
            result = self.dispatch({'action': 'end_timed_downtime', 'session': session, 'downtime_id': 'test'})
        self.assertEqual(result['data']['approval_id'], 7)
        handler.assert_called_once_with(session, 'test')


if __name__ == '__main__':
    unittest.main()
