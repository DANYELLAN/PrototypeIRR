import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from node_app.bridge import mold_approval as mold

class MoldApprovalTests(unittest.TestCase):
    def setUp(self):
        self.employee={'adp_number':'123','name':'Test Inspector'}
        self.lookup=patch.object(mold.db,'get_employee_by_adp',return_value=self.employee)
        self.lookup.start(); self.addCleanup(self.lookup.stop)

    def test_request_uses_verified_employee_and_active_station(self):
        with patch.object(mold.db,'_fetch_one_dict',return_value={'inspector_adp':'123','location_name':'E-1','logged_out_at':None}), patch.object(mold.notifications,'submit_notification_request',return_value={'id':7}) as send:
            self.assertEqual(mold.request_mold_approval('123',5,'WO')['id'],7)
            self.assertEqual(send.call_args.kwargs,{'employee':{'emp_id':'123','full_name':'Test Inspector'},'machine_no':'E-1','notification_type':'mold_approval','work_order':'WO'})

    def test_closed_or_foreign_session_cannot_request(self):
        with patch.object(mold.notifications,'submit_notification_request') as send:
            for station in [None,{'inspector_adp':'other','location_name':'E-1','logged_out_at':None},{'inspector_adp':'123','location_name':'E-1','logged_out_at':'ended'}]:
                with patch.object(mold.db,'_fetch_one_dict',return_value=station), self.assertRaises(ValueError):mold.request_mold_approval('123',5,'WO')
            send.assert_not_called()

    def test_ownership_type_and_callback_token_privacy(self):
        base={'id':7,'notification_type':'mold_approval','requester_emp_id':'123','status':'pending','callback_token':'secret'}
        with patch.object(mold.notifications,'ensure_db'), patch.object(mold.notifications,'_refresh_sharepoint_mold_decision',side_effect=lambda row:row) as refresh:
            for changes in [{'requester_emp_id':'other'},{'notification_type':'assistance_asap'}]:
                with patch.object(mold.notifications,'get_notification_request',return_value={**base,**changes}),self.assertRaises(ValueError):mold.mold_approval_status('123',7)
            refresh.assert_not_called()
            with patch.object(mold.notifications,'get_notification_request',return_value=base):
                self.assertNotIn('callback_token',mold.mold_approval_status('123',7))

    def test_type_filter_applies_before_limit_and_old_history_retained(self):
        store=mold.notifications.list_notification_requests.__globals__
        with tempfile.TemporaryDirectory() as folder, patch.dict(store,DB_PATH=Path(folder)/'notifications.db'):
            old=mold.notifications.create_notification_request('mold_approval','123','Inspector','E-1','OLD',{},callback_token='secret')
            for _ in range(35):mold.notifications.create_notification_request('change_material','123','Inspector','E-1','OTHER',{})
            mold.notifications.create_notification_request('mold_approval','other','Other','E-1','PRIVATE',{})
            with patch.object(mold.notifications,'_notification_work_orders',return_value=['WO']):
                result=mold.mold_approval_context('123')
            self.assertEqual([row['id'] for row in result['requests']],[old['id']])
            self.assertNotIn('callback_token',result['requests'][0])
            self.assertEqual(len(mold.notifications.list_notification_requests('123')),36)

if __name__=='__main__':unittest.main()
