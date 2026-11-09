import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from time_entry_app.bridge import cnc_time_backend as b
store=__import__('local_store')

class MaintenanceSharePointTests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
        for target,key,value in [(store,'DB_PATH',Path(temp.name)/'test.db'),(store,'LOCAL_MACHINE_CONFIG',Path(temp.name)/'machines.json'),(b,'IT_SHAREPOINT_WRITES_ENABLED',False),(b,'SHAREPOINT_WRITES_ENABLED',False),(b,'MAINTENANCE_SHAREPOINT_WRITES_ENABLED',True)]:
            p=patch.object(target,key,value);p.start();self.addCleanup(p.stop)
        self.fields={'Title':'Test','Description':'No real work','Priority':'Low'}
    def submit(self,description='Test request'):
        return b.submit_maintenance_request({'emp_id':'TEST','full_name':'Test operator'},'TEST','Test operator','Test',description,'Low','1','2','test@example.com')
    def test_maintenance_switch_cannot_enable_other_lists(self):
        self.assertTrue(b._sharepoint_writes_enabled('maintenance_requests'))
        for key in ['startstop','timeentry','checklist']:
            self.assertFalse(b._sharepoint_writes_enabled(key))
            with self.assertRaises(b.SharePointWritesDisabled):b._create_item(key,{})
    def test_success_saves_and_marks_local_request_synced(self):
        with patch.object(b,'_read_list',return_value=[]),patch.object(b,'_find_maintenance_request',return_value=None),patch.object(b,'_create_item',return_value={'id':'123'}) as create:
            result=self.submit()
        self.assertTrue(result['submitted']);self.assertEqual(result['sharepoint_item_id'],'123')
        self.assertEqual(store.list_pending_records(),[])
        self.assertEqual(len(create.call_args.args[1]['TimeEntryRequestKey']),32)
    def test_failed_delivery_keeps_original_request_for_retry(self):
        with patch.object(b,'_read_list',return_value=[]),patch.object(b,'_find_maintenance_request',side_effect=RuntimeError('offline')):
            result=self.submit()
        self.assertFalse(result['submitted']);self.assertTrue(result['queued'])
        self.assertEqual(len(store.list_pending_records(record_types=['maintenance_request'])),1)
    def test_retry_only_delivers_maintenance_even_beyond_first_batch(self):
        store.queue_record('maintenance_request',{'fields':self.fields})
        for _ in range(60):store.queue_record('startstop',{'fields':{'Title':'Do not send'}})
        with patch.object(b,'_deliver_maintenance_request',return_value={'id':'123'}) as deliver:
            result=b.sync_pending_writes_to_sharepoint(limit=1)
        self.assertEqual(result['synced'],1);deliver.assert_called_once()
        self.assertEqual(len(store.list_pending_records(limit=100)),60)
    def test_older_request_gets_same_key_on_each_retry(self):
        store.queue_record('maintenance_request',{'fields':self.fields})
        with patch.object(b,'_deliver_maintenance_request',side_effect=RuntimeError('offline')) as deliver:
            b.sync_pending_writes_to_sharepoint();b.sync_pending_writes_to_sharepoint()
        self.assertEqual(deliver.call_args_list[0].args[1],deliver.call_args_list[1].args[1])
    def test_existing_remote_request_is_not_created_again(self):
        with patch.object(b,'_find_maintenance_request',return_value={'id':'123'}),patch.object(b,'_create_item') as create:
            self.assertEqual(b._deliver_maintenance_request(self.fields,'key')['id'],'123')
        create.assert_not_called()
    def test_lost_post_response_recovers_existing_request(self):
        with patch.object(b,'_find_maintenance_request',side_effect=[None,{'id':'123'}]),patch.object(b,'_create_item',side_effect=TimeoutError('response lost')):
            self.assertEqual(b._deliver_maintenance_request(self.fields,'key')['id'],'123')
    def test_invalid_data_does_not_create_a_local_queue_entry(self):
        with self.assertRaises(ValueError):self.submit('x'*256)
        self.assertEqual(store.list_pending_records(),[])
    def test_fully_disabled_replay_never_sends(self):
        store.queue_record('maintenance_request',{'fields':self.fields})
        with patch.object(b,'MAINTENANCE_SHAREPOINT_WRITES_ENABLED',False),patch.object(b,'_deliver_maintenance_request') as deliver:
            result=b.sync_pending_writes_to_sharepoint()
        self.assertTrue(result['dry_run']);deliver.assert_not_called()

if __name__=='__main__':unittest.main()
