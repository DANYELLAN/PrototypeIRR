import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from datetime import datetime,timezone
from time_entry_app.bridge import cnc_time_backend as b
from time_entry_app.scripts.migrate_it_outbox import import_it_outbox
store=__import__('local_store')

class ITSharePointTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        for target,key,value in [(store,'DB_PATH',Path(self.temp.name)/'test.db'),(store,'LOCAL_MACHINE_CONFIG',Path(self.temp.name)/'machines.json'),(b,'SHAREPOINT_WRITES_ENABLED',False),(b,'MAINTENANCE_SHAREPOINT_WRITES_ENABLED',False),(b,'IT_SHAREPOINT_WRITES_ENABLED',True)]:
            p=patch.object(target,key,value);p.start();self.addCleanup(p.stop)
    def submit(self):
        return b.send_it_request('test@example.com','Test Operator','CNC 5','Software','Screen is frozen','Quality')
    def test_fields_match_actual_list_and_keep_full_description(self):
        issue='Description '*100
        fields=b._it_request_fields('test@example.com','Operator','CNC 5','Software',issue,'Quality','2026-11-05T01:00:00Z')
        self.assertEqual(fields['Status'],'New');self.assertEqual(fields['Location'],'Ennis')
        self.assertEqual(fields['IssueSubjec'],'Software');self.assertEqual(fields['Department'],'Quality')
        self.assertIn('Machine: CNC 5',fields['IssueDescription']);self.assertIn(issue.strip(),fields['IssueDescription'])
        self.assertEqual(fields['DateofIssue'],'2026-11-04')
        self.assertNotIn('Priority',fields)
    def test_success_is_saved_and_synced(self):
        with patch.object(b,'_find_it_request',return_value=None),patch.object(b,'_create_item',return_value={'id':'25'}) as create:
            result=self.submit()
        self.assertTrue(result['delivered']);self.assertEqual(result['sharepoint_item_id'],'25')
        self.assertEqual(create.call_args.args[0],'it_support_requests');self.assertEqual(store.list_pending_records(),[])
    def test_connection_failure_retains_request_and_retry_sends_it(self):
        with patch.object(b,'_find_it_request',side_effect=RuntimeError('offline')):
            result=self.submit()
        self.assertTrue(result['queued']);self.assertFalse(result['delivered'])
        with patch.object(b,'_deliver_it_request',return_value={'id':'25'}):
            result=b.sync_pending_writes_to_sharepoint()
        self.assertEqual(result['synced'],1);self.assertEqual(store.list_pending_records(),[])
    def test_only_it_records_can_be_sent_with_it_switch(self):
        store.queue_record('maintenance_request',{'fields':{'Title':'Leave pending'}})
        store.queue_record('startstop',{'fields':{'Title':'Leave pending'}})
        with patch.object(b,'_find_it_request',side_effect=RuntimeError('offline')):self.submit()
        with patch.object(b,'_deliver_it_request',return_value={'id':'25'}),patch.object(b,'_deliver_maintenance_request') as maintenance:
            result=b.sync_pending_writes_to_sharepoint()
        self.assertEqual(result['synced'],1);maintenance.assert_not_called()
        self.assertEqual(len(store.list_pending_records()),2)
        for key in ['startstop','timeentry','maintenance_requests']:
            with self.assertRaises(b.SharePointWritesDisabled):b._create_item(key,{})
    def test_existing_item_and_lost_response_do_not_create_duplicates(self):
        with patch.object(b,'_find_it_request',return_value={'id':'25'}),patch.object(b,'_create_item') as create:
            self.assertEqual(b._deliver_it_request({},'key')['id'],'25');create.assert_not_called()
        with patch.object(b,'_find_it_request',side_effect=[None,{'id':'25'}]),patch.object(b,'_create_item',side_effect=TimeoutError('response lost')):
            self.assertEqual(b._deliver_it_request({},'key')['id'],'25')
    def test_invalid_request_is_not_queued(self):
        with self.assertRaises(ValueError):b.send_it_request('','Operator','5','Software','Issue')
        self.assertEqual(store.list_pending_records(),[])
    def test_legacy_import_is_repeatable_and_keeps_original_file(self):
        path=Path(self.temp.name)/'outbox.jsonl'
        it={'kind':'it_support','queued_at':'2026-11-05T12:00:00Z','payload':{'user_email':'test@example.com','user_name':'Operator','machine_no':'CNC 5','category':'Software','issue':'Help'}}
        original=json.dumps(it)+'\n'+json.dumps({'kind':'supervisor_message','payload':{'comment':'Do not import'}})+'\n'
        path.write_text(original,encoding='utf-8')
        self.assertEqual(import_it_outbox(path)['imported'],1)
        self.assertEqual(import_it_outbox(path)['already_imported'],1)
        self.assertEqual(path.read_text(encoding='utf-8'),original)
        self.assertEqual(len(store.list_pending_records()),1)

if __name__=='__main__':unittest.main()
