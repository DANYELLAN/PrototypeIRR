import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from time_entry_app import local_store as store

class AcumaticaTenantIsolationTests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
        for target,key,value in [(store,'DB_PATH',Path(temp.name)/'test.db'),(store,'LOCAL_MACHINE_CONFIG',Path(temp.name)/'machines.json')]:
            p=patch.object(target,key,value);p.start();self.addCleanup(p.stop)
        p=patch.dict(os.environ,{'ACUMATICA_TENANT':'Test'});p.start();self.addCleanup(p.stop)
    def test_batch_numbers_and_receipts_are_independent_between_tenants(self):
        store.save_acumatica_batch('2026-11-05','L1','000123','Test batch')
        store.upsert_acumatica_sync_item(11,1,'sent',batch_nbr='000123',line_nbrs=[1])
        with patch.dict(os.environ,{'ACUMATICA_TENANT':'Benoit'}):
            self.assertIsNone(store.find_latest_acumatica_batch('2026-11-05','L1'))
            self.assertIsNone(store.get_acumatica_sync_item(11))
            store.save_acumatica_batch('2026-11-05','L1','000123','Benoit batch')
            store.upsert_acumatica_sync_item(11,1,'failed',batch_nbr='000123',error='Benoit error')
            self.assertEqual(store.find_latest_acumatica_batch('2026-11-05','L1')['description'],'Benoit batch')
        self.assertEqual(store.find_latest_acumatica_batch('2026-11-05','L1')['description'],'Test batch')
        self.assertEqual(store.get_acumatica_sync_item(11)['status'],'sent')
    def test_run_locks_and_status_summaries_belong_to_the_selected_tenant(self):
        test_run=store.begin_acumatica_sync_run('test')
        store.upsert_acumatica_sync_item(11,1,'sent')
        with patch.dict(os.environ,{'ACUMATICA_TENANT':'Benoit'}):
            self.assertIsNotNone(store.begin_acumatica_sync_run('production'))
            self.assertIsNone(store.begin_acumatica_sync_run('duplicate'))
            self.assertNotIn('sent',store.get_acumatica_sync_summary()['counts'])
        self.assertIsNone(store.begin_acumatica_sync_run('duplicate-test'))
        self.assertEqual(store.get_acumatica_sync_summary()['counts']['sent'],1)
    def test_case_changes_do_not_create_a_different_tenant_history(self):
        with patch.dict(os.environ,{'ACUMATICA_TENANT':'Benoit'}):store.upsert_acumatica_sync_item(11,1,'sent')
        with patch.dict(os.environ,{'ACUMATICA_TENANT':' BENOIT '}):self.assertEqual(store.get_acumatica_sync_item(11)['status'],'sent')
    def test_cutover_excludes_approvals_from_before_the_switch(self):
        store.ensure_db();conn=store._connect()
        for date in ['2026-11-04T12:00:00Z','2026-11-05T12:00:00Z']:
            conn.execute("INSERT INTO approval_queue(record_type,payload,approval_status,submitted_at,reviewed_at) VALUES ('manual_time',?,'approved',?,?)",(json.dumps({'fields':{}}),date,date))
        conn.commit();conn.close()
        with patch.dict(os.environ,{'ACUMATICA_TENANT':'Benoit'}):
            rows=store.list_acumatica_candidates(reviewed_after='2026-11-05T00:00:00Z')
            self.assertEqual(len(rows),1);self.assertEqual(rows[0]['reviewed_at'],'2026-11-05T12:00:00Z')

if __name__=='__main__':unittest.main()
