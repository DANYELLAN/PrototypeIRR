import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from time_entry_app.bridge import entry_comments as comments
from time_entry_app.bridge import cnc_time_backend as backend


class EntryCommentTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        for name,value in [('DB_PATH',Path(self.temp.name)/'time.db'),('LOCAL_MACHINE_CONFIG',Path(self.temp.name)/'machines.json')]:
            p=patch.object(comments.store,name,value);p.start();self.addCleanup(p.stop)
        self.entry={'sp_id':'1','entry_list':'startstop','tran_description':'THREAD BOX END CONNECTION','operation_description':'THREAD BOX END CONNECTION','status':'In Progress'}

    def test_legacy_automatic_description_is_hidden_without_altering_record(self):
        self.assertEqual(comments.saved_comment(self.entry),'')
        self.assertEqual(self.entry['tran_description'],'THREAD BOX END CONNECTION')
        self.assertEqual(backend._time_entry_comment('Machining',comments='',fallback='Thread BOX'),'')

    def test_explicit_comment_is_kept_even_if_identical_to_description_and_can_be_cleared(self):
        comments.remember_comment(self.entry,'THREAD BOX END CONNECTION')
        self.assertEqual(comments.saved_comment(self.entry),'THREAD BOX END CONNECTION')
        comments.remember_comment(self.entry,'')
        self.assertEqual(comments.saved_comment(self.entry),'')
        self.assertEqual(comments.saved_comment({**self.entry,'entry_list':'timeentry','tran_description':'Actual note'}),'Actual note')

    def test_downtime_reason_is_not_mistaken_for_an_entered_comment(self):
        self.assertEqual(comments.display_comment({'tran_description':'M1','details_type_ii':'M1'}),'')
        self.assertEqual(comments.display_comment({'tran_description':'M1 - Chuck repaired','details_type_ii':'M1'}),'Chuck repaired')

    def test_active_edit_saves_and_clears_the_comment_in_durable_record(self):
        record=comments.store.queue_record('startstop',{'fields':{'Status':'In Progress','TranDescription':'THREAD BOX END CONNECTION'}})
        entry={**self.entry,'sp_id':f'local:{record}'}
        with patch.object(backend,'_get_startstop_by_id',return_value=entry):
            backend.edit_active_time_entry(entry['sp_id'],comments='Operator note')
            self.assertEqual(comments.saved_comment(entry),'Operator note')
            backend.edit_active_time_entry(entry['sp_id'],comments='')
            self.assertEqual(comments.saved_comment(entry),'')
        queued=next(row for row in comments.store.list_pending_records() if row['id']==record)
        self.assertEqual(json.loads(queued['payload'])['fields']['TranDescription'],'')
