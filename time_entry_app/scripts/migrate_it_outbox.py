"""Import saved IT outbox entries into the durable SharePoint delivery queue.
The source file is retained and repeated imports do not duplicate requests.
"""
import hashlib
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from time_entry_app.bridge import cnc_time_backend as backend
store=__import__('local_store')


def import_it_outbox(path=None):
    path=Path(path) if path else backend.OUTBOX_FILE
    result={"imported":0,"already_imported":0,"errors":[]}
    if not path.exists():
        return result
    store.ensure_db()
    for line_no,line in enumerate(path.read_text(encoding="utf-8").splitlines(),1):
        if not line.strip():
            continue
        try:
            record=json.loads(line)
            if record.get("kind")!="it_support":
                continue
            payload=record.get("payload") or {}
            key=hashlib.sha256((json.dumps(record,sort_keys=True)+"|"+str(line_no)).encode()).hexdigest()
            fields=backend._it_request_fields(payload.get("user_email"),payload.get("user_name"),payload.get("machine_no"),payload.get("category"),payload.get("issue"),payload.get("department"),record.get("queued_at"))
            fields["TimeEntryRequestKey"]=key
            conn=store._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                exists=conn.execute("SELECT id FROM time_entries WHERE record_type='it_support_request' AND json_extract(payload,'$.fields.TimeEntryRequestKey')=?",(key,)).fetchone()
                if exists:
                    result["already_imported"]+=1
                else:
                    conn.execute("INSERT INTO time_entries(record_type,machine_no,emp_id,employee_name,payload,created_at,sync_status,source) VALUES ('it_support_request',?,'',?,?,?,'pending','legacy-it-outbox')",(str(payload.get("machine_no") or ""),payload.get("user_name") or "",json.dumps({"fields":fields}),record.get("queued_at") or backend._iso_now()))
                    result["imported"]+=1
                conn.commit()
            finally:
                conn.close()
        except Exception as error:
            result["errors"].append({"line":line_no,"error":str(error)})
    return result


if __name__=="__main__":
    print(json.dumps(import_it_outbox()))
