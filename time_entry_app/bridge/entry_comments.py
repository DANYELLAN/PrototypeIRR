"""Keep explicitly saved comments separate from legacy description fallbacks."""
store = __import__('local_store')


def _db():
    store.ensure_db()
    connection = store._connect()
    connection.execute('CREATE TABLE IF NOT EXISTS entry_comment_overrides (entry_list TEXT NOT NULL, entry_id TEXT NOT NULL, comment TEXT NOT NULL, PRIMARY KEY(entry_list,entry_id))')
    return connection


def remember_comment(entry, comment):
    if comment is None:
        return
    connection = _db()
    try:
        connection.execute('INSERT OR REPLACE INTO entry_comment_overrides VALUES(?,?,?)',
                           (entry.get('entry_list') or 'timeentry', str(entry['sp_id']), str(comment).strip()))
        connection.commit()
    finally:
        connection.close()


def display_comment(entry):
    raw = str(entry.get('tran_description') or '').strip()
    if raw in {str(entry.get('operation_description') or '').strip(), str(entry.get('description') or '').strip()}:
        return ''
    reason = str(entry.get('details_type_ii') or '').strip()
    if reason and raw == reason:
        return ''
    if reason and raw.startswith(reason + ' - '):
        return raw[len(reason) + 3:]
    return raw


def apply_comments(entries):
    connection = _db()
    try:
        explicit = {(row['entry_list'], row['entry_id']): row['comment'] for row in connection.execute('SELECT * FROM entry_comment_overrides')}
    finally:
        connection.close()
    for entry in entries:
        key = (entry.get('entry_list') or 'timeentry', str(entry['sp_id']))
        entry['user_comments'] = explicit.get(key, display_comment(entry))
    return entries


def saved_comment(entry):
    return apply_comments([dict(entry)])[0]['user_comments']
