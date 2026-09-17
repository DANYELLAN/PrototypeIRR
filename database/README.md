# BenoitIRR database migration

The `benoitirr_logical_*` folder contains a database-specific export of all 14 application tables. It intentionally excludes other PostgreSQL databases and global cluster data.

On the new server:

1. Install PostgreSQL 18, Node.js, and Python 3.14.
2. Confirm the PostgreSQL settings in the app root `.env` file.
3. Open PowerShell in the Inspection app folder.
4. Run:

```powershell
powershell -ExecutionPolicy Bypass -File .\database\restore_benoitirr.ps1
```

The script selects the newest `benoitirr_logical_*` folder automatically. It creates the database when missing, initializes the current app schemas, imports tables in dependency order, resets identity sequences, and verifies every table count.

If the target database already contains application tables and is meant to be replaced, run:

```powershell
powershell -ExecutionPolicy Bypass -File .\database\restore_benoitirr.ps1 -ReplaceExisting
```

`-ReplaceExisting` deletes existing application-table rows before importing the backup. Do not use it against a database that must be preserved.
