# Benoit Time Entry App

This workspace is dedicated to the CNC time-entry experience and is intentionally separate from the inspection workflow app.

## Purpose

- Run the machine time-entry, daily checklist, maintenance, and support flows.
- Use the same SharePoint and backend data sources as the rest of the project.
- Keep the app isolated from the inspection-report workflow while preserving the ability to link records later.

## Architecture

- `src/cncTimeServer.js` — the standalone Express app for the time-entry workflow
- `src/cncTimeBridge.js` — Node-to-Python bridge helper
- `bridge/cnc_time_bridge.py` — Python entry point for the CNC time app
- `bridge/cnc_time_backend.py` — SharePoint / employee / time logic
- `public/` — app assets and PWA resources

## Run the app

1. Install Node.js 22 or newer, Python 3, and PostgreSQL. This computer has Node.js 24, Python 3.13, and a running PostgreSQL 18 service.
2. Keep this folder inside the project: the Python backend also needs the root `config.py`, `postgres_sync.py`, `sharepoint_api.py`, and `sharepoint_client.py`. From this folder, install dependencies:

```powershell
python -m pip install -r requirements.txt
npm.cmd ci
```

3. Configure the root `.env` file using `.env.example` as a reference. Both Node and Python load this file; environment variables already set in the terminal take precedence. PostgreSQL connection settings must point to the database containing the cached SharePoint lists.

4. Start the standalone app:

```powershell
npm.cmd start
```

Alternatively, double-click `start-time-entry.cmd` in the project root. It uses `npm.cmd`, which works with this computer's PowerShell execution policy. Keep its terminal open while using the app. For development, use `npm.cmd run dev`.

5. Open:

```text
http://localhost:3100
```

The local approval/session database is created automatically at `time_entry_app/data/cnc_time_local.db`. Back up this file to preserve local entries, approval history, and Acumatica send history. The PostgreSQL cache is separate and must also be preserved; copying source files alone does not copy its records.

Set `CNC_TIME_SYNC_ENABLED=false` in the terminal or root `.env` to disable startup/hourly SharePoint synchronization. This still allows local use of cached reference data. LiveView charts require access to the configured LiveView server. Support delivery requires the configured webhooks, and external synchronization requires working Microsoft/Acumatica authentication.

## Review and local verification (October 5, 2026)

- Reinstalled the incomplete Node dependencies: Express was missing its `lib/express` module and prevented startup.
- Added root `.env` loading to the Node server and replaced its hardcoded default session signing key with a random startup key.
- Verified PostgreSQL connectivity and cached time-app reference lists, including 787 employees and 3,552 production-operation records; initialized the local SQLite schema.
- All 25 existing Python time-app tests passed. Local HTTP startup and asset checks use disabled background synchronization so verification does not send business records to external systems.
- Review findings still requiring application hardening: sign-in identifies people by ADP number without a password; entry edit/delete/pause/stop routes accept an entry ID without passing the signed-in employee to the backend for an ownership check; the service worker caches authenticated GET responses; Express sessions use memory and are lost on restart. These findings do not prevent local startup.

## Linkage to the inspection app

This app is intentionally separate, but it still shares the same project environment, database, and business context as the inspection app. That means you can keep the two experiences independent while still linking them by employee, machine, work order, or later record IDs when needed.

## Notes

- The app uses the same project environment and backend data sources as the inspection workflow.
- Submitted production, manual, and misc labor records are held in a local approval queue first. Users with the `approver` role can review them at `/admin`; approval releases the stored payload into the existing downstream SharePoint/Acumatica queue, while rejection keeps it out of downstream systems.
- Approval access is granted from employee department/title text using `CNC_TIME_APPROVER_DEPARTMENT_KEYWORDS`, or explicitly with `CNC_TIME_APPROVER_ADP_NUMBERS`.
- If `CNC_TIME_IT_WEBHOOK_URL` or `CNC_TIME_SUPERVISOR_WEBHOOK_URL` are not set, support requests are queued locally in `data/cnc_time_outbox.jsonl`.

## Acumatica labor sync

Approved CNC entries are grouped by labor date and area (`L1`, `L2`, or `T&B`) and sent to the configured Acumatica `Labor` endpoint as on-hold batches. The integration is disabled unless `ACUMATICA_ENABLED=true` is set in the ignored root `.env` file. `ACUMATICA_CUTOVER_AT` must also contain the UTC go-live timestamp; approvals reviewed before that timestamp are never selected.

Run the same idempotent sync used by the approval-page button:

```powershell
npm.cmd run sync:acumatica
```

On the always-on Windows server, install the daily 6:00 AM local-time task from an elevated PowerShell prompt:

```powershell
.\scripts\install_acumatica_task.ps1
```

Keep the server time zone set to Central Time. Task history and Acumatica send results are also retained in the local SQLite database.
