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

1. Install Node.js if needed.
2. From this folder, install dependencies:

```powershell
npm install
```

3. Start the standalone app:

```powershell
npm start
```

4. Open:

```text
http://localhost:3100
```

The `start` and `dev` commands load the ignored root `.env` file. Set a unique
`SESSION_SECRET` there before running the app on a shared computer.

## Linkage to the inspection app

This app is intentionally separate, but it still shares the same project environment, database, and business context as the inspection app. That means you can keep the two experiences independent while still linking them by employee, machine, work order, or later record IDs when needed.

## Notes

- The app uses the same project environment and backend data sources as the inspection workflow.
- Submitted production, manual, and misc labor records are held in a local approval queue first. Users with the `approver` role can review them at `/admin`; approval releases the stored payload into the existing downstream SharePoint/Acumatica queue, while rejection keeps it out of downstream systems.
- Approval access is granted from employee department/title text using `CNC_TIME_APPROVER_DEPARTMENT_KEYWORDS`, or explicitly with `CNC_TIME_APPROVER_ADP_NUMBERS`.
- Operator time-entry access is available to every active Ennis employee with an ADP number. Export and approval access remain controlled separately.
- IT employees matching `CNC_TIME_FULL_ACCESS_DEPARTMENT_KEYWORDS` receive operator, export, and approval permissions. Specific employees can also be granted full access with `CNC_TIME_FULL_ACCESS_ADP_NUMBERS`.
- If `CNC_TIME_IT_WEBHOOK_URL` or `CNC_TIME_SUPERVISOR_WEBHOOK_URL` are not set, support requests are queued locally in `data/cnc_time_outbox.jsonl`.

## Employee notifications

Every signed-in employee has access to `/notifications` for Mold Approval, Change Material, and Assistance Needed ASAP requests. Requests are stored in the local SQLite database before delivery, so delivery failures do not erase the request.

- `CNC_TIME_QUALITY_APPROVAL_WEBHOOK_URL` receives mold approval requests. Its Power Automate flow should post the Quality response to the supplied `decision_url`, including the supplied `decision_token`, a `decision` value of `approved` or `rejected`, and the responder name.
- `CNC_TIME_CHANGE_MATERIAL_WEBHOOK_URL` receives change-material notifications.
- `CNC_TIME_ASSISTANCE_WEBHOOK_URL` receives urgent assistance requests.
- `CNC_TIME_NOTIFICATION_CALLBACK_BASE_URL` must be an HTTPS address reachable by Power Automate. Leave it blank until the host has a secure externally reachable callback or an on-premises integration path.

When a webhook is blank, the request remains visible in the app and its outbound payload is also appended to `data/cnc_time_outbox.jsonl` for later integration.

## Acumatica labor sync

Approved CNC entries are grouped by labor date and area (`L1`, `L2`, or `T&B`) and sent to the configured Acumatica `Labor` endpoint as on-hold batches. The integration is disabled unless `ACUMATICA_ENABLED=true` is set in the ignored root `.env` file. `ACUMATICA_CUTOVER_AT` must also contain the UTC go-live timestamp; approvals reviewed before that timestamp are never selected.

Run the same idempotent sync used by the approval-page button:

```powershell
npm run sync:acumatica
```

On the always-on Windows server, install the daily 6:00 AM local-time task from an elevated PowerShell prompt:

```powershell
.\scripts\install_acumatica_task.ps1
```

Keep the server time zone set to Central Time. Task history and Acumatica send results are also retained in the local SQLite database.

Install the web app as an automatic startup task from the same elevated prompt:

```powershell
.\scripts\install_cnc_time_app_task.ps1
```

To install both host tasks together, run:

```powershell
.\scripts\install_host_tasks.ps1
```

To restart the live app under its installed task, run from an elevated prompt:

```powershell
.\scripts\start_cnc_time_app_task.ps1
```

Both tasks run as the local `SYSTEM` account, do not require a user to remain
signed in, and restart automatically after transient failures. The combined
installer also allows inbound TCP port `3100` on Domain and Private networks.

If Python packages were originally installed only for a user account, install
them for the host service account from an elevated prompt:

```powershell
.\scripts\install_host_python_dependencies.ps1
```
