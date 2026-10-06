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
- New Misc downtime submissions are linked to a running Direct Time entry only for the same employee, work order, and operation. Stopping that entry subtracts the linked pending/approved downtime and existing deductible breaks, with a minimum of zero minutes. Manual and Misc entered totals are unchanged; With Relief breaks retain their existing non-deductible behavior.
- Direct Time stores its downtime deduction at stop time and displays it in admin review. Older unlinked downtime is not retroactively deducted. Changes or rejection of downtime after Direct Time has stopped require reviewing/correcting that Direct Time total too.
- Acumatica downtime uses an entered-minutes-plus-one positive line (quantity 1) and a negative-one-minute line (quantity -1), preserving the entered net downtime. This balancing minute is not deducted from Direct Time or added to the local Misc total.
- Machinists whose cumulative saved working time exceeds 630 minutes (10.5 hours) for a labor date receive a centered review dialog after submitting/correcting time. Exactly 630 minutes is allowed. Direct, Manual, and Misc entries across that labor day's shifts count using their net stored minutes, without adding lunch back or counting Acumatica balancing minutes. Unfinished timers are not part of the saved-entry snapshot.
- The daily review offers editing or confirmation with a required nonblank reason. Pending entries can be edited; approved entries require admin correction. Confirmation stores the operator, labor date, exact entry snapshot, total, reason, and timestamp in `daily_time_reviews`, and displays the reason with pending entries in admin review. Added/corrected entries invalidate the old confirmation. Admin approval (including Approve All) blocks an over-limit machinist day until the current snapshot has been confirmed.
- Approval access is granted from employee department/title text using `CNC_TIME_APPROVER_DEPARTMENT_KEYWORDS`, or explicitly with `CNC_TIME_APPROVER_ADP_NUMBERS`.
- Logistics employees have approval access by default. Restart the host after changing role configuration; employees must sign out and back in to refresh their permissions.
- Admin `Approve All` approves the displayed pending records after confirmation, preserves each reviewer's audit trail, and reports individual failures. `Retry All` retries only failed Acumatica entries eligible after the cutover date, using the existing sync lock and duplicate-send protection.
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

### Existing SharePoint-triggered Teams flows

Set `CNC_TIME_NOTIFICATION_TRANSPORT=sharepoint` in the root `.env` on Server 2
and restart the app to use the existing flows exported on October 6, 2026.
This setting is separate from `CNC_TIME_SHAREPOINT_WRITES_ENABLED`; production
time writes retain their existing configuration.

- Material changes create items in QMS1061 list `7a805b80-4edd-4409-bf0c-0d3b26938999`
  using `Title`, `Machinist`, `MachineNumber`, `WO`, and `MaterialType`. The exported
  flow posts to Teams group chat `19:672904bead9a495ab6f0fd9bf3a0aeed@thread.v2`
  and checks for new items every minute.
- Mold approvals create items in QMS1061 list `c7c723b1-123d-4a1b-b8a4-6e1b8b2a865e`
  using `Title`, `MachinistName`, `MachineNumber`, and `WONumber`. The exported flow
  sends an approval card to `EnnisQC@benoit-inc.com` and checks every five minutes.
  It writes `ApprovalStatus` (`Approved` or `Not Approved`), `QCResponder`, and
  `ResponseDateTime` back to the item. While the operator watches the request,
  the app reads its decision and displays Approved to Run or Do Not Run.
- Assistance uses the QMS1061 list configured in `CNC_TIME_ASSISTANCE_SHAREPOINT_LIST_ID`
  when SharePoint transport is selected. Until that list ID is configured, it
  uses `CNC_TIME_ASSISTANCE_WEBHOOK_URL`, or the local outbox when the URL is blank.

Server 2 only needs outbound access to Microsoft Graph for this route; no public
callback URL is needed. Its existing `CNC_TIME_SHAREPOINT` credentials must allow
creation and reading of items in both lists, and the two flows must be enabled.
Confirm the list column types accept the text values above before enabling.
The exports describe flow configuration, not whether the live flows are enabled.

The app marks an accepted list write as Submitted to flow, not confirmed Teams
delivery. Requests saved before activation are not automatically replayed.
Failed submissions remain saved locally and require investigation before a new
submission; an ambiguous network failure may have already created the remote item.
The existing cards do not include the optional additional message field.

### Assistance Resolution

`config/assistance.json` records the nine assistance recipients and the shared
chat name, Ennis Assistance ASAP. Each request goes to the same group chat;
the operator does not select a recipient. Create the chat and its supporting
SharePoint list/flow using `power_automate/assistance-setup.txt` and the supplied
`power_automate/assistance-card.json`. These are setup assets, not a deployed flow.

The card includes the operator, station, work order, and assistance description,
with a Mark resolved action and optional resolution note. The flow writes
`RequestStatus=Resolved`, `ResolvedBy`, `ResolutionNote`, and `ResolvedAt` back to
the list. The app polls watched assistance requests alongside mold approvals,
records the first resolution, and displays the responder, note, and timestamp.
Polling failures leave the request open. No inbound callback is needed.

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
The web-app installer records the resolved Node.js executable in its task
action so startup does not depend on the SYSTEM account's PATH. Pass
`-NodePath` to the installer to select a specific executable. After changing
the Node.js install location, reinstall the task. The launcher logs early
startup failures to `data/cnc-time-host.log`.

If Python packages were originally installed only for a user account, install
them for the host service account from an elevated prompt:

```powershell
.\scripts\install_host_python_dependencies.ps1
```
