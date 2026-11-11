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
# Button diagnostics

IT users can open **Button Diagnostics** in the navigation (or `/admin/click-diagnostics`). It records pointer presses and clicks across the app, browser validation failures, form submissions/cancellations, server receipt, server response with duration and notice outcome, page loads, and script-error occurrence. Filter by button label, station, employee ID, interaction ID, or browser-tab ID. The same interaction ID connects a button press to its form request. Keyboard activation is included. No input values, form bodies, credentials, or error messages are collected.

Use `pointerdown` followed by `click` to confirm the browser saw a press. `invalid` means browser validation blocked submission. `submit_cancelled` means confirmation or app validation cancelled it. `server_received` confirms the request reached the app; `server_response` shows elapsed milliseconds and the app's notice kind (`warning` can accompany HTTP 302). A response does not by itself prove the next page displayed; `page_loaded` from the same browser tab helps confirm navigation. Pointer events on non-button elements help identify clicks landing on an overlay. Disabled controls may produce only a press, and some browsers suppress even that.

Logs start after restarting the updated app and reloading browser clients; they cannot reconstruct earlier clicks. Browser events queue in session storage (up to 200) and retry every five seconds while the tab stays open. Missing events are inconclusive when JavaScript/storage/network fails or a tab closes. Client timestamps and server receipt timestamps can differ; correlate IDs rather than arrival order. Server logs rotate at roughly 5 MiB, keeping four archives (roughly 25 MiB total), under `time_entry_app/data/diagnostics/`; the view reads the latest 1,000 records and shows up to 500 matches. Files are local to this server and excluded from Git. Access to the viewer requires the `it` role, assigned by the existing IT/full-access department and ADP rules (`CNC_TIME_FULL_ACCESS_DEPARTMENT_KEYWORDS` and `CNC_TIME_FULL_ACCESS_ADP_NUMBERS`). Approvers without that role cannot view it, including by direct URL. Sign out and back in after restarting to refresh roles. Logging failures do not block time entry.


## Mold approval moved to inspection

Create new mold approval requests in the inspection app’s **Mold Approval** tab. Time Entry retains Change Material and Assistance ASAP, existing notification history/status polling, and the token-protected mold decision callback for outstanding requests. Its notification store and backend are shared with inspection so existing Quality/Teams/SharePoint configuration continues to work.


## Offline operation in the browser

App installation is optional and is not offered by the current UI. Use trusted HTTPS,
sign in, and open Time Entry once while connected. `/time` opens Review / Corrections
and prepares available WO operations and detail options for offline use. If the server
cannot be reached, the prepared browser worksheet opens as the offline fallback. Its status strip distinguishes
HTTPS setup, preparation, connection loss, and the number of actions waiting to upload.

The tablet stores drafts, timer starts, lunch/break transitions, manual time, and misc
time in IndexedDB. Timer timestamps are captured when the operator acts, not when an
upload eventually succeeds. With Relief breaks remain productive; Lunch and No Relief
breaks are deducted. Downtime for the active operation is deducted from its timer.
Closing/reopening the prepared page retains the timer and queued actions.

Uploads run in order while the page is open, including after reconnect and foreground
resume. Every event has a UUID. `tablet_time_events`, `tablet_time_timers`, and insertion
into `approval_queue` commit together in the local SQLite database. Retrying a request
after a lost response returns the saved receipt. Employee, machine and shift must match
the authenticated session; new sign-ins need connectivity. Missing WOs or other server
conflicts keep the queued data for review rather than silently changing it.

Time from tablet timers and manual/misc submissions enters the existing supervisor
approval queue; Acumatica export and daily over-limit confirmation remain server-side.
Existing timers started in the original UI must be stopped there before starting a new
tablet timer. Review/corrections remain available at `/time?traditional=1` while online.
Checklist, support, maintenance and approvals require connectivity. Recovery downloads
are restricted to the currently selected local worksheet, not other employees' profiles.

On Server2-PC, trusted HTTPS is on port 3443. The CA's public download is
`http://10.100.100.224:3100/tablet-ca.crt`; private keys stay under the secured
`C:\ProgramData\Benoit\Apps\tls` folder. The background task `Benoit Time Entry Server`
starts after reboot and runs the protected `Run-BenoitApp.ps1` watchdog. Sessions may
require sign-in after a server restart; queued tablet data remains on the tablet.
Reserve Server2-PC's Wi-Fi address in the router so bookmarks and certificate SANs
remain valid. Renew the server certificate before its expiry, keeping the same CA.

The worker at `/cnc-time-sw.js` has root scope and caches only static assets. Login,
administration, API responses and personalized server pages are not placed in its cache.
Cookies are named `benoit.time.sid` to avoid collisions with Inspection on the same host.
Use the same HTTPS address consistently; HTTP and HTTPS store separate local work.


### Navigation and active timer controls

The tablet worksheet retains the normal Machine Overview, Time Entry, Daily Checklist,
Notifications, Maintenance and IT Support tabs while a timer is running or paused.
Role-specific tabs come from the same navigation definition as the traditional screen;
their server permissions remain enforced. Leaving and returning to Time Entry does not
stop its saved timer. The Active Entry card uses the regular app's status and action
styling. Log Downtime is available alongside Take Break, Lunch, Resume Production and
Stop And Submit. Active downtime is pinned to the current timer/WO/operation, queues
through the existing offline event flow and deducts from production time once. A stale
worksheet cannot attach downtime to a replacement timer. Refresh connected clients
after restarting the app so offline asset release v15 can install; keep tablet storage
intact to preserve unsynced work.


### Relief identity and handoffs

**Take Break → With Relief → Start Break** prompts the relief operator for an ADP
number before coverage begins. Connected tablets validate the ADP immediately; offline
actions preserve the entered ADP for server validation on sync. The server requires an
active Ennis employee different from the original operator and resolves the name itself.
Production keeps accruing during relieved coverage. **End Relieved Break** records the
original operator returning and closes coverage without deducting its minutes.

During a **No Relief** break, **Relief Arrived** prompts for the replacement ADP and
splits the break at the arrival timestamp. Only the preceding uncovered interval is
deducted. Both intervals, relief identity, and original operator return are retained in
the tablet timer/approval audit; the traditional screen keeps the same identity and
linked intervals in `break_events`. A conversion and its traditional sync record commit
together. Refresh connected tablets after restarting to install offline asset release v16.


### Default Time Entry view

`/time` opens the full Review / Corrections layout while connected. If the server
cannot be reached, the service worker opens the cached tablet worksheet so prepared
devices can continue recording offline. The worksheet returns to `/time` when the
server connection is restored. Existing offline timers and queued actions remain intact;
the normal page embeds their existing timer/sync controls so they can finish without
switching the whole page to another layout. Other sections are unchanged. Offline
asset release v18 restores the offline worksheet fallback.


### Maintenance requests to SharePoint

`CNC_TIME_MAINTENANCE_SHAREPOINT_WRITES_ENABLED=true` enables maintenance delivery to
Benoit Maintenance / Ennis Maintenance Request without enabling other queued writes.
The global `CNC_TIME_SHAREPOINT_WRITES_ENABLED` setting remains independent.
Requests are queued locally before sending and background sync retries pending
maintenance requests on startup and at the configured sync interval (one hour by default).
The SharePoint list uses an indexed unique text column, `TimeEntryRequestKey`, so
retries can recover an existing item instead of creating duplicates. The submission
page reports either sent to SharePoint with its item number, or saved locally and
waiting to send. SharePoint currently limits the title and description to 255 characters.


### IT requests to SharePoint Help Desk

IT Request submissions now target Benoit IT / IT Support Help Desk, replacing the
legacy IT webhook/outbox path. `CNC_TIME_IT_SHAREPOINT_WRITES_ENABLED=true` enables
this list independently of maintenance and the global SharePoint write setting.
The form shows a read-only User Name from the ADP-authenticated employee; no
station email is requested or required. Requester name, department when available, category, full issue description,
and machine number are retained. New tickets use Status New and Location Ennis;
Date of Issue uses Central time. Priority is left for IT to assign.
Legacy queued requests retain any existing requester email; new form requests
omit RequesterEmail rather than substituting a station address or a name.

The local database retains requests before delivery. The existing background sync
retries pending support requests, and the list's unique indexed `TimeEntryRequestKey`
column prevents duplicate tickets when a response is lost. Confirmation distinguishes
sent-to-SharePoint from saved-and-waiting. The legacy outbox can be imported with
`python time_entry_app/scripts/migrate_it_outbox.py`; imports are repeatable, preserve
the source file, and ignore supervisor messages.


### Full admin access by department or title

`CNC_TIME_FULL_ACCESS_DEPARTMENTS` and `CNC_TIME_FULL_ACCESS_JOB_TITLES` accept
comma-separated exact matches, ignoring case and extra whitespace. Titles match
either the normalized position title or ADPJobTitleDesc. Matching either department
or title grants operator, exporter, approver and IT roles; the active Ennis employee
requirement still applies. Existing explicit ADP access and IT keyword rules remain.
Users must sign out and back in to refresh their session roles.

The full-access keyword configuration also includes `integration technologist`, so
all title levels and location variants containing that phrase receive the same
access. Active Ennis eligibility remains required.


Button Diagnostics includes station tabs for every configured machine/station, plus
observed stations and an Unassigned tab when pre-selection events exist. All Stations
shows the combined view. Tab counts reflect date and text filters across all retained log files; station, date and
text filters apply before selecting the newest 500 matching events. Tabs preserve the
text filter and the selected station remains selected when refreshing. IT access is
required for every station view.

Button Diagnostics supports inclusive From/To dates in America/Chicago (Central Time).
Set both dates to the same day for a single-date search. Either boundary can be left
blank. Station tabs preserve all filters, Clear Dates preserves station and text,
and reversed or invalid ranges display a validation message. Receipt times are shown
in Central Time; available log dates are displayed. The current log and all four
retained rotation files are searched before applying the 500-row display limit.

Lunch reminders follow the signed-in shift in both connected and offline Time Entry:
Day Shift (40) at 11:30 AM, Night Shift (41) at 10:00 PM, in America/Chicago.
The same styled Yes/No dialog starts the existing lunch action or dismisses the
reminder. Daily dismissal remains scoped to the operator, station and shift.


Acumatica tenant cutover: the Benoit tenant uses the Labor / 25.100.001 endpoint.
Its LaborEntry Details must include Machine, DetailType and LaborComments, with the
same mappings as Test. The production cutover timestamp excludes approvals from
before the switch. Acumatica batch IDs, delivery receipts, status summaries and run
locks are isolated by tenant: legacy unprefixed tables remain Test history; other
tenants use separately prefixed tables in the same database. Existing Test receipts
cannot be used to update a Benoit batch with the same number. No old approvals are
replayed automatically across the cutover.

Acumatica eligibility is based on the station, not the employee Machinist flag.
Only CNC machines 1-6 (using the existing machine-to-area mapping) receive Acumatica
labor payloads. The same rule applies to timer, manual, downtime and approval edits.
Approval status and the Benoit cutover timestamp remain required for sending.
# Timed Downtime

For an active entry, Log Downtime now collects a reason and optional comments,
then Start Downtime saves the start timestamp. A persistent modal shows elapsed
downtime until End Downtime & Resume Production is pressed. Ending queues one
Misc Time record for the existing supervisor approval flow and excludes that
interval from production once. Separate historical/manual Misc Time entry is
unchanged. Breaks, active-entry corrections and stopping an entry are blocked
until its downtime ends.

The regular screen stores sessions in SQLite (`timed_downtime_sessions`), and
the tablet screen stores start/end events in its existing offline queue. Both
survive reloads. Duplicate end requests cannot create a second approval. Duration
is measured from timestamps, retained in seconds, and uses the app's existing
whole-minute accounting for labor totals (sub-minute intervals are not rounded
up). Offline asset release v28 includes the popup module; refresh connected
tablets to install it. Never clear tablet storage while work is waiting to sync.

# End Set-Up Handoff

Running entries with detail Set-Up replace Stop And Submit with End Set-Up in
the quantity row. The entered quantity is submitted on the Set-Up record and is
not carried into the new entry. End Set-Up opens a
confirmation dialog defaulted to Machining, with all normal detail choices
available. Cancel leaves Set-Up running. Confirm submits Set-Up for the normal
supervisor approval flow and starts a new entry for the same WO, operation,
description, operator, station and shift, at one shared boundary timestamp.
Existing breaks and downtime are deducted from Set-Up and not copied to the
new entry. End any active break or downtime before using the handoff.

The regular screen commits the approval, old-entry completion and new local
start in one SQLite transaction, with a `setup_transitions` receipt preventing
duplicates after retries. Its completion projection keeps the old Set-Up closed
even when SharePoint updates are pending. The tablet path uses one atomic
`end_setup` event in the existing offline queue. Refresh connected tablets to
install offline asset release v30; do not clear storage while work is pending.

# Detail-Specific Entry Completion

All active CNC-style details, including Machining, Turn & Bore, Recut, Rebore
and Change Over, use End Entry and a quantity popup with Confirm And Submit.
Set-Up remains the exception: its inline quantity and End Set-Up/detail-type
handoff are unchanged. Completion behavior follows the entry's detail type,
not the employee's Machinist onboarding flag or the station's display name.

Inspection, Sandblast, Drift and Stenciling use Enter Starting Pipe (or Change
Starting Pipe after saving), then End Entry asks for the ending pipe. Quantity
is calculated on the backend as `ending - starting + 1`, including both pipes.
The starting pipe is tied to the entry/WO/operation and survives refreshes;
editing the job invalidates the old starting selection. Empty, non-integer,
reversed and excessive ranges are rejected before stopping the entry. The
saved approval includes both pipe numbers for traceability.

Tablet start/stop events retain pipe selections in the existing offline queue.
Pre-update offline stop events that already contain a manual quantity still
sync and are marked `legacy_pipe_quantity` in their approval payload. New pipe
completion events always calculate from the saved start and supplied end.
Offline asset release v31 includes these controls. Refresh while connected;
never clear tablet storage while work is pending.

# Comments In Recent Entries

The Comments editor starts blank for entries whose legacy transaction description
was automatically copied from the operation/part description. Existing genuine
notes remain visible; no historical records are bulk-cleared. New Set-Up handoffs
and manual/tablet entries no longer copy operation descriptions into Comments.

Explicit edits, including clearing a comment or saving text identical to the
operation description, are tracked in the local `entry_comment_overrides` table.
An active entry's Recent Entries editor saves to the active Start/Stop record;
submitted entries continue to use the existing correction flow. Saved comments
carry into the completed approval, while a new entry begins with blank comments.

# Active Time Corrections And Missed Lunch

Edit Time on an active entry offers a Central Time start correction and Log Lunch
(1 Hour). These adjust the existing running entry without creating another entry
or approval. Start corrections retain the job and saved starting pipe. Missed lunch
records the scheduled hour (11:30–12:30 day shift, 22:00–23:00 night shift), rejects
overlapping deductions and prevents duplicate lunch deductions. The full scheduled
hour must have elapsed within the entry. During an active day-shift lunch reminder,
Log Lunch becomes available at 12:30 Central and keeps production running.

Tablet adjustments persist in the offline event queue and replay with the original
timer ID. Refresh connected tablets to install offline asset release v33; never
clear storage while events are pending.

Start corrections use a time-only input and preserve the existing Central date
on the backend and during offline replay. Manual Time and Manual Downtime
(previously Misc Time) are collapsed initially on connected and offline screens.
The loading screen covers initial preparation and normal navigation or form
submission, clears when ready or returning with Back, and leaves cancelled
actions, downloads and offline form handling alone.

Recent Entries opens Edit in a responsive modal with the existing correction
fields and POST action. Cancel or Escape discards unsaved edits. The two-column
tablet form becomes one column on phones, scrolls vertically within the viewport,
and does not expand the recent-entry table. Offline asset release v34 includes
the shared style and script update.

Start-time correction uses a numeric keyboard instead of the native clock picker.
Enter a familiar time such as 800 or 8:00 and select AM or PM. The existing time
and period are preselected, and the Central date remains locked. Older queued
24-hour corrections remain supported during offline replay. Offline asset release
v36 includes this input change.

# Acumatica Labor Before Downtime

Exports process non-downtime labor before downtime within each labor-date/area
batch. Downtime is sent only after the remote batch confirms a non-downtime line
for the same work order, operation, CNC machine, employee and shift. If labor is
unapproved, missing or failed, downtime remains pending with a waiting explanation
and is reconsidered by the next normal manual or scheduled sync. Targeted retries
also enforce this prerequisite without sending other approvals automatically.

Released batches are not modified. A supplemental batch needs matching production
labor before receiving downtime. Existing downtime line markers and paired-line
calculations retain their duplicate protection and durations.

Only stations mapped to CNC 1–6 are eligible for Acumatica labor export; eligibility
does not depend on an employee's Machinist flag. The sender also rejects stale
payloads whose machine conflicts with the entry station or contains non-CNC or
mixed-machine transactions. Inspection, Sandblast, Drift and Stenciling stations
remain excluded. The Python bridge and scheduled sync load these rules on their
next invocation; a web server restart is unnecessary.

# Tablet Network Load

Dynamic HTML and JSON responses over 1 KB use gzip when the browser accepts it,
while retaining the same decoded content. The offline service worker continues
to cache static app files; compression does not cache entry data or delay saves.

The connected Time Entry page now supplies its freshly fetched tablet preparation
data as escaped, inert JSON. Offline preparation reuses this snapshot instead of
making a second context API request and repeating the backend reads. Older pages
without a snapshot retain the context API fallback. Pending offline events and
drafts remain authoritative and are preserved.

Offline release v37 includes the client update. A representative rendered fixture
with 25 recent entries and 200 operations decreased from 673,728 to 22,840 transfer
bytes (97%) under gzip. This measures payload size, not factory Wi-Fi speed or a
full tablet load time; the actual gain depends on page data and signal quality.

# Time Page Read Performance

The Time Entry page and tablet context API fetch dashboard data, active tablet
timer and daily review in one Python bridge invocation instead of three. The same
read functions run in the same order, using the existing within-process list cache.
No cross-request data cache, rendered layout or entry workflow was changed by this
optimization. A fixture comparison checks byte-identical HTML, and an isolated
launch benchmark measured median startup/read coordination of 0.38 seconds versus
1.18 seconds before batching. This is not a full-page load measurement.

