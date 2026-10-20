# AutoIRR Node App

## Live Pipe Report and verification

**Live Pipe Report** appears in navigation for Director of Quality, Quality Control Specialist, Quality Manager, Quality Supervisor (exact `ADPJobTitleDesc` matches), and IT employees. IT is recognized by the configured employee record's IT department or an IT title (`IT`, `IT Manager`, `Information Technology`, `Information Systems`, or `Integration Technologist`). Access is rechecked against the synced employee directory on every report, refresh, and verification request. Admin-only login works without reserving a station. This report permission does not expand saved-history editing beyond the four Quality titles.

The report shows all signed-in stations and the latest inspection attempt for every pipe, including inspections not yet published at end of shift. Filter by station and review the same worksheet grid used for inspections: Element, DWG DIM, and Gauge down the left, with one column per pipe. Separate grids keep stations, WOs, connections, and Digital IRRs aligned. Each pipe column includes its inspector, result, verification, timestamps, and a link to full saved attempt history. Recipe rows are visible even before live values arrive; checks outside that pipe’s plan are marked Not due. Each page contains 20 pipes, with Previous/Next arrows at the bottom. Visible report pages refresh every five seconds; each grid’s horizontal scroll position and the selected page/filter are preserved. Update timestamps and a refresh error message show when information may be stale.

While an inspector types, the active worksheet sends a provisional measurement snapshot after a 600 ms pause and retries outstanding updates every five seconds. These live drafts are separate from submitted inspection records. Draft writes require the current inspector, session, and in-progress attempt; older requests cannot overwrite newer snapshots. The report labels drafts as live/not submitted and does not allow verifying them.

After submission, **Verify Inspection** records the reviewer's name, ADP number, title, timestamp, and a fingerprint of the saved inspection snapshot. Verification confirms the record was reviewed as correct; it does not change a pass, approved pass, re-work, or scrap disposition. An edit to saved measurements or identifiers causes **Needs re-verification**, and stale pages cannot verify content they have not displayed. Earlier snapshot verifications remain stored. Schema initialization creates `inspection_live_drafts`, `inspection_verifications`, and report indexes automatically. Restart the app and sign in again after updating; reload inspector worksheets to enable live draft updates.

Validation: `node --test --test-isolation=none node_app/tests/*.mjs`, and `.venv/Scripts/python.exe -m unittest discover -s tests`. Set `IRR_REPORT_DB_TESTS=1` to run the PostgreSQL report integration test, which creates and removes an isolated test schema without modifying production inspection records.

## Inspector measurement entry and next pipe

Inspection setup now follows **WO → Description → Digital IRR**. Select a released/in-process work order, then one of its operation descriptions. Size, weight, connection, and Box/Pin are read-only values derived from that WO description. Changing the WO/description clears the previous pipe/recipe selection and refreshes the matching IRRs automatically; no Load Options step is needed. If the WO has only one description with complete connection specifications, it is selected automatically.

Digital IRRs match the selected description's size (fractional and decimal forms are equivalent), weight, full connection name, and Box/Pin. A known drawing conflict excludes a recipe, while an exact drawing match ranks first; explicit description aliases remain authoritative. A unique best match is selected automatically. Tied matches require choosing among the matching IRRs, and missing specifications or no matching IRR prevent starting until the correct description/recipe data is available. The server revalidates the WO description and derives the fields again when starting, rather than trusting posted manual values. Older saved pipe/history labels with equivalent fraction/decimal, weight-unit, and connection-spacing formats remain recognized; duplicate equivalent records encountered during fallback lookup require Quality review.

Saved inspection editing and deletion are limited to employees whose **ADPJobTitleDesc** is **Director of Quality**, **Quality Control Specialist**, **Quality Manager**, or **Quality Supervisor** (complete title match, ignoring letter case and outer spaces). IT/general admin access and other job-title fields do not grant this permission. Edit controls follow that rule in both the worksheet's previous-pipe columns and Pipe History. The server rechecks the synced employee record on each edit-page request, save, and delete; missing/inactive employee records are denied. Sign out and back in after restarting to refresh the title used by navigation. Ordinary inspectors can still enter current-pipe measurements and load another pipe; submitted results cannot be overwritten by a stale completion request.

- Enter the complete numeric measurement, including its decimal point and sign where needed. The worksheet no longer supplies leading digits, interprets entries as offsets from nominal, or copies values from the previous pipe. Recipe specifications and saved history remain visible for reference.
- A visual checkbox passes only when checked. Unchecked, blank, or omitted visual results are failures and use the existing re-work / manager-approval flow. Numeric measurements still require an entry.
- After a successful pass, approved pass, or re-work submission is saved, the next sequential pipe loads automatically with empty measurements and unchecked visual boxes. The existing pipe-number field, step buttons, and Load Pipe control remain available to change the pipe number.
- If the next pipe cannot load (for example, its number is already completed), the saved result is preserved and the worksheet asks the inspector to check/change the pipe number. A non-sequential pipe number without trailing digits also requires entering the next number.
- Restart the inspection server (or let development watch mode reload it) and reload browser clients to use the updated workflow.

This folder contains the IRR inspection workflow web app:

- `src/server.js`
  - the inspection workflow app

## What it includes

- Express-based login flow
- Sidebar theme toggle
- Inspector session start / logout
- Workflow page with:
  - work order selection
  - connection selection
  - recipe selection
  - recipe preview
  - inspection preparation
  - inspection completion
  - pipe history search
  - NCR queue view
- Admin page for manager PIN setup

## Architecture

- `src/server.js`
  - Express web app and routes
- `src/pythonBridge.js`
  - Node-to-Python bridge helper
- `bridge/workflow_bridge.py`
  - Reuses the existing Python workflow logic from `workflow_db.py`
- `public/styles.css`
  - App styling

## Run The Inspection App

1. Install Node.js if it is not already installed.
2. From `node_app`, install dependencies:

```powershell
npm install
```

3. Start the app:

```powershell
npm run dev
```

4. Open:

```text
http://localhost:3000
```

## Notes

- This Node app currently reuses the existing Python business logic through the bridge script so behavior stays aligned with the Streamlit version.
- It uses the same `.env` and PostgreSQL database as the Python app.
- The CNC Time Entry app lives separately in `../time_entry_app`.


## Lunch inspections

Use **Lunch Inspection** at a signed-in station. Enter an active Quality admin or IT ADP number, select the station, and type the displayed last completed pipe number to confirm the physical pipe. Every Digital IRR element is included with blank measurements. Numeric measurements are required; unchecked visual boxes fail.

Submissions are immutable records in `lunch_inspections`, with the reviewer identity, original pipe context, full recipe snapshot, results, notes, and timestamps. They do not create or update production attempts, production measurements, pipe status, sampling rotation, or the inspector session. **Lunch Inspections History** is available to signed-in Quality admins and IT, in pages of 20. The app creates the new table/index during normal startup initialization; restart the Node app to activate the routes and schema.


## Break relief

Inspection Entry has a **Break** button. **Without Relief** pauses production entry behind a persistent End Break dialog. **With Relief** validates a different active employee ADP, shows the replacement inspector, and changes the button to **End Break**. Ending coverage restores the original inspector without logging either person into a new station session.

Break intervals are stored in `inspection_breaks`; production attempts store the final inspector ADP/name and link relief participation through `inspection_relief_attempts`. Relief columns have a red outline and a name/ADP label in the worksheet, Pipe History and Live Pipe Report. A pipe partly measured by relief retains that participation even when the owner finishes it; merely auto-loading the next empty pipe does not mark it. Drafts flush before each handoff, and a coverage version check rejects delayed drafts/submissions from an earlier handoff. Logout/unlock closes any active interval. Restart the Node app to apply the additive schema and activate the feature.


## Admin break report

Quality admins and IT can open **Admin Tools → Breaks** to review recorded break intervals across stations without starting a floor session. The report includes inspector and relief names/ADP numbers, break and relief start/end times, elapsed duration, station, branch, shift and active/completed status. All report times and break-start date filters use America/Chicago (Central Time). Relief coverage uses the recorded break interval. Filters include date range, station, person/ADP and status; pages contain 20 records with active breaks first. Active durations tick each second and the report refreshes every 15 seconds. Both page and live endpoint recheck current directory permissions; reporting only reads existing break records.


## Mold Approval

Signed-in inspectors can use **Mold Approval** to request approval for a work order at their active inspection station. Requests use the logged-in inspector badge/name and the station stored in the active session. The page shows recent requests and polls the selected pending request every 10 seconds for Quality’s response.

This reuses the existing time-entry notification transport, SharePoint list, callback token validation, and durable `time_entry_app/data/cnc_time_local.db` history. Both apps must remain in this checkout and have access to that shared store and the existing notification environment configuration. Historical time-app requests and its decision callback remain supported. The inspection app also supports the same token-protected `/api/notifications/:id/decision` callback route. No Power Automate configuration change is required for the existing callback URL. The time app no longer accepts new mold approval submissions. Restart both Node apps and reload tablets to activate the move.
