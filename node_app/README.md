# Inspection Run Report app

The unified release combines the live InspectionApp workflow with the offline,
reporting, permissions, and recipe capabilities from PrototypeIRR-1.

## Shared inspector sign-in

Inspectors sign into Time Entry once with their ADP, station, and shift, then go
directly to **Choose Machinist**. No second badge, station, or shift entry appears.
Active same-branch machinists are selected before opening the inspection workflow.

| Time Entry station | Inspection machine |
| --- | --- |
| Line 1 Inspection 1 | Machine 1 |
| Line 1 Inspection 2 | Machine 2 |
| Line 2 Inspection 1 | Machine 3 |
| Line 2 Inspection 2 | Machine 4 |

Inspection stations and inspector job titles automatically use this flow. Ordinary
machinists retain their Time Entry dashboard. An existing inspection under another
person or machine must be ended before switching. Hand-offs use short-lived,
one-use codes bound to the requesting browser; employee status, branch, station
locks, and machinist selection are checked by the inspection backend. Signing out
or ending an inspection shift returns to the shared Time Entry sign-in.

Both HTTP (3100 to 3000) and trusted tablet HTTPS (3443 to 3444) use the same flow.
For custom DNS/reverse proxies, configure CNC_TIME_PUBLIC_ORIGIN and
INSPECTION_PUBLIC_ORIGIN. The private code exchange defaults to loopback port 3100
and can be configured with CNC_TIME_INTERNAL_ORIGIN. Both services must be online
for a new sign-in; prepared inspection work continues using the offline workflow.

## Inspection entry

- WO descriptions prefill size, weight, connection, and Box/Pin. Inspectors can
  correct these fields. Matching IRRs are checked against the completed
  specifications, including known drawing conflicts; a unique best match is selected.
- Each pipe starts with blank measurements. Unsigned nonzero entries are complete
  measurements; nonzero nominal offsets require an explicit `+` or `-`.
  `0`, `.000`, and `0.000` mean zero offset when a nominal is configured.
  Offset fields show the calculated **Actual** value. Without a nominal, zero is
  a full measurement of zero. Stored numeric actuals are never offset twice.
- The custom touch keypad handles signs, decimals, selections, Clear, Next,
  and pipe-number loading. It works in the regular worksheet, lunch dialog,
  and offline tablet worksheet.
- Portrait layouts keep element numbers/names pinned while pipe columns scroll.
- Pass, approved pass, and rework save before opening the next pipe. Stale
  submissions cannot overwrite saved inspections or submit onto another pipe.

## NCR workflow

Rework keeps the existing NCR Info form and independent Pending Review process.
Active admins can review and verify; managers/supervisors explicitly send verified
NCRs to SharePoint one record at a time. Edits require re-verification. Existing
attachments, submission recovery, numbering, and review versions are preserved.
Offline failures require notes and upload as rework; Quality can complete the
remaining NCR details in the existing review screen.

## Lunch and breaks

Lunch Inspection opens the preserved original-versus-recheck comparison dialog.
Any active employee in the same branch can identify themselves by ADP. Results
remain separate in `inspection_lunch_checks`; production measurements, pipe
progression, and counts are unchanged. Existing lunch records remain readable.

Break Without Relief pauses entry. With Relief and Relief Arrived validate another
active employee in the same branch. Ending a break restores the original inspector.
Waiting/relief phases and all contributing relief inspectors are stored. Each
coverage change invalidates older tablet drafts/submissions. Admin Tools → Breaks
shows intervals, elapsed time, relief, station, shift, and filters in Central Time.

## Monitoring and verification

Inspection Monitoring retains ongoing station inspections and the latest 100 lunch
checks. Live Pipe Report adds worksheet grids, pipe history, saved-pipe verification,
and re-verification after edits. Drafts from the regular and connected offline
worksheets feed both views. Unsubmitted drafts cannot be verified.

Only Director of Quality, Quality Control Specialist, Quality Manager, and Quality
Supervisor (exact ADPJobTitleDesc matches) can edit/delete saved inspection history.
These permissions are rechecked against the synced employee record on each request.
IT can monitor and use reports without receiving saved-history editing permission.

## Offline tablets

Open **Open Offline Tablet Worksheet** after preparing the station/WO/IRR online.
Trusted HTTPS and a completed service-worker cache are required on network tablets.
Current measurements and queued pipes persist on the tablet, including after an
offline reload. Reconnection uploads in order with transactional, idempotent receipts.
New login, unprepared WOs, lunch, relief changes, and manager approvals need a connection.
Do not end the station session until queued inspections have uploaded.

HTTP remains on port 3000; tablet HTTPS uses port 3444 and the installed protected
certificate in ProgramData/Benoit/Apps/tls. The Windows task is **Benoit Inspection App**.

## Mold approval and recipe import

Mold Approval uses the existing Time Entry notification transport and history.
`config/inspection-runtime.json` points every app copy to the established notification
database and shared environment. Each copy loads its own .env first, preserving
local settings. No notification data is copied during a code deployment.

Admin recipe import accepts PDF, XLSX, and CSV and produces a draft for review.

## Run and validate

From node_app, use `npm start` (or `npm run dev` for watch mode).
The launcher honors PYTHON_EXECUTABLE and otherwise uses Python on PATH.
The installed Windows launcher uses the validated Python runtime recorded in
config/inspection-runtime.json; each copy retains its own settings and data.

From the repository root:

```powershell
node --test --test-isolation=none node_app/tests/*.mjs node_app/tests/*.js
$env:IRR_REPORT_DB_TESTS = '1'
python -m unittest discover -s tests
```

Database integration tests create and remove isolated schemas. Runtime
`/api/app-version` reports the loaded build, so static files alone cannot be
mistaken for an updated running server. Refresh tablets and sign in again after
restarting an older release.
