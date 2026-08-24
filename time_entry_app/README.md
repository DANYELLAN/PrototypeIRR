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
npm run dev
```

4. Open:

```text
http://localhost:3100
```

## Linkage to the inspection app

This app is intentionally separate, but it still shares the same project environment, database, and business context as the inspection app. That means you can keep the two experiences independent while still linking them by employee, machine, work order, or later record IDs when needed.

## Notes

- The app uses the same project environment and backend data sources as the inspection workflow.
- If `CNC_TIME_IT_WEBHOOK_URL` or `CNC_TIME_SUPERVISOR_WEBHOOK_URL` are not set, support requests are queued locally in `data/cnc_time_outbox.jsonl`.
