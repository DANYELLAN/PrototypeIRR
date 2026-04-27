# AutoIRR Node App

This folder now contains two web apps:

- `src/server.js`
  - the existing inspection workflow app
- `src/cncTimeServer.js`
  - a new web recreation of the `Ennis CNC Time Entry` PowerApp

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

## Run The CNC Time Entry App

1. From `node_app`, start the dedicated CNC app:

```powershell
npm run dev:cnc-time
```

2. Open:

```text
http://localhost:3100
```

3. The CNC app can be installed as a desktop-style PWA from the browser once it is running.

## Notes

- This Node app currently reuses the existing Python business logic through the bridge script so behavior stays aligned with the Streamlit version.
- It uses the same `.env` and PostgreSQL database as the Python app.
- The CNC Time Entry app uses Microsoft Graph + SharePoint through `bridge/cnc_time_bridge.py` and `bridge/cnc_time_backend.py`.
- If `CNC_TIME_IT_WEBHOOK_URL` or `CNC_TIME_SUPERVISOR_WEBHOOK_URL` are not set, support requests are queued locally in `node_app/data/cnc_time_outbox.jsonl`.
