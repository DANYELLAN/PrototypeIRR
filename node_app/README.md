# AutoIRR Node App

First-pass Node.js duplicate of the Streamlit app.

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

## Run

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
