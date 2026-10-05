import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import load_env_file  # noqa: E402

load_env_file()

from time_entry_app.acumatica_sync import sync_approved_entries  # noqa: E402


def main():
    result = sync_approved_entries(trigger_name="scheduled")
    print(json.dumps(result, default=str))
    return 1 if result.get("status") == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
