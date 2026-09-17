"""
Show or update the Realtime Database security rules with the service account
(no console needed).

    python scripts/firebase_rules.py            # print current rules
    python scripts/firebase_rules.py --apply    # index `status` on every top-level queue path

Every queue path (`queue`, `queue_detail`, ...) needs `.indexOn: ["status"]`
because QueueManager filters by `order_by_child("status")`.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import requests

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import load_settings  # noqa: E402

RULES = {
    "rules": {
        "$queue": {
            ".indexOn": ["status"],
            ".read": False,
            ".write": False,
        }
    }
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    settings = load_settings()
    from google.oauth2 import service_account
    from google.auth.transport.requests import Request

    creds = service_account.Credentials.from_service_account_file(
        settings.firebase_cred_path,
        scopes=["https://www.googleapis.com/auth/firebase.database", "https://www.googleapis.com/auth/userinfo.email"],
    )
    creds.refresh(Request())
    url = settings.firebase_db_url.rstrip("/") + "/.settings/rules.json"
    headers = {"Authorization": f"Bearer {creds.token}"}

    current = requests.get(url, headers=headers, timeout=30)
    print("current rules:", current.status_code)
    print(current.text)
    if not args.apply:
        return
    resp = requests.put(url, headers=headers, data=json.dumps(RULES), timeout=30)
    print("apply:", resp.status_code, resp.text[:200])


if __name__ == "__main__":
    main()
