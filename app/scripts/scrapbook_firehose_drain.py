#!/usr/bin/env python3
# /// script
# [tool.job]
# name = "Scrapbook — hourly Firehose drain"
# description = "Once an hour, drain the webhook_events queue for the Scrapbook so new Firehose tap matches appear in the Console for review."
# schedule = "0 * * * *"
# session = "ephemeral"
# ///
"""Hourly drain of Firehose events into the Scrapbook buffer.

Single source of truth: hits the same /api/firehose/drain endpoint as the
"Refresh (drain now)" button so behaviour is identical.
"""
import sys
import json
import urllib.request

CONSOLE_URL = "http://127.0.0.1:8080/applications/scrapbook/api/firehose/drain"


def main():
    req = urllib.request.Request(CONSOLE_URL, method="POST",
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, data=b"{}", timeout=60) as r:
            body = r.read().decode() or "{}"
            j = json.loads(body)
    except Exception as e:
        print(f"[scrapbook-firehose] drain failed: {e}", file=sys.stderr)
        sys.exit(1)
    print(f"[scrapbook-firehose] OK inserted={j.get('inserted')} "
          f"skipped={j.get('skipped')} errors={j.get('errors')}")
    if j.get("last_error"):
        print(f"[scrapbook-firehose] last_error={j['last_error']}", file=sys.stderr)


if __name__ == "__main__":
    main()
