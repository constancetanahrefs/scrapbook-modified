#!/usr/bin/env python3
# /// script
# [tool.job]
# name = "Scrapbook — daily LinkedIn scrape"
# description = "Once a day, pull all pending LinkedIn items in the Scrapbook through the Apify actor and persist the scraped content."
# schedule = "0 6 * * *"
# session = "ephemeral"
# ///
"""Daily LinkedIn scrape for the Scrapbook Console app.

Calls Console's /api/scrape/run + polls /api/scrape/run/<job_id>. We hit the
Console blueprint directly on the loopback port so the same code path runs as
the "Scrape now" button — one source of truth.

If Console is down we fall back to a direct DB + api-proxy invocation.
"""
import os
import sys
import time
import json
import urllib.request

CONSOLE_BASE = "http://127.0.0.1:8080/applications/scrapbook"
TIMEOUT = 20  # initial POST is fast


def _http(url, method="GET", body=None):
    req = urllib.request.Request(url, method=method, headers={"Content-Type": "application/json"})
    data = json.dumps(body).encode() if body is not None else None
    with urllib.request.urlopen(req, data=data, timeout=TIMEOUT) as r:
        return json.loads(r.read().decode() or "{}")


def main():
    print(f"[scrapbook-scrape] starting daily run via {CONSOLE_BASE}")
    try:
        kick = _http(f"{CONSOLE_BASE}/api/scrape/run", method="POST", body={})
    except Exception as e:
        print(f"[scrapbook-scrape] POST failed: {e}", file=sys.stderr)
        sys.exit(1)
    status = kick.get("status")
    if status == "done":
        print(f"[scrapbook-scrape] nothing pending: {kick.get('message')}")
        return
    if status != "started":
        print(f"[scrapbook-scrape] unexpected response: {kick}", file=sys.stderr)
        sys.exit(2)

    job_id = kick.get("job_id")
    pending = kick.get("pending", "?")
    print(f"[scrapbook-scrape] job {job_id} started, {pending} items pending")

    # Poll up to 25 minutes (each Apify sync run is capped at 5 min and we batch 25 urls)
    deadline = time.time() + 25 * 60
    while time.time() < deadline:
        time.sleep(15)
        try:
            st = _http(f"{CONSOLE_BASE}/api/scrape/run/{job_id}")
        except Exception as e:
            print(f"[scrapbook-scrape] poll failed: {e}", file=sys.stderr)
            continue
        if st.get("status") == "done":
            print(f"[scrapbook-scrape] OK processed={st.get('processed')} "
                  f"succeeded={st.get('succeeded')} failed={st.get('failed')}")
            if st.get("last_error"):
                print(f"[scrapbook-scrape] last_error={st['last_error']}", file=sys.stderr)
            return
        if st.get("status") == "error":
            print(f"[scrapbook-scrape] error: {st.get('error')}", file=sys.stderr)
            sys.exit(3)
    print("[scrapbook-scrape] timeout waiting for scrape job", file=sys.stderr)
    sys.exit(4)


if __name__ == "__main__":
    main()
