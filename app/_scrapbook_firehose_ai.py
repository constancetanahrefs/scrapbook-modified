"""Scrapbook — AI-assisted Firehose tap builder.

Turns a plain-English description ("watch for competitor pricing changes and AI-search
chatter") into a Firehose tap plus one or more Lucene rules, with a preview + chat
refinement step before anything is created.

Flow
----
1. `interpret(description)`            → a draft plan (tap name + rules + explanation)
2. `refine(plan, instruction)`         → an updated plan, conversationally
3. `create(plan)`                      → firehose.create_tap → firehose.create_rule × n
                                         → register locally → subscribe to the SSE stream

Constraints that shape this (from the connector schemas):
* `firehose.create_tap` takes only `name` + `metadata`; it mints and stores the tap
  token itself as `firehose-tap-<tap_id>`. We never see the raw token.
* `firehose.create_rule` needs the TAP's own token secret, not the management key.
* **Max 25 rules per ORGANISATION across all taps** — a shared budget, so we count
  existing usage and refuse to overspend it.
* Firehose is a live stream: rules only match documents published from then on.
"""

from __future__ import annotations

import json
import re
from typing import Optional

import requests

from applications._scrapbook_core import CHAT_MODEL, chat

API_PROXY_CAPS = "http://127.0.0.1:18081/capabilities"
MGMT_SECRET = "firehose_main"
ORG_RULE_CAP = 25          # hard limit imposed by Firehose, across ALL taps
MAX_RULES_PER_TAP = 6      # our own sanity cap so one description can't eat the budget


class FirehoseAIError(RuntimeError):
    """Carries a user-facing message (safe to show verbatim in the UI)."""


def _invoke(cap_id: str, args: dict, secret_name: str, timeout: int = 30) -> dict:
    try:
        r = requests.post(f"{API_PROXY_CAPS}/invoke/{cap_id}",
                          json={"caller": "app", "secret_name": secret_name, "args": args},
                          timeout=timeout)
    except requests.RequestException as exc:
        raise FirehoseAIError(f"Could not reach the Firehose connector: {exc}")
    try:
        d = r.json()
    except Exception:
        raise FirehoseAIError(f"Firehose returned a non-JSON response ({r.status_code}).")
    status = d.get("status")
    if status == "not_approved":
        raise FirehoseAIError(
            f"“{cap_id}” hasn't been approved for the Console yet. Ask in chat to approve it "
            f"for the secret “{secret_name}”, then try again.")
    if status != "ok":
        raise FirehoseAIError(d.get("error") or f"Firehose call failed (status={status}).")
    return d.get("result") or {}


# ---------------------------------------------------------------------------
# Rule-budget accounting
# ---------------------------------------------------------------------------

def rule_budget() -> dict:
    """Count rules across every tap we can read, so we never blow the org cap.

    A tap whose token secret is missing can't be counted — we report it as unknown
    rather than pretending the budget is bigger than it is.
    """
    taps = (_invoke("firehose.list_taps", {}, MGMT_SECRET).get("taps") or [])
    used, unknown = 0, []
    per_tap = []
    for t in taps:
        tap_id = t.get("id") or ""
        secret = f"firehose-tap-{tap_id}"
        count = None
        for cand in (secret, f"firehose-tap-{tap_id.split('-', 1)[0]}"):
            try:
                res = _invoke("firehose.list_rules", {}, cand, timeout=20)
                count = len(res.get("rules") or [])
                break
            except FirehoseAIError:
                continue
        if count is None:
            unknown.append({"tap_id": tap_id, "name": t.get("name") or tap_id})
        else:
            used += count
            per_tap.append({"tap_id": tap_id, "name": t.get("name") or tap_id, "rules": count})
    return {"cap": ORG_RULE_CAP, "used": used, "remaining": max(0, ORG_RULE_CAP - used),
            "per_tap": per_tap, "uncounted_taps": unknown, "tap_count": len(taps)}



def preflight() -> dict:
    """Check the two write capabilities are approved for this surface BEFORE the user
    spends time describing a tap. Probes with deliberately empty args: an approved
    connector answers `args_invalid` (validation reached), an unapproved one
    `not_approved`. Nothing is created either way.
    """
    missing = []
    probes = [("firehose.create_tap", MGMT_SECRET, "create the tap")]
    for cap, secret, what in probes:
        try:
            r = requests.post(f"{API_PROXY_CAPS}/invoke/{cap}",
                              json={"caller": "app", "secret_name": secret, "args": {}}, timeout=20)
            status = (r.json() or {}).get("status")
        except Exception:
            continue          # network trouble is reported later, by the real call
        if status == "not_approved":
            missing.append({"connector": cap, "purpose": what})
    return {"ok": not missing, "missing": missing}


# ---------------------------------------------------------------------------
# Interpretation
# ---------------------------------------------------------------------------

LUCENE_GUIDE = """\
Firehose rules are LUCENE queries matched against newly-published web documents.

Supported syntax:
- bare terms:            seo tools
- OR / AND / NOT:        ahrefs OR semrush ;  seo AND pricing ;  seo NOT jobs
- exact phrase:          "site explorer"
- grouping:              (ahrefs OR semrush) AND (pricing OR review)
- field scoping:         title:webinar ;  domain:nytimes.com ;  url:/blog/
- fields available:      title, domain, url, text

Rules of thumb:
- Prefer a phrase over loose terms when the words only matter together.
- Scope to `title:` when the topic must be the subject of the article, not a passing mention.
- Add NOT clauses for predictable noise (job ads, coupon/deal spam, login pages).
- Firehose is a LIVE stream: a rule only matches documents published after it is created.
"""

PLAN_SCHEMA = """\
Return JSON ONLY, exactly this shape:
{
  "tap_name": "short display name for the tap (max 80 chars)",
  "summary": "1-2 sentences, plain English, describing what this tap will collect",
  "rules": [
    {
      "tag": "short label for this angle (max 40 chars)",
      "value": "the Lucene query",
      "nsfw": false,
      "quality": true,
      "explanation": "one sentence: what this rule catches",
      "matches": ["a realistic headline this WOULD match", "another"],
      "excludes": ["a realistic headline this would NOT match, and why in brackets"]
    }
  ],
  "caveats": ["anything the user should know — over-broad terms, noise risk, etc."]
}
"""


def _system_prompt(max_rules: int) -> str:
    return (
        "You configure Firehose monitoring taps from a plain-English description.\n\n"
        + LUCENE_GUIDE
        + f"\nSplit the description into at most {max_rules} rules — one per genuinely distinct "
        "angle (e.g. competitor mentions vs. industry trend chatter). Do NOT split for the sake "
        "of it: if one query covers the intent cleanly, return a single rule.\n"
        "Set quality=true unless the user explicitly wants unfiltered results. Set nsfw=false "
        "unless the user explicitly asks to include adult content.\n"
        "Every `matches`/`excludes` example must be a plausible real headline — these are shown "
        "to the user as the preview, so they must honestly reflect the query you wrote.\n\n"
        + PLAN_SCHEMA
    )


def _parse_plan(raw: str) -> dict:
    txt = re.sub(r"^```(?:json)?|```$", "", (raw or "").strip(), flags=re.M).strip()
    try:
        plan = json.loads(txt)
    except Exception:
        m = re.search(r"\{.*\}", txt, re.S)
        if not m:
            raise FirehoseAIError("The model didn't return a usable plan. Try rewording your description.")
        try:
            plan = json.loads(m.group(0))
        except Exception:
            raise FirehoseAIError("The model didn't return a usable plan. Try rewording your description.")
    return _validate_plan(plan)


def _validate_plan(plan: dict) -> dict:
    if not isinstance(plan, dict):
        raise FirehoseAIError("The model returned an unexpected plan shape.")
    rules_in = plan.get("rules") or []
    if not isinstance(rules_in, list) or not rules_in:
        raise FirehoseAIError("The model proposed no rules. Try describing what you want to watch for.")
    rules = []
    for r in rules_in[:MAX_RULES_PER_TAP]:
        if not isinstance(r, dict):
            continue
        value = str(r.get("value") or "").strip()
        if not value:
            continue
        rules.append({
            "tag": str(r.get("tag") or "")[:40],
            "value": value[:1000],
            "nsfw": bool(r.get("nsfw")),
            "quality": bool(r.get("quality", True)),
            "explanation": str(r.get("explanation") or "")[:400],
            "matches": [str(x)[:200] for x in (r.get("matches") or [])][:4],
            "excludes": [str(x)[:200] for x in (r.get("excludes") or [])][:4],
        })
    if not rules:
        raise FirehoseAIError("Every proposed rule was empty. Try rewording your description.")
    return {
        "tap_name": (str(plan.get("tap_name") or "Untitled tap")[:80]).strip(),
        "summary": str(plan.get("summary") or "")[:600],
        "rules": rules,
        "caveats": [str(c)[:300] for c in (plan.get("caveats") or [])][:6],
    }


def interpret(description: str) -> dict:
    desc = (description or "").strip()
    if len(desc) < 8:
        raise FirehoseAIError("Describe what you'd like to watch for in a sentence or two.")
    raw = chat([
        {"role": "system", "content": _system_prompt(MAX_RULES_PER_TAP)},
        {"role": "user", "content": f"Set up a tap for:\n\n{desc}"},
    ], model=CHAT_MODEL, json_mode=True, max_tokens=2000, temperature=0.2)
    plan = _parse_plan(raw)
    plan["description"] = desc
    return plan


def refine(plan: dict, instruction: str, history: Optional[list] = None) -> dict:
    instr = (instruction or "").strip()
    if not instr:
        raise FirehoseAIError("Say what you'd like changed.")
    current = json.dumps({k: plan.get(k) for k in ("tap_name", "summary", "rules", "caveats")},
                         indent=1)
    msgs = [{"role": "system", "content": _system_prompt(MAX_RULES_PER_TAP)}]
    for turn in (history or [])[-6:]:
        role = turn.get("role")
        if role in ("user", "assistant") and turn.get("content"):
            msgs.append({"role": role, "content": str(turn["content"])[:1500]})
    msgs.append({"role": "user", "content":
                 f"ORIGINAL REQUEST: {plan.get('description', '')}\n\n"
                 f"CURRENT PLAN:\n{current}\n\n"
                 f"CHANGE REQUESTED: {instr}\n\n"
                 "Return the full updated plan as JSON."})
    raw = chat(msgs, model=CHAT_MODEL, json_mode=True, max_tokens=2000, temperature=0.2)
    out = _parse_plan(raw)
    out["description"] = plan.get("description", "")
    return out


def set_rule_value(plan: dict, index: int, value: str) -> dict:
    """Hand-edit one rule's Lucene query (kept for full user control)."""
    rules = list(plan.get("rules") or [])
    if not (0 <= index < len(rules)):
        raise FirehoseAIError("That rule no longer exists.")
    v = (value or "").strip()
    if not v:
        raise FirehoseAIError("A rule query can't be empty.")
    rules[index] = {**rules[index], "value": v[:1000], "explanation": "Edited by hand."}
    return {**plan, "rules": rules}


# ---------------------------------------------------------------------------
# Creation
# ---------------------------------------------------------------------------

def create_tap_only(plan: dict) -> dict:
    """STEP 1 of 2 — create the tap and return its freshly-minted secret name.

    Why this is split: `create_tap` mints a brand-new secret (`firehose-tap-<tap_id>`),
    and connector approvals are per (connector, secret, surface). A grant therefore
    cannot exist for a secret that doesn't exist yet, so rule creation on a NEW tap can
    never be pre-approved. The user approves `firehose.create_rule` for this specific
    secret, then calls `finish_setup()`.
    """
    plan = _validate_plan(plan)
    wanted = len(plan["rules"])

    budget = rule_budget()
    if budget["remaining"] < wanted:
        raise FirehoseAIError(
            f"This plan needs {wanted} rule(s) but only {budget['remaining']} of Firehose's "
            f"{ORG_RULE_CAP}-rule organisation budget is free"
            + (f" ({budget['used']} already in use)" if budget["used"] else "")
            + ". Remove a rule from this plan, or delete an unused rule on another tap.")

    created = _invoke("firehose.create_tap",
                      {"name": plan["tap_name"],
                       "metadata": {"created_by": "scrapbook-ai-tap-builder",
                                    "description": plan.get("description", "")[:500]}},
                      MGMT_SECRET, timeout=60)
    tap_id = str(created.get("tap_id") or "")
    if not tap_id:
        raise FirehoseAIError("Firehose created no tap id — nothing was set up.")
    secret_name = created.get("secret_name") or f"firehose-tap-{tap_id}"
    return {"tap_id": tap_id, "tap_name": plan["tap_name"], "secret_name": secret_name,
            "rules_pending": wanted}


def rules_approved(secret_name: str) -> bool:
    """Has `firehose.create_rule` been approved for this tap's secret yet?"""
    try:
        r = requests.post(f"{API_PROXY_CAPS}/invoke/firehose.create_rule",
                          json={"caller": "app", "secret_name": secret_name, "args": {}},
                          timeout=20)
        return (r.json() or {}).get("status") != "not_approved"
    except Exception:
        return False


def finish_setup(plan: dict, tap_id: str, secret_name: str,
                 register_cb=None, subscribe: bool = True) -> dict:
    """STEP 2 of 2 — register the rules on an already-created tap, then subscribe."""
    plan = _validate_plan(plan)
    rule_results, failures = [], []
    for r in plan["rules"]:
        args = {"value": r["value"], "nsfw": r["nsfw"], "quality": r["quality"]}
        if r.get("tag"):
            args["tag"] = r["tag"]
        try:
            res = _invoke("firehose.create_rule", args, secret_name, timeout=45)
            rule = res.get("rule") or {}
            rule_results.append({"id": rule.get("id"), "value": rule.get("value") or r["value"],
                                 "tag": rule.get("tag") or r.get("tag") or ""})
        except FirehoseAIError as exc:
            failures.append({"value": r["value"], "tag": r.get("tag") or "", "error": str(exc)})

    if not rule_results:
        detail = failures[0]["error"] if failures else "no rules were accepted"
        raise FirehoseAIError(
            f"No rules could be registered on “{plan['tap_name']}”, so it is monitoring nothing. "
            f"Reason: {detail}")

    if register_cb:
        try:
            register_cb(tap_id, plan["tap_name"], secret_name)
        except Exception as exc:  # noqa: BLE001
            failures.append({"value": "(local registration)", "tag": "",
                             "error": f"Tap created but not recorded in Scrapbook: {exc}"})

    sub = _subscribe(secret_name) if subscribe else None
    return {"tap_id": tap_id, "tap_name": plan["tap_name"], "secret_name": secret_name,
            "rules": rule_results, "failures": failures, "subscription": sub,
            "rules_created": len(rule_results), "rules_requested": len(plan["rules"])}


def _subscribe(secret_name: str) -> dict:
    """Subscribe to the tap's SSE stream so matches land in the existing ingest cursor."""
    try:
        r = requests.post("http://127.0.0.1:18081/connectors/subscriptions",
                          json={"connector_id": "firehose.events.stream",
                                "secret_name": secret_name,
                                "cursor_name": "scrapbook-firehose-ingest",
                                "args": {}},
                          timeout=30)
        if r.status_code >= 400:
            return {"ok": False, "error": f"HTTP {r.status_code}: {r.text[:200]}"}
        d = r.json()
        return {"ok": True, "subscription_id": d.get("id") or d.get("subscription_id")}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": str(exc)}
