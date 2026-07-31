"""Scrapbook core — shared settings object, job registry, filter pipeline, Ahrefs + LLM access.

Implements the shared plumbing every non-Scraps section of the spec depends on:

* `settings` — one flat key/value bag (target_site, target_country, competitors,
  brand_terms, filters). Spec: data-model.md "Shared".
* `jobs` — the 30-second rule. Any scan/pull/audit returns {job_id} immediately and
  is polled via GET /api/job/<job_id>. Spec: api.md "Shared".
* `keep_keyword()` — the ONE keep/drop rule the research tabs share, including the
  own-brand exception, the always-on NSFW drop, the prefix category match and the
  opt-in growth gate. Spec: filter-pipeline.md.
* `ahrefs()` / `llm_client()` / `embed()` — server-side access only, never the browser.
"""

from __future__ import annotations

import threading
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from src.connectors import invoke as _connector_invoke, ConnectorError  # noqa: F401
from src.llm import console_openai_client, embed_texts

APP_SLUG = "applications:scrapbook"
AHREFS_SECRET = "ahrefs_oauth"

CHAT_MODEL = "anthropic/claude-sonnet-4.5"
CHEAP_MODEL = "anthropic/claude-haiku-4.5"
EMBED_MODEL = "text-embedding-3-small"

_llm = console_openai_client(app_slug=APP_SLUG)


def llm_client():
    return _llm


def chat(messages: list, model: str = CHEAP_MODEL, temperature: float = 0.3,
         max_tokens: int = 2000, json_mode: bool = False) -> str:
    kw: dict[str, Any] = dict(model=model, messages=messages, temperature=temperature,
                             max_tokens=max_tokens)
    if json_mode:
        try:
            r = _llm.chat.completions.create(response_format={"type": "json_object"}, **kw)
            return (r.choices[0].message.content or "").strip()
        except Exception:
            pass
    r = _llm.chat.completions.create(**kw)
    return (r.choices[0].message.content or "").strip()


def embed(texts: list[str]) -> list[list[float]]:
    """Embed a batch of strings. Chunked so a big crawl doesn't blow the request size."""
    out: list[list[float]] = []
    for i in range(0, len(texts), 96):
        out.extend(embed_texts(texts[i:i + 96], model=EMBED_MODEL, app_slug=APP_SLUG))
    return out


def ahrefs(cap: str, args: dict, timeout: int = 120) -> dict:
    """Call an Ahrefs capability through the connector proxy (never from the browser).

    Spec workflow 07 transport note: a silent transport change once made every scan
    return zero rows for a month. Callers MUST treat an all-zero scan as a failure;
    this helper raises loudly rather than returning {} on a bad status.
    """
    return _connector_invoke(cap, args, secret=AHREFS_SECRET, timeout=timeout)


# ---------------------------------------------------------------------------
# Jobs — the 30-second rule
# ---------------------------------------------------------------------------

_jobs: dict[str, dict] = {}
_jobs_lock = threading.Lock()
JOB_KEEP = 60  # most recent jobs retained in memory


def job_new(kind: str = "") -> str:
    jid = str(uuid.uuid4())
    with _jobs_lock:
        _jobs[jid] = {"status": "running", "kind": kind, "progress": "Starting…",
                      "result": None, "error": None,
                      "started_at": datetime.now(timezone.utc).isoformat()}
        if len(_jobs) > JOB_KEEP:
            for old in list(_jobs)[:-JOB_KEEP]:
                _jobs.pop(old, None)
    return jid


def job_progress(jid: str, msg: str) -> None:
    with _jobs_lock:
        if jid in _jobs:
            _jobs[jid]["progress"] = msg


def job_done(jid: str, result: Any = None, progress: str = "") -> None:
    with _jobs_lock:
        if jid in _jobs:
            _jobs[jid].update(status="completed", result=result,
                              progress=progress or "Done", error=None)


def job_fail(jid: str, err: str) -> None:
    with _jobs_lock:
        if jid in _jobs:
            _jobs[jid].update(status="failed", error=err, progress=f"Failed: {err}")


def job_get(jid: str) -> Optional[dict]:
    with _jobs_lock:
        j = _jobs.get(jid)
        return dict(j) if j else None


def job_run(kind: str, fn: Callable[..., Any], *args, **kwargs) -> str:
    """Start `fn(job_id, *args)` on a daemon thread and return the job id."""
    jid = job_new(kind)

    def _wrap():
        try:
            fn(jid, *args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            job_fail(jid, str(exc))
            print(f"[scrapbook:{kind}] job {jid} failed:\n{traceback.format_exc()}")

    threading.Thread(target=_wrap, daemon=True).start()
    return jid


# ---------------------------------------------------------------------------
# Shared settings object
# ---------------------------------------------------------------------------

DEFAULT_FILTERS = {
    "min_volume": 100,
    "max_kd": None,
    "min_position": 41,
    "min_growth_3m": 10,
    "exclude_branded": True,
    "exclude_local": True,
    "exclude_terms": ["cheap", "free", "near me"],
    "allowed_categories": [],
    "drop_uncategorized": False,
}

DEFAULT_SETTINGS = {
    "target_site": "",
    "target_country": "us",
    "competitors": [],
    "brand_terms": [],
    "filters": DEFAULT_FILTERS,
    "radar_subreddits": ["SEO", "bigseo", "juststart"],
    "radar_queries": ["ai overviews", "ai search", "llm seo"],
}


def own_brand(settings: dict) -> str:
    """'example' from 'example.com' — the token the own-brand exception protects."""
    site = (settings.get("target_site") or "").strip().lower()
    site = site.replace("https://", "").replace("http://", "").split("/")[0]
    site = site[4:] if site.startswith("www.") else site
    return site.split(".")[0] if site else ""


def brand_tokens(settings: dict) -> list[str]:
    toks = [own_brand(settings)] + [str(t).lower() for t in (settings.get("brand_terms") or [])]
    return [t for t in toks if t]


# ---------------------------------------------------------------------------
# The shared filter pipeline (filter-pipeline.md)
# ---------------------------------------------------------------------------

def keep_keyword(keyword: str, cand: dict, filters: dict, brands: list[str],
                 *, apply_growth: bool = False, text_only: bool = False) -> bool:
    """Return True to KEEP the candidate, False to DROP it.

    `cand` accepts: volume, difficulty, growth_rate{months_3/6/12}, attrs{branded,local},
    categories{category[],nsfw[]}.

    `apply_growth`  — growth gate is opt-in per tab (Trending + Growth Scanner only).
    `text_only`     — pass 1 for Content-Gap-style two-pass filtering: only the checks
                      that need no Keywords-Explorer enrichment.
    """
    kw = (keyword or "").lower()
    f = {**DEFAULT_FILTERS, **(filters or {})}

    vol = cand.get("volume") or 0
    if vol < (f.get("min_volume") or 0):
        return False

    kd = cand.get("difficulty")
    if f.get("max_kd") is not None and kd is not None and kd > f["max_kd"]:
        return False

    exclude_terms = [str(t).lower() for t in (f.get("exclude_terms") or [])]
    if exclude_terms and any(t and t in kw for t in exclude_terms):
        return False

    if text_only:
        return True

    if apply_growth and f.get("min_growth_3m") is not None:
        growth = cand.get("growth_rate")
        if growth is not None:
            g3 = growth.get("months_3") if isinstance(growth, dict) else None
            if g3 is None or g3 < f["min_growth_3m"]:
                return False

    attrs = cand.get("attrs") or {}
    if f.get("exclude_branded") and attrs.get("branded"):
        # Own-brand exception: drop competitor-brand searches, keep ours.
        if not any(b in kw for b in brands):
            return False

    if f.get("exclude_local") and attrs.get("local"):
        return False

    cats = cand.get("categories") or {}
    nsfw = cats.get("nsfw") or []
    kw_cats = cats.get("category") or []
    # NSFW is always dropped — never a user toggle.
    if "Adult" in nsfw or "Nsfw" in nsfw or "Adult" in kw_cats:
        return False

    allowed = f.get("allowed_categories") or []
    if allowed:
        if not kw_cats:
            if f.get("drop_uncategorized"):
                return False
        elif not any(c.startswith(a) for c in kw_cats for a in allowed):
            return False

    return True


def keep_position(position: Optional[int], min_position: int = 41) -> bool:
    """Position filter, applied AFTER rank-checking the target site (Content Gap).

    no ranking            → keep (a true opportunity)
    position < threshold  → drop (we already rank well enough)
    position >= threshold → keep (room to improve)
    """
    if position is None:
        return True
    return position >= (min_position if min_position is not None else 41)


def normalize_ke_row(row: dict) -> dict:
    """Map an Ahrefs Keywords-Explorer export row onto the filter-pipeline candidate shape."""
    intents = {str(v).lower() for v in (row.get("intents") or [])}
    cat = row.get("category")
    growth = row.get("growth_rate")
    if not isinstance(growth, dict):
        growth = {"months_3": row.get("growth_3mo"), "months_6": row.get("growth_6mo"),
                  "months_12": row.get("growth_12mo")}
    return {
        "volume": row.get("volume") or 0,
        "difficulty": row.get("difficulty"),
        # Preserve None — Ahrefs genuinely has no KD/TP for very fresh long-tail
        # keywords, and showing "0" would misrepresent unknown as zero.
        "traffic_potential": row.get("traffic_potential"),
        "cpc_cents": (int(float(row["cpc"]) * 100) if row.get("cpc") is not None else None),
        "parent_topic": row.get("parent_keyword") or row.get("parent_topic"),
        "growth_rate": growth,
        "attrs": {"branded": bool(row.get("branded") or "branded" in intents),
                  "local": bool(row.get("local") or "local" in intents)},
        "categories": {"category": ([cat] if isinstance(cat, str) and cat else
                                    (cat if isinstance(cat, list) else [])),
                       "nsfw": row.get("nsfw") or []},
    }
