"""Scrapbook — Topic research + Monitoring engines.

Workflows implemented here:
  07 Trending keywords  — seed scan into a DURABLE keyword bank (merge, never wipe)
  08 Topics             — crawl → chunk → embed → cluster → concentration verdict
  09 Reddit Radar       — subreddit/query scan + LLM weekly digest
  10 Growth Scanner     — category fence discovery → keyword pool → domain ranking → snapshot

Workflow 06 (Content gap) is deliberately NOT built: it duplicates the Keyword Research
Hub's Content Gap tab, and the user chose to skip duplicates.
"""

from __future__ import annotations

import json
import math
import random
import re
import subprocess
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.db_cross import cross_session_scope

from applications._scrapbook_core import (
    CHEAP_MODEL, CHAT_MODEL, ahrefs, brand_tokens, chat, embed, job_done, job_fail,
    job_progress, keep_keyword, normalize_ke_row,
)
from applications._scrapbook_models import (
    SBCategory, SBRadarReport, SBRedditPost, SBTopicCluster, SBTopicPage, SBTopicScanState,
    SBTrendingKeyword, SBTrendingScan, SBTrendingSeed,
)


def _now():
    return datetime.now(timezone.utc)


# ===========================================================================
# Workflow 07 — Trending keywords
# ===========================================================================

IDEA_MODES = ("matching_terms", "matching_questions")


def run_trending_scan(job_id: str, settings: dict) -> None:
    country = (settings.get("target_country") or "us").lower()
    filters = settings.get("filters") or {}
    brands = brand_tokens(settings)
    min_vol = filters.get("min_volume") or 0

    with cross_session_scope() as s:
        seeds = [r.seed for r in s.query(SBTrendingSeed).filter(SBTrendingSeed.active.is_(True)).all()]

    if not seeds:
        job_fail(job_id, "No active seed topics. Add seeds first.")
        return

    found: dict[str, dict] = {}
    warnings: list[str] = []
    ok_calls = 0

    for i, seed in enumerate(seeds, 1):
        for mode in IDEA_MODES:
            job_progress(job_id, f"Seed {i}/{len(seeds)}: '{seed}' ({mode.replace('_', ' ')})…")
            try:
                res = ahrefs("ahrefs_keywords_explorer.ideas_by_terms_export", {
                    "seed_keywords": [seed], "country": country, "mode": mode,
                    "limit": 100, "offset": 0,
                    "order_by": "volume", "direction": "desc",
                    "filters": ({"min_volume": min_vol} if min_vol else {}),
                })
                ok_calls += 1
            except Exception as exc:  # noqa: BLE001
                warnings.append(f"{mode} failed for '{seed}': {exc}")
                continue
            for row in res.get("records") or []:
                kw = (row.get("keyword") or "").strip().lower()
                if not kw:
                    continue
                cand = normalize_ke_row(row)
                # Growth gate ON for this tab (spec: opt-in per tab).
                if not keep_keyword(kw, cand, filters, brands, apply_growth=True):
                    continue
                prev = found.get(kw)
                if prev:
                    # Track distinct seeds only — the same seed surfaces a keyword in
                    # several modes, and repeating it just makes the column noise.
                    seeds_seen = [s_.strip() for s_ in prev["source_seed"].split(",")]
                    if seed not in seeds_seen:
                        prev["source_seed"] = ", ".join(seeds_seen + [seed])[:400]
                    # A later mode may carry metrics the first one lacked.
                    for key, val in (("difficulty", cand["difficulty"]),
                                     ("traffic_potential", cand["traffic_potential"]),
                                     ("cpc_cents", cand["cpc_cents"]),
                                     ("parent_topic", cand["parent_topic"])):
                        if not prev.get(key) and val:
                            prev[key] = val
                    continue
                g = cand["growth_rate"] or {}
                found[kw] = {
                    "keyword": kw, "country": country,
                    "volume": cand["volume"], "difficulty": cand["difficulty"],
                    "traffic_potential": cand["traffic_potential"],
                    "cpc_cents": cand["cpc_cents"], "parent_topic": cand["parent_topic"],
                    "source_seed": seed,
                    "growth_3m": g.get("months_3"), "growth_6m": g.get("months_6"),
                    "growth_12m": g.get("months_12"),
                }

    # Transport guard (spec workflow 07): zero across ALL seeds is a FAILURE, not a success.
    if ok_calls == 0:
        job_fail(job_id, "Every Ahrefs call failed — " + (warnings[0] if warnings else "check connector approval."))
        return
    if not found:
        with cross_session_scope() as s:
            s.add(SBTrendingScan(country=country, seeds_used=seeds, keywords_found=0,
                                 new_keywords=0, status="failed", completed_at=_now(),
                                 note="Scan returned 0 keywords across all seeds — treated as a failure."))
        job_fail(job_id, "Scan returned 0 keywords across ALL seeds. Treating as a transport failure, "
                         "not an empty result — check the Ahrefs connector and filter thresholds.")
        return

    job_progress(job_id, f"Merging {len(found)} keywords into the bank…")
    new_count = 0
    with cross_session_scope() as s:
        for kw, data in found.items():
            existing = s.get(SBTrendingKeyword, (kw, country))
            if existing is None:
                new_count += 1
            stmt = pg_insert(SBTrendingKeyword.__table__).values(
                first_seen_at=_now(), last_seen_at=_now(), **data)
            # COALESCE growth so a later null pull never wipes a known value.
            stmt = stmt.on_conflict_do_update(
                index_elements=["keyword", "country"],
                set_={
                    "volume": stmt.excluded.volume,
                    "difficulty": func.coalesce(stmt.excluded.difficulty, SBTrendingKeyword.difficulty),
                    "traffic_potential": func.coalesce(stmt.excluded.traffic_potential, SBTrendingKeyword.traffic_potential),
                    "cpc_cents": func.coalesce(stmt.excluded.cpc_cents, SBTrendingKeyword.cpc_cents),
                    "parent_topic": func.coalesce(stmt.excluded.parent_topic, SBTrendingKeyword.parent_topic),
                    "growth_3m": func.coalesce(stmt.excluded.growth_3m, SBTrendingKeyword.growth_3m),
                    "growth_6m": func.coalesce(stmt.excluded.growth_6m, SBTrendingKeyword.growth_6m),
                    "growth_12m": func.coalesce(stmt.excluded.growth_12m, SBTrendingKeyword.growth_12m),
                    "source_seed": stmt.excluded.source_seed,
                    "last_seen_at": _now(),
                })
            s.execute(stmt)

    # Backfill growth for bank rows still missing it.
    filled = _backfill_growth(job_id, country)
    # Annotate where the target blog already ranks.
    annotated = _annotate_blog_rank(job_id, settings, country)

    with cross_session_scope() as s:
        s.add(SBTrendingScan(country=country, seeds_used=seeds, keywords_found=len(found),
                             new_keywords=new_count, completed_at=_now(),
                             note=f"{len(warnings)} warning(s); growth backfilled {filled}; "
                                  f"blog-rank annotated {annotated}"))

    job_done(job_id, {"found": len(found), "new": new_count, "warnings": warnings,
                      "growth_backfilled": filled, "blog_ranked": annotated},
             f"{len(found)} keywords ({new_count} new) merged into the bank")


def _backfill_growth(job_id: str, country: str, cap: int = 400) -> int:
    with cross_session_scope() as s:
        rows = (s.query(SBTrendingKeyword.keyword)
                .filter(SBTrendingKeyword.country == country)
                .filter(or_(SBTrendingKeyword.growth_3m.is_(None),
                            SBTrendingKeyword.difficulty.is_(None),
                            SBTrendingKeyword.traffic_potential.is_(None),
                            SBTrendingKeyword.traffic_potential == 0))
                .limit(cap).all())
    kws = [r[0] for r in rows]
    if not kws:
        return 0
    filled = 0
    for i in range(0, len(kws), 100):
        batch = kws[i:i + 100]
        job_progress(job_id, f"Backfilling growth: {i}/{len(kws)}…")
        try:
            res = ahrefs("ahrefs_keywords_explorer.keywords_overview_by_terms_export", {
                "keywords": batch, "country": country, "with_position": False,
                "limit": max(1, len(batch)), "order_by": "volume", "direction": "desc",
            })
        except Exception:  # noqa: BLE001
            continue
        with cross_session_scope() as s:
            for row in res.get("records") or []:
                kw = (row.get("keyword") or "").strip().lower()
                rec = s.get(SBTrendingKeyword, (kw, country)) if kw else None
                if not rec:
                    continue
                cand = normalize_ke_row(row)
                g = cand["growth_rate"] or {}
                touched = False
                if g.get("months_3") is not None and rec.growth_3m is None:
                    rec.growth_3m = g["months_3"]; touched = True
                if g.get("months_6") is not None and rec.growth_6m is None:
                    rec.growth_6m = g["months_6"]; touched = True
                if g.get("months_12") is not None and rec.growth_12m is None:
                    rec.growth_12m = g["months_12"]; touched = True
                if rec.difficulty is None and cand["difficulty"] is not None:
                    rec.difficulty = cand["difficulty"]; touched = True
                if not rec.traffic_potential and cand["traffic_potential"]:
                    rec.traffic_potential = cand["traffic_potential"]; touched = True
                if not rec.cpc_cents and cand["cpc_cents"]:
                    rec.cpc_cents = cand["cpc_cents"]; touched = True
                if not rec.parent_topic and cand["parent_topic"]:
                    rec.parent_topic = cand["parent_topic"]; touched = True
                if touched:
                    filled += 1
    return filled


def _annotate_blog_rank(job_id: str, settings: dict, country: str, cap: int = 500) -> int:
    target = (settings.get("target_site") or "").strip()
    if not target:
        return 0
    with cross_session_scope() as s:
        kws = [r[0] for r in s.query(SBTrendingKeyword.keyword)
               .filter(SBTrendingKeyword.country == country).limit(cap).all()]
    if not kws:
        return 0
    hits = 0
    for i in range(0, len(kws), 100):
        batch = kws[i:i + 100]
        job_progress(job_id, f"Checking where {target} ranks: {i}/{len(kws)}…")
        try:
            res = ahrefs("ahrefs_keywords_explorer.keywords_overview_by_page_or_domain", {
                "target": target, "mode": "domain", "country": country,
                "keywords": batch, "include_related_keywords": False,
                "limit": max(1, len(batch)),
            })
        except Exception:  # noqa: BLE001
            continue
        with cross_session_scope() as s:
            for row in res.get("records") or []:
                kw = (row.get("keyword") or "").strip().lower()
                rec = s.get(SBTrendingKeyword, (kw, country)) if kw else None
                if not rec:
                    continue
                pos = row.get("top_position")
                if pos is not None:
                    rec.blog_position = pos
                    rec.blog_url = row.get("top_url") or ""
                    hits += 1
    return hits


def suggest_seeds(limit: int = 6) -> list[str]:
    """Optional: propose NEW seeds from recently discovered high-volume keywords."""
    with cross_session_scope() as s:
        rows = (s.query(SBTrendingKeyword.keyword, SBTrendingKeyword.volume)
                .order_by(SBTrendingKeyword.first_seen_at.desc())
                .limit(120).all())
        existing = {r[0].lower() for r in s.query(SBTrendingSeed.seed).all()}
    if not rows:
        return []
    sample = sorted(rows, key=lambda r: -(r[1] or 0))[:60]
    listing = "\n".join(f"- {k} ({v}/mo)" for k, v in sample)
    try:
        out = chat([
            {"role": "system", "content": "You propose short seed topics for keyword research. "
                                          "Return JSON only: {\"seeds\": [\"...\"]}"},
            {"role": "user", "content": f"Recently discovered rising keywords:\n{listing}\n\n"
                                        f"Propose up to {limit} NEW seed topics (1-3 words each) that would "
                                        f"expand coverage of these themes. Avoid duplicating them verbatim."},
        ], model=CHEAP_MODEL, json_mode=True, max_tokens=400)
        seeds = json.loads(re.sub(r"^```json|```$", "", out.strip(), flags=re.M)).get("seeds") or []
    except Exception:  # noqa: BLE001
        return []
    return [str(x).strip().lower() for x in seeds
            if str(x).strip() and str(x).strip().lower() not in existing][:limit]


# ===========================================================================
# Workflow 08 — Topics (semantic clustering)
# ===========================================================================

SITEMAP_SKIP = re.compile(r"(changelog|announcement|release-note|press|/tag/|/category/|/author/|/page/\d+)", re.I)
CHUNK_CHARS = 2000


class FetchBlocked(RuntimeError):
    """The workspace firewall blocked this host — actionable, not a generic failure."""


def _fetch_text(url: str, max_chars: int = 40000, timeout: int = 30,
                raise_blocked: bool = False) -> str:
    """Fetch page body text via the web-fetch skill (server-side; never the browser)."""
    try:
        # --no-cache is REQUIRED here: the shared /tmp/web-fetch-cache is owned by the
        # agent user, and console-http (running as `console`) crashes with a
        # PermissionError inside read_cache before it can report anything useful.
        out = subprocess.run(
            ["python3", "/opt/letaido/agent/skills/web-fetch/scripts/fetch.py",
             "--no-cache", "--max-length", str(max_chars), url],
            capture_output=True, text=True, timeout=timeout)
        raw = out.stdout or ""
        err = out.stderr or ""
        if raise_blocked and "blocked by the workspace firewall" in err:
            from urllib.parse import urlparse
            raise FetchBlocked(urlparse(url).netloc)
        if "all sources failed" in err or not raw.strip():
            return ""
        parts = raw.split("\n---\n", 1)
        return (parts[1] if len(parts) > 1 else raw).strip()[:max_chars]
    except FetchBlocked:
        raise
    except Exception:  # noqa: BLE001
        return ""


def _extract_urls(raw: str) -> list[str]:
    """Pull URLs out of a sitemap however it arrives.

    The fetch utility converts XML to markdown, so <loc> tags are often already
    stripped — match both the raw tag and bare URLs in the text.
    """
    locs = re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", raw)
    if not locs:
        # The fetch utility strips XML tags, often leaving URLs butted together with
        # no whitespace — stop each match at the next scheme rather than at whitespace.
        locs = re.findall(r"https?://(?:(?!https?://)[^\s<>\"')\]])+", raw)
    out, seen = [], set()
    for u in locs:
        u = u.rstrip(".,)\"'")
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


LOCALE_RE = re.compile(r"^/([a-z]{2}(?:-[A-Za-z]{2,4})?)/")


def _locale_of(url: str) -> str:
    """First path segment when it looks like a language code ('en', 'zh-CN'), else ''."""
    from urllib.parse import urlparse
    m = LOCALE_RE.match(urlparse(url).path or "")
    return m.group(1) if m else ""


def _sitemap_urls(domain: str, cap: int = 120, only_locale: str = "") -> list[str]:
    """Resolve a domain (or domain+path prefix) to a list of crawlable page URLs."""
    raw_target = domain.strip().rstrip("/")
    if not raw_target.startswith("http"):
        raw_target = "https://" + raw_target
    from urllib.parse import urlparse
    parsed = urlparse(raw_target)
    root = f"{parsed.scheme}://{parsed.netloc}"
    prefix = raw_target if parsed.path.strip("/") else ""

    candidates = [f"{root}/sitemap.xml", f"{root}/sitemap_index.xml",
                  f"{root}/post-sitemap.xml", f"{root}/sitemap-index.xml"]
    if prefix:
        candidates.insert(0, f"{prefix}/sitemap.xml")

    pages: list[str] = []
    blocked_host = ""
    for sm in candidates:
        try:
            raw = _fetch_text(sm, max_chars=400000, timeout=40, raise_blocked=True)
        except FetchBlocked as fb:
            blocked_host = str(fb)
            break
        if not raw:
            continue
        found = _extract_urls(raw)
        # Follow nested sitemap indexes, preferring ones matching our path prefix.
        nested = [u for u in found if u.endswith(".xml") and u != sm]
        if prefix:
            nested.sort(key=lambda u: 0 if prefix.rstrip("/") in u else 1)
        for nu in nested[:8]:
            pages.extend(u for u in _extract_urls(_fetch_text(nu, max_chars=400000, timeout=40))
                         if not u.endswith(".xml"))
        pages.extend(u for u in found if not u.endswith(".xml"))
        if only_locale:
            # A multilingual help centre sitemap interleaves translations of the SAME
            # article. Clustering those together inflates cluster sizes and makes the
            # topic map meaningless, so restrict to one locale.
            loc = only_locale.lower()
            kept = [u for u in pages if _locale_of(u).lower() == loc]
            if kept:
                pages = kept
        pages = [u for u in pages
                 if parsed.netloc in u
                 and (not prefix or prefix.rstrip("/") in u)
                 and not SITEMAP_SKIP.search(u)]
        if len(pages) >= 5:
            break

    if blocked_host and not pages:
        raise FetchBlocked(blocked_host)

    seen, out = set(), []
    for u in pages:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out[:cap]


def _norm(vec: list[float]) -> list[float]:
    n = math.sqrt(sum(x * x for x in vec)) or 1.0
    return [x / n for x in vec]


def _mean(vecs: list[list[float]]) -> list[float]:
    if not vecs:
        return []
    dim = len(vecs[0])
    return [sum(v[i] for v in vecs) / len(vecs) for i in range(dim)]


def _cos(a: list[float], b: list[float]) -> float:
    return sum(x * y for x, y in zip(a, b))


def _kmeans(vecs: list[list[float]], k: int, iters: int = 25, seed: int = 7):
    rnd = random.Random(seed)
    centroids = [list(v) for v in rnd.sample(vecs, k)]
    labels = [0] * len(vecs)
    for _ in range(iters):
        moved = False
        for i, v in enumerate(vecs):
            best, bl = -2.0, 0
            for c, cen in enumerate(centroids):
                sim = _cos(v, cen)
                if sim > best:
                    best, bl = sim, c
            if labels[i] != bl:
                labels[i], moved = bl, True
        groups = defaultdict(list)
        for i, l in enumerate(labels):
            groups[l].append(vecs[i])
        for c in range(k):
            if groups[c]:
                centroids[c] = _norm(_mean(groups[c]))
        if not moved:
            break
    return labels, centroids


def _silhouette(vecs, labels, centroids) -> float:
    """Cheap centroid-based silhouette proxy: (b - a) / max(a, b) using centroid sims."""
    if len(set(labels)) < 2:
        return -1.0
    total = 0.0
    for v, l in zip(vecs, labels):
        a = 1 - _cos(v, centroids[l])
        b = min((1 - _cos(v, c)) for i, c in enumerate(centroids) if i != l)
        total += (b - a) / max(a, b, 1e-9)
    return total / len(vecs)


def run_topics_scan(job_id: str, domain: str, settings: dict, enrich: bool = True,
                    locale: str = "") -> None:
    domain = (domain or settings.get("target_site") or "").strip()
    if not domain:
        job_fail(job_id, "No domain given and no target_site configured.")
        return

    with cross_session_scope() as s:
        st = s.get(SBTopicScanState, domain) or SBTopicScanState(domain=domain)
        st.status, st.step = "running", "Collecting URLs from sitemap…"
        s.merge(st)

    job_progress(job_id, "Collecting URLs from sitemap…")
    try:
        urls = _sitemap_urls(domain, only_locale=locale)
    except FetchBlocked as fb:
        msg = (f"The workspace firewall blocks outbound requests to {fb}, so the crawl "
               f"can't read its sitemap. Ask in chat to approve access to {fb}, then re-run.")
        with cross_session_scope() as s:
            st = s.get(SBTopicScanState, domain)
            if st:
                st.status, st.step = "failed", "firewall blocked"
        job_fail(job_id, msg)
        return
    if len(urls) < 5:
        job_fail(job_id, f"Only found {len(urls)} crawlable URLs for {domain} — need at least 5. "
                         "Check the domain has a reachable sitemap.")
        with cross_session_scope() as s:
            st = s.get(SBTopicScanState, domain)
            if st:
                st.status, st.step = "failed", f"only {len(urls)} urls"
        return

    # Crawl + embed is the slow part (~3 min for 120 pages) and the Console dev server
    # restarts whenever ANY app file changes — including other people's apps. Losing the
    # whole crawl to someone else's edit is unacceptable, so each page is checkpointed to
    # scrapbook_topic_pages as it completes and a re-run resumes instead of restarting.
    done_pages: dict[str, dict] = {}
    with cross_session_scope() as s:
        for row in s.query(SBTopicPage).filter(SBTopicPage.domain == domain).all():
            if row.page_vector:
                done_pages[row.url] = {"url": row.url, "title": row.title,
                                       "chars": row.chars, "vector": row.page_vector}
    if done_pages:
        job_progress(job_id, f"Resuming — {len(done_pages)} pages already embedded.")

    pages: list[dict] = list(done_pages.values())
    todo = [u for u in urls if u not in done_pages]
    for i, url in enumerate(todo, 1):
        if i % 5 == 1:
            job_progress(job_id,
                         f"Fetching + embedding pages: {len(done_pages) + i}/{len(urls)}…")
        text = _fetch_text(url, max_chars=24000)
        if len(text) < 400:
            continue
        title = ""
        m = re.search(r"^#\s+(.+)$", text, re.M)
        if m:
            title = m.group(1).strip()[:300]
        chunks = [text[j:j + CHUNK_CHARS] for j in range(0, min(len(text), CHUNK_CHARS * 8), CHUNK_CHARS)]
        try:
            vecs = [_norm(v) for v in embed(chunks)]
        except Exception as exc:  # noqa: BLE001
            job_progress(job_id, f"Embedding failed on {url}: {exc}")
            continue
        if not vecs:
            continue
        rec = {"url": url, "title": title or url, "chars": len(text),
               "vector": _norm(_mean(vecs))}
        pages.append(rec)
        # Checkpoint immediately — survives a restart mid-crawl.
        with cross_session_scope() as s:
            existing = (s.query(SBTopicPage)
                        .filter(SBTopicPage.domain == domain, SBTopicPage.url == url).first())
            if existing:
                existing.title, existing.chars = rec["title"], rec["chars"]
                existing.page_vector = rec["vector"]
            else:
                s.add(SBTopicPage(domain=domain, url=url, title=rec["title"],
                                  chars=rec["chars"], page_vector=rec["vector"],
                                  bucket=""))

    if len(pages) < 5:
        job_fail(job_id, f"Only {len(pages)} pages yielded usable text — too few to cluster.")
        return

    job_progress(job_id, f"Clustering {len(pages)} pages…")
    vecs = [p["vector"] for p in pages]
    centre = _norm(_mean(vecs))
    for p in pages:
        p["distance"] = 1 - _cos(p["vector"], centre)

    dists = [p["distance"] for p in pages]
    mean_d = sum(dists) / len(dists)
    sd = math.sqrt(sum((d - mean_d) ** 2 for d in dists) / len(dists)) or 1e-9
    for p in pages:
        d = p["distance"]
        if d < mean_d - sd / 2:
            p["bucket"] = "core"
        elif d < mean_d:
            p["bucket"] = "near"
        elif d < mean_d + sd / 2:
            p["bucket"] = "mid"
        else:
            p["bucket"] = "far"

    best = None
    for k in range(2, min(9, len(pages) // 3 + 2)):
        labels, centroids = _kmeans(vecs, k)
        score = _silhouette(vecs, labels, centroids)
        if best is None or score > best[0]:
            best = (score, k, labels, centroids)
    _score, k, labels, centroids = best

    groups: dict[int, list[int]] = defaultdict(list)
    for i, l in enumerate(labels):
        groups[l].append(i)

    job_progress(job_id, f"Labelling {k} clusters…")
    cluster_labels: dict[int, str] = {}
    for c, idxs in groups.items():
        titles = [pages[i]["title"][:120] for i in sorted(idxs, key=lambda i: pages[i]["distance"])[:8]]
        try:
            label = chat([
                {"role": "system", "content": "You name topical clusters of blog pages. "
                                              "Reply with a 2-5 word label only, no quotes."},
                {"role": "user", "content": "Representative page titles:\n" + "\n".join(f"- {t}" for t in titles)},
            ], model=CHEAP_MODEL, max_tokens=30, temperature=0.2).strip().strip('"')[:80]
        except Exception:  # noqa: BLE001
            label = f"Cluster {c + 1}"
        cluster_labels[c] = label or f"Cluster {c + 1}"

    enriched = 0
    if enrich:
        job_progress(job_id, "Enriching pages with organic metrics…")
        enriched = _enrich_pages(pages)

    buckets = Counter(p["bucket"] for p in pages)
    spread = sd / (mean_d or 1e-9)
    verdict = "tight" if spread < 0.25 else "focused" if spread < 0.45 else "diffuse"
    concentration = {
        "pages": len(pages), "mean_distance": round(mean_d, 4), "sd": round(sd, 4),
        "buckets": dict(buckets), "verdict": verdict,
        "silhouette": round(_score, 3), "k": k,
        "histogram": _histogram(dists),
        "enriched": enriched,
    }

    with cross_session_scope() as s:
        s.query(SBTopicPage).filter(SBTopicPage.domain == domain).delete()
        s.query(SBTopicCluster).filter(SBTopicCluster.domain == domain).delete()
        s.flush()
        for c, idxs in groups.items():
            avg = sum(pages[i]["distance"] for i in idxs) / len(idxs)
            s.add(SBTopicCluster(domain=domain, label=cluster_labels[c], size=len(idxs),
                                 centroid=[round(x, 5) for x in centroids[c]],
                                 sample_urls=[pages[i]["url"] for i in idxs[:6]],
                                 avg_distance=avg))
        s.flush()
        rows = {cl.label: cl.id for cl in s.query(SBTopicCluster).filter(SBTopicCluster.domain == domain).all()}
        for i, p in enumerate(pages):
            s.add(SBTopicPage(domain=domain, url=p["url"], title=p["title"], chars=p["chars"],
                              page_vector=[round(x, 5) for x in p["vector"]],
                              distance=p["distance"], bucket=p["bucket"],
                              cluster_id=rows.get(cluster_labels[labels[i]]),
                              traffic=p.get("traffic"), refdomains=p.get("refdomains"), ur=p.get("ur")))
        st = s.get(SBTopicScanState, domain) or SBTopicScanState(domain=domain)
        st.status, st.step, st.concentration = "completed", "done", concentration
        s.merge(st)

    job_done(job_id, {"domain": domain, "clusters": len(groups), **concentration},
             f"{len(pages)} pages → {len(groups)} clusters ({verdict})")


def _histogram(dists: list[float], bins: int = 12) -> list[dict]:
    lo, hi = min(dists), max(dists)
    span = (hi - lo) or 1e-9
    counts = [0] * bins
    for d in dists:
        idx = min(bins - 1, int((d - lo) / span * bins))
        counts[idx] += 1
    return [{"from": round(lo + span * i / bins, 4), "to": round(lo + span * (i + 1) / bins, 4),
             "count": c} for i, c in enumerate(counts)]


def _enrich_pages(pages: list[dict]) -> int:
    """Attach organic traffic / refdomains / UR via Ahrefs batch analysis."""
    hits = 0
    for i in range(0, len(pages), 100):
        batch = pages[i:i + 100]
        try:
            res = ahrefs("ahrefs_batch_analysis.ba_table", {
                "targets": [{"target": p["url"], "mode": "exact", "protocol": "both"} for p in batch],
            })
        except Exception:  # noqa: BLE001
            continue
        by_url = {}
        for row in res.get("records") or []:
            key = (row.get("target") or row.get("url") or "").rstrip("/")
            if key:
                by_url[key] = row
        for p in batch:
            row = by_url.get(p["url"].rstrip("/"))
            if not row:
                continue
            p["traffic"] = row.get("org_traffic") or row.get("traffic")
            p["refdomains"] = row.get("refdomains")
            p["ur"] = row.get("ur") or row.get("url_rating")
            hits += 1
    return hits


# ===========================================================================
# Workflow 09 — Reddit Radar
# ===========================================================================

REDDIT_UA = "letaido-scrapbook/1.0"


def _reddit_json(path: str, params: dict) -> dict:
    """Public JSON endpoint via the fetch utility (no Reddit credentials required)."""
    import urllib.parse
    url = f"https://www.reddit.com{path}?" + urllib.parse.urlencode({**params, "raw_json": 1})
    raw = _fetch_text(url, max_chars=400000, timeout=35)
    if not raw:
        return {}
    m = re.search(r"\{.*\}", raw, re.S)
    if not m:
        return {}
    try:
        return json.loads(m.group(0))
    except Exception:  # noqa: BLE001
        return {}


def run_radar_scan(job_id: str, settings: dict) -> None:
    subs = settings.get("radar_subreddits") or []
    queries = settings.get("radar_queries") or []
    if not subs and not queries:
        job_fail(job_id, "No subreddits or queries configured for the radar.")
        return

    fetched, warnings = 0, []
    with cross_session_scope() as s:
        for i, sub in enumerate(subs, 1):
            job_progress(job_id, f"r/{sub} ({i}/{len(subs)})…")
            data = _reddit_json(f"/r/{sub}/new.json", {"limit": 50})
            children = ((data.get("data") or {}).get("children") or [])
            if not children:
                warnings.append(f"r/{sub}: no posts returned (rate-limited or private?)")
            for ch in children:
                fetched += _upsert_reddit(s, ch.get("data") or {}, f"r/{sub}")
        for j, q in enumerate(queries, 1):
            job_progress(job_id, f"Search '{q}' ({j}/{len(queries)})…")
            data = _reddit_json("/search.json", {"q": q, "sort": "new", "limit": 50, "t": "month"})
            children = ((data.get("data") or {}).get("children") or [])
            if not children:
                warnings.append(f"query '{q}': no results")
            for ch in children:
                fetched += _upsert_reddit(s, ch.get("data") or {}, q)

    report = _build_radar_report(job_id, warnings)
    job_done(job_id, {"fetched": fetched, "warnings": warnings, "report_id": report},
             f"{fetched} new posts stored; report generated")


def _upsert_reddit(s, d: dict, matched: str) -> int:
    pid = d.get("id")
    if not pid:
        return 0
    if s.get(SBRedditPost, pid):
        return 0
    created = d.get("created_utc")
    s.add(SBRedditPost(
        id=pid, subreddit=d.get("subreddit") or "", title=(d.get("title") or "")[:2000],
        url=d.get("url") or "", permalink="https://www.reddit.com" + (d.get("permalink") or ""),
        body=(d.get("selftext") or "")[:8000], author=d.get("author") or "",
        score=int(d.get("score") or 0), num_comments=int(d.get("num_comments") or 0),
        created_utc=datetime.fromtimestamp(created, tz=timezone.utc) if created else None,
        matched_query=matched))
    return 1


def _build_radar_report(job_id: str, warnings: list[str]) -> int | None:
    since = _now() - timedelta(days=7)
    week_start = since.date().isoformat()
    with cross_session_scope() as s:
        posts = (s.query(SBRedditPost)
                 .filter(SBRedditPost.created_utc >= since)
                 .order_by(SBRedditPost.score.desc()).limit(80).all())
        rows = [{"title": p.title, "sub": p.subreddit, "score": p.score,
                 "comments": p.num_comments, "url": p.permalink,
                 "body": (p.body or "")[:600], "q": p.matched_query} for p in posts]
        by_sub = dict(Counter(p.subreddit for p in posts))
        by_query = dict(Counter(p.matched_query for p in posts))

    if not rows:
        summary = ("## Quiet week\n\nNo new posts matched the configured subreddits or queries "
                   "in the last 7 days. The corpus is unchanged.")
    else:
        job_progress(job_id, "Summarising the week…")
        listing = "\n".join(
            f"- [{r['score']}▲ {r['comments']}💬] r/{r['sub']} — {r['title']}\n  {r['body'][:240]}"
            for r in rows[:50])
        try:
            summary = chat([
                {"role": "system", "content":
                    "You write a weekly Reddit digest for an SEO/AI-search team. Markdown. "
                    "Sections: ## Themes (3-5 bullets, each naming the driving threads), "
                    "## Notable threads (title + why it matters + link), ## Sentiment (2-3 sentences). "
                    "Ground every claim in the supplied posts; never invent threads."},
                {"role": "user", "content": f"Posts from the last 7 days:\n{listing}"},
            ], model=CHAT_MODEL, max_tokens=1600, temperature=0.3)
        except Exception as exc:  # noqa: BLE001
            summary = f"## Report generation failed\n\n{exc}\n\n{len(rows)} posts were collected."

    stats = {"posts": len(rows), "by_subreddit": by_sub, "by_query": by_query,
             "top": rows[:10], "warnings": warnings}
    with cross_session_scope() as s:
        rep = SBRadarReport(week_start=week_start, summary_md=summary, stats=stats)
        s.add(rep)
        s.flush()
        return rep.id


# ===========================================================================
# Workflow 10 — Growth Scanner
# ===========================================================================

MAX_ANCHORS = 16


def run_category_scan(job_id: str, primary_seed: str, related_seeds: list[str],
                      strip_brands: bool, settings: dict, category_id: str | None = None,
                      mode: str = "create") -> None:
    """mode: create | refresh (same anchors) | reseed (re-discover)."""
    country = (settings.get("target_country") or "us").lower()
    filters = settings.get("filters") or {}
    brands = brand_tokens(settings)

    with cross_session_scope() as s:
        cat = s.get(SBCategory, category_id) if category_id else None
        excluded = list(cat.excluded_topics) if cat else []
        if mode == "refresh" and cat:
            anchors = list(cat.anchors)
            relevant = list(cat.relevant_topic_labels)
            primary_seed = cat.primary_seed
            related_seeds = list(cat.related_seeds)
            strip_brands = cat.strip_brands
        else:
            anchors, relevant = [], []

    seeds = [s_ for s_ in [primary_seed, *(related_seeds or [])] if s_]
    if not seeds:
        job_fail(job_id, "A primary seed is required.")
        return

    if mode != "refresh" or not anchors:
        job_progress(job_id, "Discovering parent-topic clusters…")
        parents: Counter = Counter()
        for seed in seeds:
            try:
                res = ahrefs("ahrefs_keywords_explorer.ideas_by_terms_export", {
                    "seed_keywords": [seed], "country": country, "mode": "matching_terms",
                    "limit": 100, "order_by": "volume", "direction": "desc",
                })
            except Exception as exc:  # noqa: BLE001
                job_progress(job_id, f"seed '{seed}' failed: {exc}")
                continue
            for row in res.get("records") or []:
                pt = (row.get("parent_keyword") or row.get("parent_topic") or "").strip()
                if pt:
                    parents[pt.lower()] += (row.get("volume") or 0)
        if not parents:
            job_fail(job_id, "No parent topics discovered — every Ahrefs ideas call failed or returned nothing.")
            return
        anchors = [p for p, _ in parents.most_common(MAX_ANCHORS)]
        relevant = _vet_clusters(anchors, primary_seed, strip_brands)

    anchors = [a for a in anchors if a not in excluded]
    fence = {a for a in (relevant or anchors) if a not in excluded}
    if not fence:
        job_fail(job_id, "Every discovered cluster was excluded — nothing left to scan.")
        return

    job_progress(job_id, f"Building keyword pool over {len(fence)} clusters…")
    pool: dict[str, dict] = {}
    for i, anchor in enumerate(sorted(fence), 1):
        job_progress(job_id, f"Cluster {i}/{len(fence)}: '{anchor}'…")
        try:
            res = ahrefs("ahrefs_keywords_explorer.ideas_by_terms_export", {
                "seed_keywords": [anchor], "country": country, "mode": "matching_terms",
                "limit": 100, "order_by": "volume", "direction": "desc",
            })
        except Exception:  # noqa: BLE001
            continue
        for row in res.get("records") or []:
            kw = (row.get("keyword") or "").strip().lower()
            pt = (row.get("parent_keyword") or row.get("parent_topic") or "").strip().lower()
            # Server-side parent filters are unreliable — fence client-side on parent_topic.
            if not kw or pt not in fence:
                continue
            cand = normalize_ke_row(row)
            if not keep_keyword(kw, cand, filters, brands, apply_growth=True):
                continue
            g = cand["growth_rate"] or {}
            pool[kw] = {"keyword": kw, "volume": cand["volume"], "difficulty": cand["difficulty"],
                        "parent_topic": pt, "growth_3m": g.get("months_3"),
                        "traffic_potential": cand["traffic_potential"]}

    job_progress(job_id, "Ranking the domains that own these clusters…")
    domains = _rank_domains(sorted(fence), country)

    snapshot = {"pulled_at": _now().isoformat(),
                "domains": domains,
                "keywords": sorted(pool.values(), key=lambda d: -(d["volume"] or 0))[:300],
                "cluster_count": len(fence), "keyword_count": len(pool)}

    with cross_session_scope() as s:
        cat = s.get(SBCategory, category_id) if category_id else None
        if cat is None:
            cat = SBCategory(name=primary_seed.title(), primary_seed=primary_seed)
            s.add(cat)
        cat.primary_seed = primary_seed
        cat.related_seeds = related_seeds or []
        cat.anchors = anchors
        cat.relevant_topic_labels = sorted(fence)
        cat.strip_brands = strip_brands
        cat.excluded_topics = excluded
        cat.snapshots = ([*(cat.snapshots or []), snapshot])[-12:]
        s.flush()
        cid = cat.id

    job_done(job_id, {"category_id": cid, "clusters": len(fence), "keywords": len(pool),
                      "domains": len(domains)},
             f"{len(fence)} clusters, {len(pool)} keywords, {len(domains)} domains")


def _vet_clusters(anchors: list[str], seed: str, strip_brands: bool) -> list[str]:
    """LLM-vet which clusters are genuinely on-topic; optionally drop brand-named ones."""
    if not anchors:
        return []
    rule = ("Also DROP clusters that are a specific brand/product name; keep generic category clusters."
            if strip_brands else "Keep brand-named clusters.")
    try:
        out = chat([
            {"role": "system", "content": "You vet topical clusters for a market-tracking scan. "
                                          "Return JSON only: {\"keep\": [\"...\"]} using the exact input strings."},
            {"role": "user", "content": f"Category seed: '{seed}'\n{rule}\n\nClusters:\n"
                                        + "\n".join(f"- {a}" for a in anchors)},
        ], model=CHEAP_MODEL, json_mode=True, max_tokens=800)
        keep = json.loads(re.sub(r"^```json|```$", "", out.strip(), flags=re.M)).get("keep") or []
        kept = [a for a in anchors if a in {str(k).strip().lower() for k in keep}]
        return kept or anchors
    except Exception:  # noqa: BLE001
        return anchors


def _rank_domains(anchors: list[str], country: str) -> list[dict]:
    """traffic_share + coverage + authority_score = share × sqrt(coverage)."""
    traffic: dict[str, float] = defaultdict(float)
    coverage: dict[str, set] = defaultdict(set)
    for anchor in anchors:
        try:
            res = ahrefs("ahrefs_keywords_explorer.traffic_by_domains", {
                "seed_keywords": [anchor], "country": country, "limit": 25,
            })
        except Exception:  # noqa: BLE001
            continue
        for row in res.get("records") or []:
            dom = (row.get("domain") or "").strip().lower()
            if not dom:
                continue
            traffic[dom] += float(row.get("traffic") or 0)
            coverage[dom].add(anchor)
    total = sum(traffic.values()) or 1.0
    out = []
    for dom, t in traffic.items():
        share = t / total
        cov = len(coverage[dom])
        out.append({"domain": dom, "traffic": round(t, 2),
                    "traffic_share": round(share, 5), "coverage": cov,
                    "authority_score": round(share * math.sqrt(cov), 5)})
    return sorted(out, key=lambda d: -d["authority_score"])[:40]
