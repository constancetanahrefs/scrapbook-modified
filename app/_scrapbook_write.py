"""Scrapbook — Write engines.

Workflows implemented here:
  11 Ideas               — keyword-data-LED headline/angle generation from saved scraps
  12 Example finder      — reference-doc corpus, embedding + full-text search
  13 Ahrefs weaver       — weave fetched Ahrefs data points into a draft as cited mentions

(Workflow 14, the Blog Refresh Engine, was extracted into its own standalone app —
applications/blog_refresh_engine.py + _bre_core.py + _bre_engine.py.)
"""

from __future__ import annotations

import time

import difflib
import hashlib
import json
import math
import re
from datetime import datetime, timezone
from typing import Optional

from src.db_cross import cross_session_scope

from applications._scrapbook_core import (
    CHAT_MODEL, CHEAP_MODEL, ahrefs, chat, embed, job_done, job_fail, job_progress,
)
from applications._scrapbook_models import SBExampleDoc, SBIdea, SBWeaverRun


def _now():
    return datetime.now(timezone.utc)


def _loose_json(txt: str) -> dict:
    t = re.sub(r"^```(?:json)?|```$", "", (txt or "").strip(), flags=re.M).strip()
    try:
        return json.loads(t)
    except Exception:  # noqa: BLE001
        m = re.search(r"\{.*\}", t, re.S)
        if m:
            try:
                return json.loads(m.group(0))
            except Exception:  # noqa: BLE001
                pass
    return {}


# ===========================================================================
# Workflow 11 — Ideas (keyword-data-LED)
# ===========================================================================

def run_ideas(job_id: str, scraps: list[dict], settings: dict) -> None:
    """scraps: [{id, title, content}] already selected by the caller."""
    if not scraps:
        job_fail(job_id, "No scraps to work from. Save some posts first.")
        return
    country = (settings.get("target_country") or "us").lower()

    # 1. Extract seed phrases from the scraps.
    job_progress(job_id, "Reading your scraps for seed topics…")
    corpus = "\n\n".join(f"[{i + 1}] {s.get('title', '')}\n{(s.get('content') or '')[:1500]}"
                         for i, s in enumerate(scraps[:20]))
    out = chat([
        {"role": "system", "content": "You extract short keyword-research seed phrases from saved content. "
                                      "Return JSON only: {\"seeds\": [\"...\"]} — 5 to 8 seeds, 1-3 words each."},
        {"role": "user", "content": corpus},
    ], model=CHEAP_MODEL, json_mode=True, max_tokens=400)
    seeds = [str(x).strip().lower() for x in (_loose_json(out).get("seeds") or []) if str(x).strip()][:8]
    if not seeds:
        job_fail(job_id, "Could not derive seed topics from the selected scraps.")
        return

    # 2. Build the candidate keyword pool — this is what GROUNDS every idea.
    pool: dict[str, dict] = {}
    ok = 0
    for i, seed in enumerate(seeds, 1):
        for mode in ("matching_terms", "matching_questions"):
            job_progress(job_id, f"Keyword pool {i}/{len(seeds)}: '{seed}'…")
            try:
                res = ahrefs("ahrefs_keywords_explorer.ideas_by_terms_export", {
                    "seed_keywords": [seed], "country": country, "mode": mode,
                    "limit": 60, "order_by": "volume", "direction": "desc",
                })
                ok += 1
            except Exception:  # noqa: BLE001
                continue
            for row in res.get("records") or []:
                kw = (row.get("keyword") or "").strip().lower()
                if not kw:
                    continue
                vol = row.get("volume") or 0
                is_question = bool(re.match(r"^(how|what|why|when|which|who|where|can|does|is|are|should)\b", kw))
                if vol < 20 and not is_question:
                    continue
                intents = [str(v).lower() for v in (row.get("intents") or [])]
                pool[kw] = {
                    "keyword": kw, "volume": vol, "difficulty": row.get("difficulty"),
                    "cpc": round(float(row.get("cpc") or 0), 2),
                    "traffic_potential": row.get("traffic_potential") or 0,
                    "intent": ", ".join(intents) or ("question" if is_question else ""),
                    "parent_topic": row.get("parent_keyword") or row.get("parent_topic"),
                    "seed": seed,
                }
    if ok == 0:
        job_fail(job_id, "Every Ahrefs keyword call failed — check the connector approval.")
        return
    if not pool:
        job_fail(job_id, "The keyword pool came back empty for these seeds.")
        return

    # 3. Compose ideas by PICKING keywords verbatim from the menu.
    job_progress(job_id, f"Composing ideas from {len(pool)} candidate keywords…")
    menu = sorted(pool.values(), key=lambda d: -(d["volume"] or 0))[:80]
    menu_txt = "\n".join(
        f"- {d['keyword']} | vol {d['volume']} | KD {d['difficulty']} | TP {d['traffic_potential']}"
        f" | intent {d['intent'] or 'n/a'}" for d in menu)
    out = chat([
        {"role": "system", "content":
            "You write article ideas that are LED by keyword data. Rules:\n"
            "- Every idea MUST pick 2-4 keywords VERBATIM from the candidate menu. Never invent a keyword.\n"
            "- Build the headline around the highest-volume keyword you picked, SEO-minded but human.\n"
            "- The angle is 1-2 sentences on the specific take, informed by the source scraps.\n"
            "Return JSON only: {\"ideas\":[{\"headline\":\"...\",\"angle\":\"...\",\"keywords\":[\"exact menu keyword\"]}]} "
            "— 6 to 10 ideas."},
        {"role": "user", "content": f"CANDIDATE KEYWORD MENU:\n{menu_txt}\n\nSOURCE SCRAPS:\n{corpus[:6000]}"},
    ], model=CHAT_MODEL, json_mode=True, max_tokens=2600, temperature=0.6)

    raw_ideas = _loose_json(out).get("ideas") or []
    if not raw_ideas:
        job_fail(job_id, "The model returned no usable ideas.")
        return

    # 4. Attach metrics LOCALLY from the pool — no extra Ahrefs round-trip, no invented numbers.
    saved = []
    src_ids = [s.get("id") for s in scraps if s.get("id")]
    with cross_session_scope() as s:
        for idea in raw_ideas:
            picked = []
            for kw in (idea.get("keywords") or []):
                d = pool.get(str(kw).strip().lower())
                if d:
                    picked.append({k: d[k] for k in
                                   ("keyword", "volume", "difficulty", "cpc",
                                    "intent", "parent_topic", "traffic_potential")})
            if not picked:
                continue  # ungrounded idea — drop it
            row = SBIdea(headline=(idea.get("headline") or "")[:400],
                         angle=(idea.get("angle") or "")[:1200],
                         keyword_metrics=picked, source_item_ids=src_ids)
            s.add(row)
            s.flush()
            saved.append({"id": row.id, "headline": row.headline, "angle": row.angle,
                          "keyword_metrics": picked})
    job_done(job_id, {"ideas": saved, "pool_size": len(pool), "seeds": seeds},
             f"{len(saved)} ideas grounded in {len(pool)} real keywords")


# ===========================================================================
# Workflow 12 — Example finder
# ===========================================================================

def search_examples(query: str, limit: int = 20) -> list[dict]:
    """Embedding search with a keyword-overlap fallback, plus a why-it-matched snippet."""
    q = (query or "").strip()
    with cross_session_scope() as s:
        docs = s.query(SBExampleDoc).order_by(SBExampleDoc.created_at.desc()).limit(500).all()
        rows = [{"id": d.id, "title": d.title, "url": d.url, "tags": list(d.tags or []),
                 "content": d.content or "", "embedding": d.embedding} for d in docs]
    if not q:
        return [_ex_out(r, 0.0, (r["content"] or "")[:280]) for r in rows[:limit]]

    qvec = None
    try:
        qvec = embed([q])[0]
    except Exception:  # noqa: BLE001
        pass

    scored = []
    terms = [t for t in re.findall(r"\w+", q.lower()) if len(t) > 2]
    for r in rows:
        score = 0.0
        if qvec and r["embedding"]:
            a, b = qvec, r["embedding"]
            dot = sum(x * y for x, y in zip(a, b))
            na = math.sqrt(sum(x * x for x in a)) or 1
            nb = math.sqrt(sum(x * x for x in b)) or 1
            score = dot / (na * nb)
        low = (r["title"] + " " + r["content"]).lower()
        hits = sum(low.count(t) for t in terms)
        score += min(hits, 20) * 0.01
        if score <= 0:
            continue
        scored.append((score, r, _snippet(r["content"], terms)))
    scored.sort(key=lambda t: -t[0])
    return [_ex_out(r, sc, sn) for sc, r, sn in scored[:limit]]


def _snippet(content: str, terms: list[str], width: int = 260) -> str:
    low = (content or "").lower()
    for t in terms:
        i = low.find(t)
        if i >= 0:
            start = max(0, i - width // 3)
            return ("…" if start else "") + content[start:start + width].strip() + "…"
    return (content or "")[:width]


def _ex_out(r: dict, score: float, snippet: str) -> dict:
    return {"id": r["id"], "title": r["title"], "url": r["url"], "tags": r["tags"],
            "score": round(score, 4), "snippet": snippet,
            "chars": len(r["content"] or "")}


def embed_example(doc_id: int) -> None:
    with cross_session_scope() as s:
        d = s.get(SBExampleDoc, doc_id)
        if not d or not (d.content or "").strip():
            return
        try:
            d.embedding = embed([f"{d.title}\n\n{d.content[:6000]}"])[0]
        except Exception:  # noqa: BLE001
            d.embedding = None


# ===========================================================================
# Workflow 13 — Ahrefs weaver
# ===========================================================================

def run_weaver(job_id: str, draft_md: str, target: str, keywords: list[str], settings: dict) -> None:
    draft = (draft_md or "").strip()
    if len(draft) < 80:
        job_fail(job_id, "Paste a draft of at least a paragraph.")
        return
    country = (settings.get("target_country") or "us").lower()
    target = (target or settings.get("target_site") or "").strip()

    # 1. Identify where a data point would strengthen the draft.
    job_progress(job_id, "Finding places a data point would help…")
    out = chat([
        {"role": "system", "content":
            "You find opportunities to back a draft with SEO data. Return JSON only: "
            "{\"keywords\":[\"...\"],\"domains\":[\"...\"]} — up to 8 keywords whose search demand, "
            "difficulty or growth would strengthen a claim, and up to 4 domains whose authority/traffic "
            "is worth citing. Use terms that actually appear in or are directly implied by the draft."},
        {"role": "user", "content": draft[:12000]},
    ], model=CHEAP_MODEL, json_mode=True, max_tokens=500)
    plan = _loose_json(out)
    kws = [str(k).strip().lower() for k in (keywords or plan.get("keywords") or []) if str(k).strip()][:10]
    doms = [str(d).strip().lower() for d in (plan.get("domains") or []) if str(d).strip()][:4]
    if target and target not in doms:
        doms.insert(0, target)

    # 2. Fetch the data — one batched pull, cached per request.
    job_progress(job_id, f"Fetching Ahrefs data for {len(kws)} keywords, {len(doms)} domains…")
    points: list[dict] = []
    if kws:
        try:
            res = ahrefs("ahrefs_keywords_explorer.keywords_overview_by_terms_export", {
                "keywords": kws, "country": country, "with_position": False,
                "limit": max(1, len(kws)), "order_by": "volume", "direction": "desc",
            })
            for row in res.get("records") or []:
                kw = (row.get("keyword") or "").strip().lower()
                if not kw:
                    continue
                points.append({
                    "kind": "keyword", "subject": kw,
                    "volume": row.get("volume"), "difficulty": row.get("difficulty"),
                    "traffic_potential": row.get("traffic_potential"),
                    "cpc": round(float(row.get("cpc") or 0), 2),
                    "growth_3m": row.get("growth_3mo"), "growth_12m": row.get("growth_12mo"),
                })
        except Exception as exc:  # noqa: BLE001
            job_progress(job_id, f"Keyword pull failed: {exc}")
    for dom in doms:
        try:
            res = ahrefs("ahrefs_site_explorer.domain_rating", {"target": dom, "protocol": "both"})
            recs = res.get("records") or []
            dr = (recs[0].get("domain_rating") if recs else None)
            res2 = ahrefs("ahrefs_site_explorer.metrics", {"target": dom, "mode": "domain",
                                                           "country": country, "protocol": "both"})
            r2 = (res2.get("records") or [{}])[0]
            points.append({"kind": "domain", "subject": dom, "domain_rating": dr,
                           "org_traffic": r2.get("org_traffic"), "org_keywords": r2.get("org_keywords")})
        except Exception:  # noqa: BLE001
            continue

    points = [p for p in points if any(v not in (None, 0) for k, v in p.items()
                                       if k not in ("kind", "subject"))]
    if not points:
        job_fail(job_id, "No Ahrefs data points came back, so there is nothing to weave. "
                         "Nothing was invented.")
        return

    # 3. Weave — additive, cited, never fabricated.
    job_progress(job_id, f"Weaving {len(points)} data points into the draft…")
    pts_txt = json.dumps(points, indent=1)
    woven = chat([
        {"role": "system", "content":
            "You add data points to a draft IN FLOW. Hard rules:\n"
            "- Use ONLY the supplied values. Never invent, round misleadingly, or extrapolate a number.\n"
            "- If a value is missing/null, SKIP that insertion entirely.\n"
            "- Each insertion carries an attribution phrase (e.g. 'according to Ahrefs data, ...').\n"
            "- Keep the author's voice. Insertions are additive — do NOT rewrite or restructure.\n"
            "- Wrap each inserted sentence/clause in {{+ ... +}} so the UI can highlight it.\n"
            "Return the full markdown draft only, no commentary."},
        {"role": "user", "content": f"AVAILABLE DATA POINTS (the only numbers you may use):\n{pts_txt}\n\n"
                                    f"DRAFT:\n{draft}"},
    ], model=CHAT_MODEL, max_tokens=6000, temperature=0.3)

    used = [p for p in points if str(p["subject"]) in woven.lower()]
    h = hashlib.sha256(draft.encode()).hexdigest()
    with cross_session_scope() as s:
        s.add(SBWeaverRun(input_hash=h, data_points=points, output_md=woven))
    job_done(job_id, {"output_md": woven, "data_points": points, "used": len(used),
                      "insertions": woven.count("{{+")},
             f"{woven.count('{{+')} data points woven in")
