# Porting Scrapbook outside Letaido — the APIs you need

Scrapbook was built on the [Letaido](https://letaido.com) platform, where Ahrefs,
the LLM and the database are reached through typed helper layers
(`src.connectors.invoke("ahrefs_…")`, `src.llm`, `src.db_cross`). If you're
rebuilding on your own stack you talk to the **public Ahrefs API v3**, an
OpenAI-compatible LLM, and a Postgres you own — directly.

This document maps **every external call** the app makes to its public
equivalent, so an agent (or a human) can recreate the app faithfully.

- **Ahrefs base URL:** `https://api.ahrefs.com/v3`
- **Ahrefs auth:** `Authorization: Bearer $AHREFS_API_KEY`
- **Ahrefs docs:** <https://docs.ahrefs.com/api/reference>
- Keywords Explorer + Site Explorer + Batch Analysis require a plan with API access.

> Scrapbook is a **multi-service** app. Ahrefs powers the research/monitoring/write
> tabs, but the capture side also uses an LLM, an embeddings model, a web-page
> fetcher, a LinkedIn scraper, the Ahrefs Firehose stream and Reddit. Each is
> mapped below.

---

## 1. Platform services to replace (the three imports)

Every module starts by importing Letaido helpers. Swap these for your own and the
rest of the code is plain Flask 3 + SQLAlchemy 2 + Pydantic v2 + Jinja2 + Tailwind
(CDN).

| Letaido import | What it does | Your equivalent |
|---|---|---|
| `from src.connectors import invoke as _connector_invoke` | Authenticated Ahrefs call; unwraps large/paginated payloads; retries | `requests.post`/`get` to `api.ahrefs.com/v3` with a Bearer key (see §3) |
| `from src.llm import console_openai_client, embed_texts` | OpenAI-compatible chat + embeddings client with spend attribution | `OpenAI(api_key=…)` (chat) + any embeddings endpoint |
| `from src.db_cross import cross_engine, cross_session, CrossBase, cross_session_scope` | SQLAlchemy 2.x engine/session/Base against a shared Postgres (`console_site_db`) | Standard SQLAlchemy 2 setup against your own Postgres |

`_scrapbook_core.py` is the single choke-point for Ahrefs + LLM access:

```python
def ahrefs(cap, args, timeout=120):      # -> replace body with requests to api.ahrefs.com/v3
    return _connector_invoke(cap, args, secret=AHREFS_SECRET, timeout=timeout)

def chat(messages, model=CHEAP_MODEL, ...):   # -> OpenAI(...).chat.completions.create(...)
def embed(texts):                              # -> embeddings endpoint, batched 96 at a time
```

Rewrite those three functions and every tab keeps working. **Nothing else in the
app knows the difference.**

---

## 2. Which tab uses which service

| Section / tab | Ahrefs | LLM | Embeddings | Other |
|---|---|---|---|---|
| Scraps · Posts | — | — | — | Obsidian-clip markdown parser (built in, no API) |
| Scraps · URLs | — | — | — | **web-page fetcher** (§4) |
| Scraps · Media | — | speech-to-text (transcription) | — | audio extraction (ffmpeg) |
| Scraps · Scrap trends | — | — | — | — (pure SQL) |
| Scraps · Ask my posts | — | ✅ chat | ✅ retrieval | — |
| Scraps · Firehose | — | ✅ (rule interpret) | — | **Ahrefs Firehose** stream (§5) |
| Topic research · Trending keywords | ✅ Keywords Explorer | optional (seed suggest) | — | — |
| Topic research · Topics | ✅ Batch Analysis (enrich) | ✅ (cluster labels) | ✅ clustering | web fetcher (sitemap + pages) |
| Monitoring · Reddit Radar | — | ✅ (weekly digest) | — | **Reddit** (§6) |
| Monitoring · Growth Scanner | ✅ Keywords Explorer | ✅ (cluster vetting) | — | — |
| Write · Ideas | ✅ Keywords Explorer | ✅ chat | — | — |
| Write · Example finder | — | — | ✅ search | web fetcher (URL import) |
| Write · Ahrefs weaver | ✅ Keywords Explorer + Site Explorer | ✅ chat | — | — |

---

## 3. Ahrefs connector → public API v3 map

Every Ahrefs call in the app, with the public endpoint that replaces it. Method
names are pinned in `_scrapbook_research.py` and `_scrapbook_write.py`.

| App call (`ahrefs("…")`) | Public Ahrefs API v3 | Used by | Notes |
|---|---|---|---|
| `ahrefs_keywords_explorer.ideas_by_terms_export` | `GET /v3/keywords-explorer/matching-terms` (and the `matching-questions` variant) | Trending, Growth Scanner, Ideas | The workhorse. Args: `seed_keywords`/`keywords`, `country`, `mode` (`all`/`phrase_match`/`questions`), `limit`, `offset`, `order_by`, `filters:{min_volume}`. Returns keyword + `volume`, `difficulty`, `cpc`, `traffic_potential`, `parent_topic`, `growth_rate:{months_3,6,12}`, `attrs:{branded,local}`, `categories:{category[],nsfw[]}`. |
| `ahrefs_keywords_explorer.keywords_overview_by_terms_export` | `GET /v3/keywords-explorer/overview` (terms mode) | Trending growth-backfill, weaver | Batch enrich a keyword list → volume/KD/CPC/TP/parent_topic/**growth_rate**. |
| `ahrefs_keywords_explorer.keywords_overview_by_page_or_domain` | `GET /v3/keywords-explorer/overview` (target mode) | Trending blog-rank annotation | Where the target site ranks for a keyword set. |
| `ahrefs_keywords_explorer.traffic_by_domains` | `GET /v3/keywords-explorer/traffic-share/by-domains` | Growth Scanner | Per-anchor domain traffic share → the "who owns this category" table. |
| `ahrefs_batch_analysis.ba_table` | `POST /v3/batch-analysis/batch-analysis` | Topics (optional enrichment) | Per-URL organic traffic / refdomains / UR, to describe topic-distance buckets. |
| `ahrefs_site_explorer.domain_rating` | `GET /v3/site-explorer/domain-rating` | weaver | DR for a domain (with `date`). |
| `ahrefs_site_explorer.metrics` | `GET /v3/site-explorer/metrics` | weaver | org traffic / traffic value / keyword count for a target. |

> **Content Gap (spec workflow 06) is deliberately NOT in this app.** It would
> pull `ahrefs_site_explorer.organic_keywords`
> (`GET /v3/site-explorer/organic-keywords`) for each competitor, two-pass-filter,
> then rank-check the target. The shared filter pipeline that would drive it
> (`keep_keyword()` in `_scrapbook_core.py`) **is** present and reused by the tabs
> above, so adding Content Gap later is a small job — see `filter-pipeline` notes
> in §7.

### The call that matters most

```bash
# Keyword ideas for a seed, rising-first — the heart of Trending + Ideas + Growth Scanner
curl "https://api.ahrefs.com/v3/keywords-explorer/matching-terms" \
  -H "Authorization: Bearer $AHREFS_API_KEY" \
  -G \
  --data-urlencode 'select=keyword,volume,difficulty,cpc,traffic_potential,parent_topic,growth_rate,serp_features' \
  --data-urlencode 'keywords=ai seo' \
  --data-urlencode 'country=us' \
  --data-urlencode 'limit=100' \
  --data-urlencode 'order_by=volume:desc'
```

The exact `select` fields and filter names occasionally shift between API
versions — **pin them and treat an all-empty result across every seed as a
failure, not "no data"** (see §7, transport note).

### Cost + rate limits

- Keywords Explorer / Site Explorer requests consume API **units** per row/field.
  Only `select` what you render.
- Keep concurrency low; the app spaces batches out and paginates by `offset`.
- Growth (`growth_rate`) is returned inline by the ideas/overview endpoints — no
  separate call.

---

## 4. Web-page fetcher (Scraps · URLs, Topics, Example finder)

Letaido calls its **web-fetch skill** (`skills/web-fetch/scripts/fetch.py`) as a
subprocess to turn a URL into clean body text. Replace `_fetch_text()` in
`_scrapbook_research.py` with any reader:

- `requests.get` + `trafilatura`/`readability-lxml`/`BeautifulSoup`, **or**
- a hosted reader (Jina Reader `r.jina.ai`, Firecrawl, etc.).

Contract the rest of the code expects: `fetch_text(url, max_chars) -> str`
(empty string on failure; raise a `FetchBlocked`-style error if you want the UI to
show "approve this host"). The URL tab caps bodies at ~40k chars and retries a
limited number of times before giving up on dead URLs.

---

## 5. Ahrefs Firehose (Scraps · Firehose)

The Firehose tab subscribes to Ahrefs' **real-time newly-published-pages stream**
and buffers matches for review. This is a separate Ahrefs product from the v3
REST API:

- Stream + rule model: Lucene-style queries, **25 rules/org across all taps**.
- In Letaido it arrives via a webhook/tap the platform manages; events are drained
  hourly (`app/scripts/scrapbook_firehose_drain.py`) into `scrapbook_firehose_events`.
- **Off-platform:** point the drain script at your own Firehose subscription
  endpoint (or drop the tab entirely — it's independent of every other section).

---

## 6. Reddit (Monitoring · Reddit Radar)

`run_radar_scan()` in `_scrapbook_research.py` fetches recent posts per configured
subreddit/query and an LLM summarises the week.

- The app uses Reddit's public JSON (`https://www.reddit.com/r/<sub>/…​.json` /
  `search.json`). Reddit 403s unauthenticated server fetches intermittently — use
  an OAuth app (`oauth.reddit.com`) or a scraping fallback (e.g. an Apify Reddit
  actor) for reliability.
- Contract: yield `{subreddit, title, url, body, score, created_utc, matched_query}`
  dicts; everything downstream (dedup, digest) is in-app.

---

## 7. Things that are NOT an Ahrefs feature — you build them

These are the parts that make Scrapbook more than an API wrapper.

1. **The shared filter pipeline** — `keep_keyword()` in `_scrapbook_core.py`. One
   keep/drop rule every research tab shares: min-volume, max-KD, growth gate
   (opt-in per tab), **own-brand exception** (keep *your* brand's keywords when
   excluding branded), always-on NSFW drop, prefix category match, substring
   `exclude_terms`. Get this right once; see `filter-pipeline.md` in the original
   spec for the exact algorithm.
2. **Markdown-clip import** — `_scrapbook_import.py`. Parses Obsidian-Web-Clipper
   `.md` for LinkedIn/X: platform + author detection (never the clip title),
   chrome-stripping, comment parsing from the raw body, and **date derivation from
   the snowflake id** (LinkedIn `activity` `id >> 22`; X `status` `(id>>22)+1288834974657`).
   No API — pure parsing.
3. **Topic clustering** — `run_topics_scan()` in `_scrapbook_research.py`. Sitemap
   → fetch → ~2k-char chunks → embed → L2-normalised page vectors → **hand-rolled
   k-means** (scan `k`, pick by silhouette) → LLM labels → distance-to-centroid
   concentration buckets. No sklearn, no vector DB (~40 lines of numpy-free math).
4. **Ask-my-posts retrieval** — embed the corpus (`saved_posts` + scraped
   `saved_urls`), rank by cosine, answer *only* from retrieved snippets with
   citations. There is no Ahrefs call here.
5. **Persistence + merge semantics** — the Trending **keyword bank**
   (`scrapbook_trending_keywords`, PK `(keyword,country)`) upserts and
   **COALESCEs** growth fields so a later null pull never wipes a known value.
   Scans merge, never wipe.
6. **The 30-second rule** — every scan/pull runs as a background job returning
   `{job_id}`, polled at `GET /api/job/<id>`. `_scrapbook_core.py` has the whole
   job registry.

### ⚠️ Transport note (learned the hard way)

A silent SDK/endpoint change once made every Trending scan return **0 keywords for
a month** with no error surfaced. **Pin the Ahrefs method + field names, and treat
"0 results across all seeds" as a failure** (alert / non-zero exit), not success.
`_scrapbook_research.py` guards this explicitly.

---

## 8. If you're an AI agent reading this to rebuild the app

**Stop and ask the human these first** — the answers change the schema, the cost
profile and which tabs are even in scope:

1. **Which sections do they actually want?** Scrapbook is five nav sections
   (Scraps, Topic research, Monitoring, Write — and, in the full spec, Publish).
   This repo ships four (Publish / Blog Refresh Engine lives in a separate app).
   Each section is independent; don't build all of them by default.
2. **What's the target site, country, competitor list and brand terms?** These
   live in one shared `settings`/`config` object and drive the filter pipeline and
   every research tab. Never hardcode them.
3. **Which Ahrefs plan + entitlements?** Keywords Explorer, Site Explorer, Batch
   Analysis and (optionally) Firehose are separate. Confirm before wiring a tab
   that needs one they don't have.
4. **Which LLM + embeddings model, and is the spend approved?** Ideas, Topics
   labels, Reddit digest, Ask, weaver all call an LLM; Topics + Ask + Example
   finder embed. Give them the per-run estimate before spending.
5. **How do they want the non-Ahrefs services wired?** Web fetcher (which reader?),
   LinkedIn (Apify actor id?), Reddit (OAuth or scrape?), Firehose (own
   subscription, or drop the tab?).
6. **Empty-start expectation.** The app ships with **no** seed data — every table
   is empty and every tab renders a friendly empty state. Don't seed demo rows.

Do not hardcode an API key, report id, brand name, domain or model into the
source. Read them from env/config, and surface the per-run cost before spending.

---

## File map

| File | What it is |
|---|---|
| `app/scrapbook.py` | Main module: DB models (items/boards/tags/chat/firehose), blueprint, capture routes, search, AI-note + chat endpoints. Mounts the other modules. |
| `app/_scrapbook_core.py` | **The choke-point** — settings object, job registry, `keep_keyword()` filter pipeline, `ahrefs()` / `chat()` / `embed()`. Rewrite these 3 functions to port. |
| `app/_scrapbook_models.py` | SQLAlchemy models for the research/monitoring/write tables. |
| `app/_scrapbook_routes.py` | All the non-capture routes (settings, jobs, trending, topics, radar, growth, ideas, examples, weaver) registered onto the blueprint. |
| `app/_scrapbook_research.py` | Trending scan, Topics clustering, Reddit Radar, Growth Scanner engines. |
| `app/_scrapbook_write.py` | Ideas, Example finder, Ahrefs weaver engines. |
| `app/_scrapbook_import.py` | Obsidian-clip markdown parser (LinkedIn/X). |
| `app/_scrapbook_firehose_ai.py` | Firehose rule interpretation + tap management. |
| `app/templates/scrapbook/index.html` | The whole SPA (Tailwind CDN, one section per nav group). |
| `app/scripts/*.py` | Scheduled workers: hourly Firehose drain, daily LinkedIn scrape. |
