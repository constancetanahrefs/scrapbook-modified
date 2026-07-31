# Scrapbook (modified)

**One workspace to capture content inspiration, research what to write about,
watch the landscape, and turn it into drafts — grounded in real
[Ahrefs](https://ahrefs.com) data.**

This is a working, modified build of Scrapbook, organised into four nav sections:

### 1. Scraps — capture inspiration
- **Posts** — save LinkedIn / X posts (paste or Obsidian-Web-Clipper markdown import), tag them, browse a card feed.
- **URLs** — save article URLs; their body text is scraped for later reference.
- **Media** — a gallery of images/video captured with posts, plus video transcription.
- **Scrap trends** — analytics over what you've saved (top tags, authors, cadence).
- **Ask my posts** — a chat that answers questions grounded in your saved scraps, with citations.
- **Boards** + **Firehose** — collections, and a real-time Ahrefs Firehose tap inbox.

### 2. Topic research — find what to write about
- **Trending keywords** — seed topics → Ahrefs Keywords Explorer growth scan → a durable, merging bank of rising keywords.
- **Topics** — semantic topic clusters over a domain's blog (embeddings + k-means + a concentration readout).

### 3. Monitoring — watch the landscape
- **Reddit Radar** — scheduled scan of subreddits/queries for AI-search & SEO chatter → a weekly LLM digest.
- **Growth Scanner** — track a category's rising keyword clusters and which domains own them, over a snapshot timeline.

### 4. Write — turn research into drafts
- **Ideas** — keyword-data-led headline & angle generator from your saved scraps.
- **Example finder** — search a corpus of reference documents for structural/style examples.
- **Ahrefs weaver** — weave Ahrefs data points into a draft as inline, cited mentions.

Built as a [Letaido](https://letaido.com) Console app, but the logic is plain
Flask 3 + SQLAlchemy 2 + Pydantic v2 + Jinja2 + Tailwind (CDN). If you're on
another stack, **[docs/PORTING.md](docs/PORTING.md)** maps every external call —
Ahrefs API v3, the LLM, embeddings, the web fetcher, Firehose and Reddit — to its
public equivalent, and tells an AI agent exactly what to ask before rebuilding.

> **Starts empty.** No posts, no keywords, no seeds, no sessions. Every tab renders
> its own empty state until you capture or scan something. Nothing is seeded.

---

## What ties it together

- **One shared keyword vocabulary** — volume, KD (0–100), traffic potential, CPC,
  parent topic, growth rate (3/6/12-month). Every research/monitoring tab speaks it.
- **One shared filter pipeline** — a single keep/drop rule (`keep_keyword()` in
  `app/_scrapbook_core.py`): min-volume, max-KD, growth gate, own-brand exception,
  always-on NSFW drop, prefix category match. Get it right once; every tab behaves.
- **One shared scrap bank** — posts, URLs and media accumulate across sessions;
  re-running a scan **merges**, never wipes.
- **One shared settings object** — target domain, country, competitors, brand
  terms, filter defaults.
- **Sessions & jobs** — every long scan runs as a background job the client polls
  (`GET /api/job/<id>`), because the proxy times out around 30s.

---

## Relationship to the original spec

This build follows the shared **scrapbook-spec** (the logic/data-shape spec any AI
platform can rebuild from). Two intentional differences:

- **Content Gap (spec workflow 06) is not built** — it duplicates a dedicated
  Keyword Research Hub. The shared filter pipeline it would need *is* present, so
  it's a small add if you want it (see `docs/PORTING.md` §3).
- **Publish / Blog Refresh Engine (spec section 5) lives in a separate app** — it
  was extracted from Scrapbook, so this repo is the four capture/research/
  monitoring/write sections.

Also kept beyond the spec: **Boards** and the **Ahrefs Firehose** inbox.

---

## Architecture

```
app/
  scrapbook.py                 # main module: models, blueprint, capture routes, search, AI notes, chat
  _scrapbook_core.py           # settings + jobs + filter pipeline + ahrefs()/chat()/embed()  ← port here
  _scrapbook_models.py         # research/monitoring/write tables
  _scrapbook_routes.py         # settings, jobs, trending, topics, radar, growth, ideas, examples, weaver
  _scrapbook_research.py       # Trending, Topics, Reddit Radar, Growth Scanner engines
  _scrapbook_write.py          # Ideas, Example finder, Ahrefs weaver engines
  _scrapbook_import.py         # Obsidian-clip markdown parser (LinkedIn/X, snowflake dates)
  _scrapbook_firehose_ai.py    # Firehose rule interpretation + tap management
  templates/scrapbook/index.html   # the whole SPA (Tailwind CDN)
  scripts/
    scrapbook_firehose_drain.py    # hourly: drain Firehose events for review
    scrapbook_linkedin_scrape.py   # daily: batch-scrape queued LinkedIn posts
```

**Data store:** PostgreSQL (tables prefixed `scrapbook_*`). On Letaido this is the
shared `console_site_db`; standalone, point it at any Postgres.

**The whole app talks to Ahrefs + the LLM through three functions in
`_scrapbook_core.py`** (`ahrefs`, `chat`, `embed`). Rewrite those to hit
`api.ahrefs.com/v3` and your own OpenAI-compatible endpoint and every tab works
unchanged — that's the entire porting surface.

---

## Running it

### On Letaido
Drop `app/*.py` into a Console scaffold's `applications/` folder and
`app/templates/scrapbook/` into `templates/`. The loader registers the blueprint
by its `NAME` + `blueprint` at `/applications/scrapbook/`. The platform provides
`src.connectors`, `src.llm`, `src.db_cross`, `src.schemas`.

### Standalone
1. `pip install -r requirements.txt`
2. Provide a Postgres and set `DATABASE_URL`.
3. Set `AHREFS_API_KEY`, an LLM key (`OPENAI_API_KEY` or compatible), and any of
   the optional service creds you want (LinkedIn scraper, Reddit, Firehose).
4. Replace the three Letaido imports as described in **[docs/PORTING.md](docs/PORTING.md) §1**.
5. Run the Flask app; open the blueprint's index route.

---

## License

[MIT](LICENSE) — © 2026 Constance Tan.

Built with [Ahrefs](https://ahrefs.com) data. "Ahrefs" is a trademark of its owner;
this project is not affiliated with or endorsed by Ahrefs.
