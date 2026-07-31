"""Scrapbook — routes for the shared surface, Scraps gaps, and the four new sections.

Registered onto the app's existing blueprint by `register(blueprint, ctx)` so the
main module stays the single mount point. Every route validates its body through a
Pydantic v2 model and every long operation returns {job_id} to be polled.
"""

from __future__ import annotations

import csv
import io
import json
import os
import re
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from typing import Optional

from flask import Response, jsonify, request
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import func

from src.db_cross import cross_session as sbs, cross_session_scope
from src.schemas import validate_json

from applications._scrapbook_core import (
    DEFAULT_SETTINGS, DEFAULT_FILTERS, job_get, job_run, keep_keyword, brand_tokens,
)
from applications._scrapbook_models import (
    SBCategory, SBConfig, SBExampleDoc, SBIdea, SBMedia, SBRadarReport,
    SBRedditPost, SBTopicCluster, SBTopicPage, SBTopicScanState, SBTrendingKeyword,
    SBTrendingScan, SBTrendingSeed,
)
from applications import _scrapbook_import as mdimport
from applications import _scrapbook_research as research
from applications import _scrapbook_write as write
from applications import _scrapbook_firehose_ai as fhai


def _now():
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Validation models
# ---------------------------------------------------------------------------

class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SettingsPatch(Strict):
    target_site: Optional[str] = None
    target_country: Optional[str] = None
    competitors: Optional[list[str]] = None
    brand_terms: Optional[list[str]] = None
    filters: Optional[dict] = None
    radar_subreddits: Optional[list[str]] = None
    radar_queries: Optional[list[str]] = None


class ImportPayload(Strict):
    files: list[dict] = Field(default_factory=list)   # [{name, content}]
    markdown: Optional[str] = None
    board_id: Optional[str] = None
    tags: list[str] = Field(default_factory=list)


class PostPaste(Strict):
    platform: str = "linkedin"
    author_name: str = ""
    author_headline: str = ""
    post_url: str = ""
    content: str
    tags: list[str] = Field(default_factory=list)
    board_id: Optional[str] = None

    @field_validator("content")
    @classmethod
    def _len(cls, v):
        if len(v.strip()) < 5:
            raise ValueError("content too short")
        return v



class TapDescribeIn(Strict):
    description: str


class TapRefineIn(Strict):
    plan: dict
    instruction: str
    history: list = Field(default_factory=list)


class TapEditRuleIn(Strict):
    plan: dict
    index: int
    value: str


class TapCreateIn(Strict):
    plan: dict
    subscribe: bool = True


class SeedsIn(Strict):
    seeds: list[str]


class ScanIn(Strict):
    pass


class TopicsScanIn(Strict):
    domain: Optional[str] = None
    enrich: bool = True
    locale: Optional[str] = None      # e.g. "en" — restrict a multilingual sitemap


class CategoryScanIn(Strict):
    primary_seed: str
    related_seeds: list[str] = Field(default_factory=list)
    strip_brands: bool = True


class LabelsIn(Strict):
    labels: list[str]


class RenameIn(Strict):
    name: str


class IdeasIn(Strict):
    source_item_ids: list[str] = Field(default_factory=list)
    limit: int = 12


class ExampleIn(Strict):
    title: str = ""
    url: str = ""
    content: str = ""


class WeaverIn(Strict):
    draft_md: str
    target: str = ""
    keywords: list[str] = Field(default_factory=list)


class ArticleIn(Strict):
    url: str
    title: str = ""
    primary_keyword: str


class AuditConfigIn(Strict):
    selected_competitors: list = Field(default_factory=list)
    selected_intent_idx: int = 0


class DecisionIn(Strict):
    card_id: str
    status: str

    @field_validator("status")
    @classmethod
    def _st(cls, v):
        if v not in ("pending", "accepted", "rejected"):
            raise ValueError("status must be pending|accepted|rejected")
        return v


class PlacementIn(Strict):
    card_id: str
    section_idx: Optional[int] = None


class ChatIn(Strict):
    message: str


class TranscribeIn(Strict):
    media_id: str
    transcript: Optional[str] = None      # paste-transcript fallback


class MediaIn(Strict):
    type: str = "image"
    url: str
    title: str = ""


# ---------------------------------------------------------------------------
# Settings helpers
# ---------------------------------------------------------------------------

def load_settings() -> dict:
    out = {k: (v.copy() if isinstance(v, (dict, list)) else v) for k, v in DEFAULT_SETTINGS.items()}
    for row in sbs.query(SBConfig).all():
        val = row.value
        if isinstance(val, dict) and "v" in val and len(val) == 1:
            val = val["v"]
        out[row.key] = val
    out["filters"] = {**DEFAULT_FILTERS, **(out.get("filters") or {})}
    return out


def _settings_bg() -> dict:
    """Settings read from a fresh session — safe inside background threads."""
    with cross_session_scope() as s:
        out = {k: (v.copy() if isinstance(v, (dict, list)) else v) for k, v in DEFAULT_SETTINGS.items()}
        for row in s.query(SBConfig).all():
            val = row.value
            if isinstance(val, dict) and "v" in val and len(val) == 1:
                val = val["v"]
            out[row.key] = val
        out["filters"] = {**DEFAULT_FILTERS, **(out.get("filters") or {})}
        return out


def _save_setting(key: str, value) -> None:
    row = sbs.get(SBConfig, key)
    payload = value if isinstance(value, (dict, list)) else {"v": value}
    if row is None:
        sbs.add(SBConfig(key=key, value=payload))
    else:
        row.value = payload
        row.updated_at = _now()
    sbs.commit()


def _clean_author(raw) -> str:
    """Some scraped pages store JSON-LD blobs in meta.author — keep only real names."""
    if isinstance(raw, dict):
        raw = raw.get("name") or ""
    if isinstance(raw, list):
        raw = next((x.get("name") if isinstance(x, dict) else x for x in raw), "") or ""
    s = str(raw or "").strip()
    if not s or len(s) > 80:
        return ""
    if s.startswith(("{", "[")) or "@id" in s or "schema/" in s or "http" in s:
        return ""
    return s


def _csv_response(rows: list[dict], fields: list[str], name: str) -> Response:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return Response(buf.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f'attachment; filename="{name}"'})


# ===========================================================================
# register()
# ===========================================================================

def register(bp, ctx: dict) -> None:
    """ctx supplies the main module's pieces: SBItem, _item_to_dict, _finalize_item_create,
    _save_bytes, DATA_DIR, llm client."""
    SBItem = ctx["SBItem"]
    item_to_dict = ctx["item_to_dict"]
    finalize = ctx["finalize_item_create"]
    save_bytes = ctx["save_bytes"]
    DATA_DIR = ctx["DATA_DIR"]

    # ---------------- Shared: settings + job polling --------------------

    @bp.route("/api/config", methods=["GET"])
    def sb_get_config():
        return jsonify(load_settings())

    @bp.route("/api/config", methods=["POST"])
    def sb_post_config():
        data = validate_json(SettingsPatch)
        payload = data.model_dump(exclude_none=True)
        if "filters" in payload:
            merged = {**DEFAULT_FILTERS, **(load_settings().get("filters") or {}), **payload["filters"]}
            payload["filters"] = {k: merged[k] for k in DEFAULT_FILTERS}
        for k, v in payload.items():
            _save_setting(k, v)
        return jsonify(load_settings())

    @bp.route("/api/job/<job_id>")
    def sb_job(job_id):
        j = job_get(job_id)
        if not j:
            return jsonify({"error": "job not found"}), 404
        return jsonify(j)

    # ---------------- Scraps: markdown import + paste -------------------

    def _save_parsed_post(parsed: dict, tags: list[str], board_id):
        """Store a parsed clip as a scrapbook item (+ media rows)."""
        url = parsed.get("post_url") or ""
        existing = None
        if url:
            existing = sbs.query(SBItem).filter(SBItem.url == url).first()
        it = existing or SBItem(type=parsed.get("platform") or "linkedin")
        it.type = "linkedin" if parsed.get("platform") == "linkedin" else "x_post"
        it.title = (parsed.get("title") or "")[:500]
        it.url = url or None
        it.content_md = parsed.get("content") or ""
        it.content_text = parsed.get("content") or ""
        meta = dict(it.meta or {})
        meta.update({
            "author": parsed.get("author_name") or "",
            "author_headline": parsed.get("author_headline") or "",
            "comments": parsed.get("comments") or [],
            "comments_count": parsed.get("comments_count") or 0,
            "media": parsed.get("media") or [],
            "published_at": (parsed["published_at"].isoformat()
                             if parsed.get("published_at") else None),
            "import_source": parsed.get("source_filename") or "paste",
        })
        it.meta = meta
        it.scrape_status = "done"
        if tags:
            it.tags = sorted(set(list(it.tags or []) + tags))
        if board_id:
            it.board_id = board_id
        if parsed.get("media") and not it.thumbnail_path:
            first_img = next((m["url"] for m in parsed["media"] if m["type"] == "image"), None)
            if first_img:
                it.thumbnail_path = first_img
        if existing is None:
            sbs.add(it)
        sbs.flush()
        # Mirror media into the gallery.
        for m in (parsed.get("media") or []):
            dup = (sbs.query(SBMedia)
                   .filter(SBMedia.url == m["url"], SBMedia.source_item_id == it.id).first())
            if dup:
                continue
            sbs.add(SBMedia(type=m.get("type") or "image", url=m["url"],
                            title=(parsed.get("title") or "")[:300], source_item_id=it.id))
        return it, existing is not None

    @bp.route("/api/posts/import-markdown", methods=["POST"])
    def sb_import_markdown():
        data = validate_json(ImportPayload)
        files = list(data.files or [])
        if data.markdown:
            files.append({"name": "pasted.md", "content": data.markdown})
        if not files:
            return jsonify({"error": "no files supplied"}), 400
        added, updated, failed, results = 0, 0, [], []
        for f in files[:100]:
            name = str(f.get("name") or "clip.md")
            content = f.get("content") or ""
            if not content.strip():
                failed.append({"file": name, "error": "empty file"})
                continue
            try:
                parsed = mdimport.parse_clip(content, name)
                it, was_update = _save_parsed_post(parsed, data.tags, data.board_id)
                results.append({"file": name, "id": it.id, "author": parsed["author_name"],
                                "platform": parsed["platform"], "comments": parsed["comments_count"],
                                "published_at": meta_iso(parsed.get("published_at")),
                                "updated": was_update})
                updated += 1 if was_update else 0
                added += 0 if was_update else 1
            except Exception as exc:  # noqa: BLE001
                failed.append({"file": name, "error": str(exc)})
        sbs.commit()
        return jsonify({"added": added, "updated": updated, "failed": failed, "results": results})

    def meta_iso(dt):
        return dt.isoformat() if dt else None

    @bp.route("/api/posts/paste", methods=["POST"])
    def sb_paste_post():
        data = validate_json(PostPaste)
        raw = data.content
        parsed = mdimport.parse_clip(raw, "pasted")
        # Explicit fields from the form win over detection.
        if data.author_name:
            parsed["author_name"] = data.author_name
        if data.author_headline:
            parsed["author_headline"] = data.author_headline
        if data.post_url:
            parsed["post_url"] = data.post_url
        if data.platform:
            parsed["platform"] = data.platform
        if not parsed["content"].strip():
            parsed["content"] = raw.strip()
            parsed["title"] = raw.strip().split("\n")[0][:180]
        it, _ = _save_parsed_post(parsed, data.tags, data.board_id)
        sbs.commit()
        return jsonify(item_to_dict(it))

    # ---------------- Scraps: media gallery + transcription -------------

    @bp.route("/api/media")
    def sb_media_list():
        kind = request.args.get("type")
        q = sbs.query(SBMedia)
        if kind in ("image", "video"):
            q = q.filter(SBMedia.type == kind)
        rows = q.order_by(SBMedia.created_at.desc()).limit(400).all()
        # Mirror any image/screenshot items that predate the gallery.
        return jsonify({"total": q.count(), "items": [{
            "id": m.id, "type": m.type, "url": m.url,
            "cached_url": (f"/applications/scrapbook/media/{m.id}/file" if m.cached_path else None),
            "title": m.title, "transcript": m.transcript,
            "transcript_status": m.transcript_status, "transcript_error": m.transcript_error,
            "source_item_id": m.source_item_id,
            "created_at": m.created_at.isoformat() if m.created_at else None,
        } for m in rows]})

    @bp.route("/api/media", methods=["POST"])
    def sb_media_add():
        data = validate_json(MediaIn)
        if data.type not in ("image", "video"):
            return jsonify({"error": "type must be image|video"}), 400
        m = SBMedia(type=data.type, url=data.url.strip(), title=data.title[:300])
        sbs.add(m)
        sbs.commit()
        return jsonify({"id": m.id})

    @bp.route("/api/media/<mid>", methods=["DELETE"])
    def sb_media_delete(mid):
        m = sbs.get(SBMedia, mid)
        if m:
            sbs.delete(m)
            sbs.commit()
        return jsonify({"ok": True})

    @bp.route("/api/media/backfill", methods=["POST"])
    def sb_media_backfill():
        """Pull media out of already-saved posts (meta.media) and image items."""
        added = 0
        rows = sbs.query(SBItem).all()
        for it in rows:
            for m in ((it.meta or {}).get("media") or []):
                url = m.get("url")
                if not url:
                    continue
                if sbs.query(SBMedia).filter(SBMedia.url == url,
                                             SBMedia.source_item_id == it.id).first():
                    continue
                sbs.add(SBMedia(type=m.get("type") or "image", url=url,
                                title=(it.title or "")[:300], source_item_id=it.id))
                added += 1
            if it.type in ("image", "screenshot") and it.file_path:
                if not sbs.query(SBMedia).filter(SBMedia.source_item_id == it.id,
                                                 SBMedia.type == "image").first():
                    sbs.add(SBMedia(type="image", url="", cached_path=it.file_path,
                                    title=(it.title or "")[:300], source_item_id=it.id))
                    added += 1
        sbs.commit()
        return jsonify({"added": added})

    @bp.route("/media/<mid>/file")
    def sb_media_file(mid):
        from flask import send_file, abort
        m = sbs.get(SBMedia, mid)
        if not m or not m.cached_path or not os.path.exists(m.cached_path):
            abort(404)
        return send_file(m.cached_path)

    @bp.route("/api/media/transcribe", methods=["POST"])
    def sb_transcribe():
        data = validate_json(TranscribeIn)
        m = sbs.get(SBMedia, data.media_id)
        if not m:
            return jsonify({"error": "media not found"}), 404
        if data.transcript:                    # paste-transcript fallback
            m.transcript = data.transcript.strip()
            m.transcript_status = "done"
            m.transcript_error = ""
            sbs.commit()
            return jsonify({"ok": True, "status": "done"})
        if m.type != "video":
            return jsonify({"error": "only video can be transcribed"}), 400
        m.transcript_status = "running"
        m.transcript_error = ""
        sbs.commit()
        jid = job_run("transcribe", _transcribe_job, data.media_id, DATA_DIR)
        return jsonify({"job_id": jid})

    # ---------------- Images: re-cache expired LinkedIn thumbnails ------

    @bp.route("/api/images/status")
    def sb_image_status():
        """How many saved posts still point at an expiring remote image?"""
        rows = sbs.query(SBItem).filter(SBItem.thumbnail_path.isnot(None)).all()
        remote = [r for r in rows if str(r.thumbnail_path).startswith(("http://", "https://"))]
        licdn = [r for r in remote if "licdn.com" in str(r.thumbnail_path)]
        return jsonify({"with_image": len(rows), "remote": len(remote),
                        "linkedin_remote": len(licdn),
                        "cached": len(rows) - len(remote)})

    @bp.route("/api/images/requeue", methods=["POST"])
    def sb_image_requeue():
        """Mark posts whose image link has expired as pending, so a re-scrape
        fetches a freshly-signed CDN url (the only way to recover the picture).

        Only touches LinkedIn items that still have a source url to scrape.
        """
        rows = [r for r in sbs.query(SBItem)
                .filter(SBItem.type == "linkedin", SBItem.url.isnot(None)).all()
                if str(r.thumbnail_path or "").startswith(("http://", "https://"))]
        for r in rows:
            r.scrape_status = "pending"
            r.scrape_error = ""
        sbs.commit()
        return jsonify({"requeued": len(rows),
                        "ids": [r.id for r in rows],
                        "next": "Run ↻ Scrape on the Posts tab to fetch fresh images."})

    @bp.route("/api/images/recache-avatars", methods=["POST"])
    def sb_avatar_recache():
        """Cache author avatars locally — LinkedIn's avatar urls also expire."""
        from applications.scrapbook import _cache_remote_image
        rows = sbs.query(SBItem).filter(SBItem.type == "linkedin").all()
        cached = failed = 0
        for r in rows:
            meta = dict(r.meta or {})
            src = meta.get("author_profile_picture")
            if not src or meta.get("author_profile_picture_cached"):
                continue
            path = _cache_remote_image(src)
            if path:
                meta["author_profile_picture_cached"] = path
                meta["author_profile_picture_source"] = src
                r.meta = meta
                cached += 1
            else:
                failed += 1
        sbs.commit()
        return jsonify({"cached": cached, "failed": failed})

    @bp.route("/api/images/recache", methods=["POST"])
    def sb_image_recache():
        """Download every still-remote thumbnail into local storage (job).

        LinkedIn CDN links are signed and expire; already-expired ones cannot be
        recovered here (the bytes are gone) and are reported as unrecoverable so
        the user knows a re-scrape of the source post is required.
        """
        jid = job_run("recache_images", _recache_images_job, ctx)
        return jsonify({"job_id": jid})


    # ---------------- Firehose: AI tap builder --------------------------

    @bp.route("/api/firehose/budget")
    def sb_fh_budget():
        """Rule usage vs Firehose's 25-per-organisation cap, plus approval readiness."""
        try:
            out = fhai.rule_budget()
        except fhai.FirehoseAIError as exc:
            out = {"error": str(exc)}
        try:
            out["preflight"] = fhai.preflight()
        except Exception:
            pass
        return jsonify(out)

    def _plan_job(fn, *args):
        """Interpretation takes ~15-25s, which is uncomfortably close to the 30s
        proxy timeout — so run it as a polled job like every other slow op."""
        from applications._scrapbook_core import job_done, job_fail

        def _run(job_id):
            try:
                job_done(job_id, {"plan": fn(*args)}, "Plan ready")
            except fhai.FirehoseAIError as exc:
                job_fail(job_id, str(exc))

        return job_run("firehose_plan", _run)

    @bp.route("/api/firehose/interpret", methods=["POST"])
    def sb_fh_interpret():
        data = validate_json(TapDescribeIn)
        return jsonify({"job_id": _plan_job(fhai.interpret, data.description)})

    @bp.route("/api/firehose/refine", methods=["POST"])
    def sb_fh_refine():
        data = validate_json(TapRefineIn)
        return jsonify({"job_id": _plan_job(fhai.refine, data.plan, data.instruction, data.history)})

    @bp.route("/api/firehose/edit-rule", methods=["POST"])
    def sb_fh_edit_rule():
        data = validate_json(TapEditRuleIn)
        try:
            return jsonify({"plan": fhai.set_rule_value(data.plan, data.index, data.value)})
        except fhai.FirehoseAIError as exc:
            return jsonify({"error": str(exc)}), 400

    PENDING_TAP_KEY = "firehose_pending_tap"

    def _register_tap(tap_id, name, secret_name):
        with cross_session_scope() as s:
            SBTap = ctx["SBFirehoseTap"]
            if not s.get(SBTap, tap_id):
                s.add(SBTap(tap_id=tap_id, name=name[:300],
                            tap_secret_name=secret_name, is_active=True))

    def _pending_get():
        row = sbs.get(SBConfig, PENDING_TAP_KEY)
        return (row.value or {}) if row else {}

    def _pending_set(val):
        row = sbs.get(SBConfig, PENDING_TAP_KEY)
        if row:
            row.value = val
        else:
            sbs.add(SBConfig(key=PENDING_TAP_KEY, value=val))
        sbs.commit()

    @bp.route("/api/firehose/create-tap", methods=["POST"])
    def sb_fh_create_tap():
        """Step 1: create the tap. Its rule secret is minted here and can't have been
        approved in advance, so we park the plan and ask the user to approve, then
        they come back and call /finish-setup."""
        data = validate_json(TapCreateIn)
        try:
            out = fhai.create_tap_only(data.plan)
        except fhai.FirehoseAIError as exc:
            return jsonify({"error": str(exc)}), 400

        _pending_set({"tap_id": out["tap_id"], "tap_name": out["tap_name"],
                      "secret_name": out["secret_name"], "plan": data.plan,
                      "subscribe": bool(data.subscribe)})
        out["needs_approval"] = not fhai.rules_approved(out["secret_name"])
        out["stage"] = "awaiting_rule_approval"
        return jsonify(out)

    @bp.route("/api/firehose/pending-tap")
    def sb_fh_pending():
        """Is a half-built tap waiting for its rule approval?"""
        p = _pending_get()
        if not p.get("tap_id"):
            return jsonify({"pending": False})
        return jsonify({"pending": True, "tap_id": p["tap_id"], "tap_name": p.get("tap_name"),
                        "secret_name": p.get("secret_name"),
                        "rules_pending": len((p.get("plan") or {}).get("rules") or []),
                        "approved": fhai.rules_approved(p.get("secret_name", ""))})

    @bp.route("/api/firehose/pending-tap", methods=["DELETE"])
    def sb_fh_pending_clear():
        _pending_set({})
        return jsonify({"ok": True})

    @bp.route("/api/firehose/finish-setup", methods=["POST"])
    def sb_fh_finish():
        """Step 2: register the rules now that the tap's secret is approved."""
        p = _pending_get()
        if not p.get("tap_id"):
            return jsonify({"error": "There's no half-built tap waiting to be finished."}), 400
        try:
            out = fhai.finish_setup(p["plan"], p["tap_id"], p["secret_name"],
                                    register_cb=_register_tap,
                                    subscribe=p.get("subscribe", True))
        except fhai.FirehoseAIError as exc:
            return jsonify({"error": str(exc), "still_pending": True}), 400
        _pending_set({})
        return jsonify(out)


    # ---------------- Scraps: trends (pulse) ----------------------------

    @bp.route("/api/pulse")
    def sb_pulse():
        rows = sbs.query(SBItem).all()
        tag_counts, author_counts, cadence = {}, {}, {}
        for it in rows:
            for t in (it.tags or []):
                tag_counts[t] = tag_counts.get(t, 0) + 1
            author = _clean_author((it.meta or {}).get("author"))
            if author:
                author_counts[author] = author_counts.get(author, 0) + 1
            if it.saved_at:
                wk = (it.saved_at - timedelta(days=it.saved_at.weekday())).date().isoformat()
                cadence[wk] = cadence.get(wk, 0) + 1
        posts = sum(1 for it in rows if it.type in ("linkedin", "x_post"))
        urls = sum(1 for it in rows if it.type == "url")
        media = sbs.query(SBMedia).count()
        by_type = dict(sbs.query(SBItem.type, func.count(SBItem.id)).group_by(SBItem.type).all())
        return jsonify({
            "top_tags": [{"tag": k, "count": v} for k, v in
                         sorted(tag_counts.items(), key=lambda kv: -kv[1])[:20]],
            "top_authors": [{"author": k, "count": v} for k, v in
                            sorted(author_counts.items(), key=lambda kv: -kv[1])[:15]],
            "cadence": [{"week": k, "count": cadence[k]} for k in sorted(cadence)][-26:],
            "totals": {"posts": posts, "urls": urls, "media": media,
                       "tags": len(tag_counts), "items": len(rows), "by_type": by_type},
        })

    # ---------------- Topic research: Trending --------------------------

    @bp.route("/api/trending/seeds", methods=["GET"])
    def sb_seeds():
        rows = sbs.query(SBTrendingSeed).order_by(SBTrendingSeed.added_at.desc()).all()
        return jsonify({"seeds": [{"id": r.id, "seed": r.seed, "active": r.active,
                                   "added_at": r.added_at.isoformat() if r.added_at else None}
                                  for r in rows]})

    @bp.route("/api/trending/seeds", methods=["POST"])
    def sb_seeds_add():
        data = validate_json(SeedsIn)
        added = 0
        for raw in data.seeds:
            seed = (raw or "").strip().lower()
            if not seed:
                continue
            if sbs.query(SBTrendingSeed).filter(SBTrendingSeed.seed == seed).first():
                continue
            sbs.add(SBTrendingSeed(seed=seed))
            added += 1
        sbs.commit()
        return jsonify({"added": added})

    @bp.route("/api/trending/seeds/<int:sid>", methods=["DELETE"])
    def sb_seed_toggle(sid):
        r = sbs.get(SBTrendingSeed, sid)
        if not r:
            return jsonify({"error": "not found"}), 404
        if request.args.get("hard") == "1":
            sbs.delete(r)
        else:
            r.active = not r.active           # DELETE toggles active (spec)
        sbs.commit()
        return jsonify({"ok": True, "active": None if request.args.get("hard") == "1" else r.active})

    @bp.route("/api/trending/seeds/suggest", methods=["POST"])
    def sb_seeds_suggest():
        return jsonify({"suggestions": research.suggest_seeds()})

    @bp.route("/api/trending/scan", methods=["POST"])
    def sb_trending_scan():
        jid = job_run("trending", lambda j: research.run_trending_scan(j, _settings_bg()))
        return jsonify({"job_id": jid})

    def _trending_query():
        q = sbs.query(SBTrendingKeyword)
        text = (request.args.get("q") or "").strip()
        if text:
            pos = [t for t in text.split() if not t.startswith("-")]
            neg = [t[1:] for t in text.split() if t.startswith("-") and len(t) > 1]
            for t in pos:
                q = q.filter(SBTrendingKeyword.keyword.ilike(f"%{t}%"))
            for t in neg:
                q = q.filter(~SBTrendingKeyword.keyword.ilike(f"%{t}%"))
        if request.args.get("min_vol"):
            q = q.filter(SBTrendingKeyword.volume >= int(request.args["min_vol"]))
        if request.args.get("max_kd"):
            q = q.filter(SBTrendingKeyword.difficulty <= int(request.args["max_kd"]))
        if request.args.get("days"):
            since = _now() - timedelta(days=int(request.args["days"]))
            q = q.filter(SBTrendingKeyword.first_seen_at >= since)
        return q

    SORTS = {
        "newest": SBTrendingKeyword.first_seen_at.desc(),
        "volume": SBTrendingKeyword.volume.desc().nullslast(),
        "kd": SBTrendingKeyword.difficulty.asc().nullslast(),
        "growth_3m": SBTrendingKeyword.growth_3m.desc().nullslast(),
        "growth_6m": SBTrendingKeyword.growth_6m.desc().nullslast(),
        "growth_12m": SBTrendingKeyword.growth_12m.desc().nullslast(),
        "blog_rank": SBTrendingKeyword.blog_position.asc().nullslast(),
    }

    def _kw_dict(r):
        return {"keyword": r.keyword, "country": r.country, "volume": r.volume,
                "difficulty": r.difficulty, "traffic_potential": r.traffic_potential,
                "cpc": (round(r.cpc_cents / 100, 2) if r.cpc_cents is not None else None), "parent_topic": r.parent_topic,
                "source_seed": r.source_seed, "growth_3m": r.growth_3m,
                "growth_6m": r.growth_6m, "growth_12m": r.growth_12m,
                "blog_position": r.blog_position, "blog_url": r.blog_url,
                "first_seen_at": r.first_seen_at.isoformat() if r.first_seen_at else None,
                "last_seen_at": r.last_seen_at.isoformat() if r.last_seen_at else None}

    @bp.route("/api/trending/feed")
    def sb_trending_feed():
        q = _trending_query()
        total = q.count()
        order = SORTS.get(request.args.get("sort", "growth_3m"), SORTS["growth_3m"])
        limit = min(int(request.args.get("limit", 200)), 500)
        offset = int(request.args.get("offset", 0))
        rows = q.order_by(order).limit(limit).offset(offset).all()
        return jsonify({"total": total, "keywords": [_kw_dict(r) for r in rows]})

    @bp.route("/api/trending/stats")
    def sb_trending_stats():
        total = sbs.query(SBTrendingKeyword).count()
        new7 = sbs.query(SBTrendingKeyword).filter(
            SBTrendingKeyword.first_seen_at >= _now() - timedelta(days=7)).count()
        new30 = sbs.query(SBTrendingKeyword).filter(
            SBTrendingKeyword.first_seen_at >= _now() - timedelta(days=30)).count()
        seeds = sbs.query(SBTrendingSeed).filter(SBTrendingSeed.active.is_(True)).count()
        last = sbs.query(SBTrendingScan).order_by(SBTrendingScan.scan_date.desc()).first()
        return jsonify({"total": total, "new_7d": new7, "new_30d": new30,
                        "active_seeds": seeds,
                        "last_scan": ({"date": last.scan_date.isoformat(),
                                       "found": last.keywords_found, "new": last.new_keywords,
                                       "status": last.status, "note": last.note} if last else None)})

    @bp.route("/api/trending/csv")
    def sb_trending_csv():
        rows = [_kw_dict(r) for r in _trending_query().order_by(
            SORTS.get(request.args.get("sort", "growth_3m"), SORTS["growth_3m"])).limit(5000).all()]
        return _csv_response(rows, ["keyword", "volume", "difficulty", "traffic_potential", "cpc",
                                    "growth_3m", "growth_6m", "growth_12m", "parent_topic",
                                    "source_seed", "blog_position", "blog_url",
                                    "first_seen_at", "last_seen_at"], "trending-keywords.csv")

    @bp.route("/api/trending/scans")
    def sb_trending_scans():
        rows = sbs.query(SBTrendingScan).order_by(SBTrendingScan.scan_date.desc()).limit(20).all()
        return jsonify({"scans": [{"id": r.id, "date": r.scan_date.isoformat() if r.scan_date else None,
                                   "seeds_used": r.seeds_used, "found": r.keywords_found,
                                   "new": r.new_keywords, "status": r.status, "note": r.note}
                                  for r in rows]})

    # ---------------- Topic research: Topics ----------------------------

    @bp.route("/api/topics")
    def sb_topics():
        domain = (request.args.get("domain") or load_settings().get("target_site") or "").strip()
        if not domain:
            return jsonify({"clusters": [], "concentration": {}, "updated_at": None, "domain": ""})
        st = sbs.get(SBTopicScanState, domain)
        clusters = sbs.query(SBTopicCluster).filter(SBTopicCluster.domain == domain).all()
        pages = sbs.query(SBTopicPage).filter(SBTopicPage.domain == domain).all()
        by_cluster = {}
        for p in pages:
            by_cluster.setdefault(p.cluster_id, []).append(p)
        return jsonify({
            "domain": domain,
            "status": st.status if st else "idle",
            "step": st.step if st else "",
            "concentration": (st.concentration if st else {}) or {},
            "updated_at": st.updated_at.isoformat() if st and st.updated_at else None,
            "clusters": [{
                "id": c.id, "label": c.label, "size": c.size,
                "avg_distance": round(c.avg_distance, 4) if c.avg_distance is not None else None,
                "sample_urls": c.sample_urls or [],
                "avg_traffic": _avg([p.traffic for p in by_cluster.get(c.id, [])]),
                "avg_refdomains": _avg([p.refdomains for p in by_cluster.get(c.id, [])]),
            } for c in sorted(clusters, key=lambda c: -c.size)],
            "pages": [{"url": p.url, "title": p.title, "distance": round(p.distance or 0, 4),
                       "bucket": p.bucket, "cluster_id": p.cluster_id,
                       "traffic": p.traffic, "refdomains": p.refdomains, "ur": p.ur}
                      for p in sorted(pages, key=lambda p: (p.distance or 0))],
        })

    def _avg(vals):
        vals = [v for v in vals if v is not None]
        return round(sum(vals) / len(vals), 1) if vals else None

    @bp.route("/api/topics/scan", methods=["POST"])
    def sb_topics_scan():
        data = validate_json(TopicsScanIn)
        domain = (data.domain or load_settings().get("target_site") or "").strip()
        if not domain:
            return jsonify({"error": "No domain given and no target site configured."}), 400
        jid = job_run("topics", lambda j: research.run_topics_scan(
            j, domain, _settings_bg(), data.enrich, data.locale or ""))
        return jsonify({"job_id": jid, "domain": domain})

    # ---------------- Monitoring: Reddit Radar --------------------------

    @bp.route("/api/radar/report")
    def sb_radar_report():
        rep = (sbs.query(SBRadarReport).order_by(SBRadarReport.created_at.desc()).first()
               if not request.args.get("id")
               else sbs.get(SBRadarReport, int(request.args["id"])))
        posts = (sbs.query(SBRedditPost).order_by(SBRedditPost.score.desc()).limit(60).all())
        history = sbs.query(SBRadarReport).order_by(SBRadarReport.created_at.desc()).limit(20).all()
        return jsonify({
            "report": ({"id": rep.id, "week_start": rep.week_start, "summary_md": rep.summary_md,
                        "stats": rep.stats, "created_at": rep.created_at.isoformat()} if rep else None),
            "history": [{"id": h.id, "week_start": h.week_start,
                         "created_at": h.created_at.isoformat()} for h in history],
            "posts": [{"id": p.id, "subreddit": p.subreddit, "title": p.title,
                       "permalink": p.permalink, "score": p.score, "comments": p.num_comments,
                       "matched_query": p.matched_query,
                       "created_utc": p.created_utc.isoformat() if p.created_utc else None}
                      for p in posts],
            "corpus": sbs.query(SBRedditPost).count(),
        })

    @bp.route("/api/radar/scan", methods=["POST"])
    def sb_radar_scan():
        jid = job_run("radar", lambda j: research.run_radar_scan(j, _settings_bg()))
        return jsonify({"job_id": jid})

    # ---------------- Monitoring: Growth Scanner ------------------------

    def _cat_dict(c, full=False):
        snaps = c.snapshots or []
        latest = snaps[-1] if snaps else {}
        out = {"id": c.id, "name": c.name, "primary_seed": c.primary_seed,
               "related_seeds": c.related_seeds or [], "anchors": c.anchors or [],
               "relevant_topic_labels": c.relevant_topic_labels or [],
               "excluded_topics": c.excluded_topics or [], "strip_brands": c.strip_brands,
               "discovery_mode": c.discovery_mode,
               "is_legacy": c.discovery_mode != "parent_topic_v2",
               "snapshot_count": len(snaps),
               "latest": {"pulled_at": latest.get("pulled_at"),
                          "domains": (latest.get("domains") or [])[:10],
                          "keyword_count": latest.get("keyword_count"),
                          "cluster_count": latest.get("cluster_count")},
               "updated_at": c.updated_at.isoformat() if c.updated_at else None}
        if full:
            out["snapshots"] = snaps
        return out

    @bp.route("/api/categories")
    def sb_categories():
        rows = sbs.query(SBCategory).order_by(SBCategory.created_at.desc()).all()
        return jsonify({"categories": [_cat_dict(c) for c in rows]})

    @bp.route("/api/categories/dashboard")
    def sb_cat_dashboard():
        rows = sbs.query(SBCategory).all()
        roll = []
        for c in rows:
            snaps = c.snapshots or []
            if not snaps:
                continue
            latest = snaps[-1]
            doms = latest.get("domains") or []
            roll.append({"id": c.id, "name": c.name, "clusters": latest.get("cluster_count"),
                         "keywords": latest.get("keyword_count"),
                         "leader": (doms[0]["domain"] if doms else None),
                         "leader_share": (doms[0]["traffic_share"] if doms else None),
                         "snapshots": len(snaps),
                         "timeline": [{"pulled_at": s_.get("pulled_at"),
                                       "top": (s_.get("domains") or [{}])[0].get("domain")}
                                      for s_ in snaps]})
        return jsonify({"categories": roll})

    @bp.route("/api/categories/<cid>")
    def sb_category(cid):
        c = sbs.get(SBCategory, cid)
        if not c:
            return jsonify({"error": "not found"}), 404
        return jsonify(_cat_dict(c, full=True))

    @bp.route("/api/category/scan", methods=["POST"])
    def sb_cat_scan():
        data = validate_json(CategoryScanIn)
        jid = job_run("category", lambda j: research.run_category_scan(
            j, data.primary_seed.strip().lower(),
            [s_.strip().lower() for s_ in data.related_seeds if s_.strip()],
            data.strip_brands, _settings_bg()))
        return jsonify({"job_id": jid})

    @bp.route("/api/categories/<cid>/refresh", methods=["POST"])
    def sb_cat_refresh(cid):
        c = sbs.get(SBCategory, cid)
        if not c:
            return jsonify({"error": "not found"}), 404
        jid = job_run("category", lambda j: research.run_category_scan(
            j, c.primary_seed, list(c.related_seeds or []), c.strip_brands,
            _settings_bg(), category_id=cid, mode="refresh"))
        return jsonify({"job_id": jid})

    @bp.route("/api/categories/<cid>/reseed", methods=["POST"])
    def sb_cat_reseed(cid):
        data = validate_json(CategoryScanIn)
        if not sbs.get(SBCategory, cid):
            return jsonify({"error": "not found"}), 404
        jid = job_run("category", lambda j: research.run_category_scan(
            j, data.primary_seed.strip().lower(),
            [s_.strip().lower() for s_ in data.related_seeds if s_.strip()],
            data.strip_brands, _settings_bg(), category_id=cid, mode="reseed"))
        return jsonify({"job_id": jid})

    @bp.route("/api/categories/<cid>/delete-clusters", methods=["POST"])
    def sb_cat_del_clusters(cid):
        data = validate_json(LabelsIn)
        c = sbs.get(SBCategory, cid)
        if not c:
            return jsonify({"error": "not found"}), 404
        drop = {str(x).strip().lower() for x in data.labels}
        c.excluded_topics = sorted(set(list(c.excluded_topics or [])) | drop)
        c.relevant_topic_labels = [x for x in (c.relevant_topic_labels or []) if x not in drop]
        sbs.commit()
        jid = job_run("category", lambda j: research.run_category_scan(
            j, c.primary_seed, list(c.related_seeds or []), c.strip_brands,
            _settings_bg(), category_id=cid, mode="refresh"))
        return jsonify({"job_id": jid, "excluded": c.excluded_topics})

    @bp.route("/api/categories/<cid>/rename", methods=["POST"])
    def sb_cat_rename(cid):
        data = validate_json(RenameIn)
        c = sbs.get(SBCategory, cid)
        if not c:
            return jsonify({"error": "not found"}), 404
        c.name = data.name.strip()[:300]
        sbs.commit()
        return jsonify({"ok": True, "name": c.name})

    @bp.route("/api/categories/<cid>", methods=["DELETE"])
    def sb_cat_delete(cid):
        c = sbs.get(SBCategory, cid)
        if c:
            sbs.delete(c)
            sbs.commit()
        return jsonify({"ok": True})

    # ---------------- Write: Ideas --------------------------------------

    @bp.route("/api/ideas")
    def sb_ideas():
        rows = sbs.query(SBIdea).order_by(SBIdea.created_at.desc()).limit(200).all()
        return jsonify({"ideas": [{"id": r.id, "headline": r.headline, "angle": r.angle,
                                   "keyword_metrics": r.keyword_metrics or [],
                                   "source_item_ids": r.source_item_ids or [],
                                   "created_at": r.created_at.isoformat() if r.created_at else None}
                                  for r in rows]})

    @bp.route("/api/ideas/<int:iid>", methods=["DELETE"])
    def sb_idea_delete(iid):
        r = sbs.get(SBIdea, iid)
        if r:
            sbs.delete(r)
            sbs.commit()
        return jsonify({"ok": True})

    @bp.route("/api/ideas/generate", methods=["POST"])
    def sb_ideas_generate():
        data = validate_json(IdeasIn)
        q = sbs.query(SBItem)
        if data.source_item_ids:
            q = q.filter(SBItem.id.in_(data.source_item_ids))
        rows = q.order_by(SBItem.saved_at.desc()).limit(max(1, min(data.limit, 30))).all()
        scraps = [{"id": r.id, "title": r.title or "",
                   "url": r.url or "",
                   "content": (r.content_md or r.content_text or r.note or "")[:2000]}
                  for r in rows if (r.content_md or r.content_text or r.note or r.title)]
        if not scraps:
            return jsonify({"error": "No saved scraps with text to work from. Save some posts first."}), 400
        jid = job_run("ideas", lambda j: write.run_ideas(j, scraps, _settings_bg()))
        return jsonify({"job_id": jid, "scraps_used": len(scraps)})

    # ---------------- Write: Example finder -----------------------------

    @bp.route("/api/examples")
    def sb_examples():
        return jsonify({"docs": write.search_examples(request.args.get("q", ""),
                                                     limit=int(request.args.get("limit", 20))),
                        "total": sbs.query(SBExampleDoc).count()})

    @bp.route("/api/examples/<int:did>")
    def sb_example_read(did):
        d = sbs.get(SBExampleDoc, did)
        if not d:
            return jsonify({"error": "not found"}), 404
        return jsonify({"id": d.id, "title": d.title, "url": d.url,
                        "content": d.content, "tags": list(d.tags or [])})

    @bp.route("/api/examples", methods=["POST"])
    def sb_example_add():
        data = validate_json(ExampleIn)
        content, title = data.content.strip(), data.title.strip()
        if not content and data.url:
            fetched = research._fetch_text(data.url.strip(), max_chars=40000)
            content = fetched
            if not title:
                m = re.search(r"^#\s+(.+)$", fetched, re.M)
                title = (m.group(1).strip() if m else data.url)[:300]
        if not content:
            return jsonify({"error": "Nothing to store — paste content or give a fetchable URL."}), 400
        d = SBExampleDoc(title=(title or "Untitled")[:300], url=data.url.strip(), content=content)
        sbs.add(d)
        sbs.commit()
        job_run("embed_example", lambda j, i=d.id: (write.embed_example(i),
                                                    __import__("applications._scrapbook_core", fromlist=["x"]).job_done(j, {"id": i})))
        return jsonify({"id": d.id, "title": d.title, "chars": len(content)})

    @bp.route("/api/examples/<int:did>", methods=["DELETE"])
    def sb_example_delete(did):
        d = sbs.get(SBExampleDoc, did)
        if d:
            sbs.delete(d)
            sbs.commit()
        return jsonify({"ok": True})

    # ---------------- Write: Ahrefs weaver ------------------------------

    @bp.route("/api/weaver", methods=["POST"])
    def sb_weaver():
        data = validate_json(WeaverIn)
        jid = job_run("weaver", lambda j: write.run_weaver(
            j, data.draft_md, data.target, data.keywords, _settings_bg()))
        return jsonify({"job_id": jid})

# ---------------------------------------------------------------------------
# Video transcription job
# ---------------------------------------------------------------------------

def _transcribe_job(job_id: str, media_id: str, data_dir: str) -> None:
    """Extract small mono 16kHz mp3 → speech model → store plain transcript."""
    from applications._scrapbook_core import CHEAP_MODEL, job_done, job_fail, job_progress, llm_client
    with cross_session_scope() as s:
        m = s.get(SBMedia, media_id)
        if not m:
            job_fail(job_id, "media not found")
            return
        src = m.cached_path or m.url

    if not src:
        job_fail(job_id, "No file or URL on this media row.")
        return

    job_progress(job_id, "Extracting audio…")
    tmp = tempfile.mkdtemp(prefix="sbtr-", dir=data_dir)
    audio = os.path.join(tmp, "a.mp3")
    try:
        r = subprocess.run(["ffmpeg", "-y", "-i", src, "-vn", "-ac", "1", "-ar", "16000",
                            "-b:a", "48k", audio], capture_output=True, timeout=600)
        if r.returncode != 0 or not os.path.exists(audio):
            raise RuntimeError((r.stderr or b"")[-400:].decode(errors="replace") or "ffmpeg failed")
    except Exception as exc:  # noqa: BLE001
        with cross_session_scope() as s:
            m = s.get(SBMedia, media_id)
            m.transcript_status, m.transcript_error = "failed", f"audio extraction failed: {exc}"
        job_fail(job_id, f"Audio extraction failed: {exc}. You can paste a transcript instead.")
        return

    job_progress(job_id, "Transcribing…")
    try:
        with open(audio, "rb") as fh:
            resp = llm_client().audio.transcriptions.create(
                model="openai/whisper-1", file=fh, response_format="text")
        text = resp if isinstance(resp, str) else getattr(resp, "text", "")
        text = re.sub(r"^(here is|transcript:)\s*", "", (text or "").strip(), flags=re.I)
    except Exception as exc:  # noqa: BLE001
        with cross_session_scope() as s:
            m = s.get(SBMedia, media_id)
            m.transcript_status, m.transcript_error = "failed", str(exc)[:500]
        job_fail(job_id, f"Transcription failed: {exc}. You can paste a transcript instead.")
        return
    finally:
        for f in (audio,):
            try:
                os.remove(f)
            except OSError:
                pass
        try:
            os.rmdir(tmp)
        except OSError:
            pass

    if not text:
        with cross_session_scope() as s:
            m = s.get(SBMedia, media_id)
            m.transcript_status, m.transcript_error = "failed", "empty transcript"
        job_fail(job_id, "The model returned an empty transcript.")
        return

    with cross_session_scope() as s:
        m = s.get(SBMedia, media_id)
        m.transcript, m.transcript_status, m.transcript_error = text, "done", ""
    job_done(job_id, {"chars": len(text)}, f"Transcribed {len(text)} characters")


def _recache_images_job(job_id: str, ctx: dict) -> None:
    """Copy remote post images into local storage so they stop expiring."""
    from applications._scrapbook_core import job_done, job_fail, job_progress
    from applications.scrapbook import _cache_remote_image
    SBItem = ctx["SBItem"]

    with cross_session_scope() as s:
        targets = [(r.id, r.thumbnail_path) for r in
                   s.query(SBItem).filter(SBItem.thumbnail_path.isnot(None)).all()
                   if str(r.thumbnail_path).startswith(("http://", "https://"))]

    if not targets:
        job_done(job_id, {"cached": 0, "failed": 0}, "Every post image is already stored locally")
        return

    cached, failed, expired = 0, 0, []
    for i, (item_id, url) in enumerate(targets, 1):
        job_progress(job_id, f"Caching image {i}/{len(targets)}…")
        path = _cache_remote_image(url)
        if path:
            with cross_session_scope() as s:
                it = s.get(SBItem, item_id)
                if it:
                    it.thumbnail_path = path
                    it.meta = {**(it.meta or {}), "image_cached": True, "image_source_url": url}
            cached += 1
        else:
            failed += 1
            expired.append(item_id)

    msg = f"{cached} image(s) now stored locally"
    if failed:
        msg += (f"; {failed} could not be fetched — those CDN links have expired, "
                f"so re-scrape those posts to restore the picture")
    job_done(job_id, {"cached": cached, "failed": failed, "unrecoverable": expired[:20]}, msg)
