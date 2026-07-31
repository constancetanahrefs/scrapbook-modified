"""Scrapbook — combined marketing brain. Phase 1: Library tab.

Saves URLs, LinkedIn posts (queued for Apify), images, screenshots, rich-text notes,
and Firehose taps. Backed by console_site_db (separate engine) so a future public-site
view can read shared items.
"""

NAME = "Scrapbook"
OWNER = "ahrefs"

import os
import json
import uuid
import hashlib
import mimetypes
import subprocess
import threading
import urllib.parse
import requests
from datetime import datetime, timezone, timedelta
from typing import Optional

from flask import (
    Blueprint, render_template, request, jsonify, abort, send_file, url_for, redirect,
)
from sqlalchemy import (
    ForeignKey, Index, String, Text, Boolean, DateTime,
    func, select, or_, and_,
)
from sqlalchemy.orm import (
    Mapped, mapped_column, relationship,
)
from sqlalchemy.dialects.postgresql import JSONB, ARRAY, TSVECTOR
from contextlib import contextmanager


# ---------------------------------------------------------------------------
# Storage paths
# ---------------------------------------------------------------------------

DATA_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "data", "scrapbook"))
os.makedirs(DATA_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# console_site_db engine/session — shared definition in src.db_cross
# (local aliases keep the rest of this module unchanged)
# ---------------------------------------------------------------------------
from src.db_cross import (  # noqa: E402
    cross_engine as _engine,
    cross_session as sb_session,
    CrossBase as SBBase,
    cross_session_scope as session_scope,
)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class SBBoard(SBBase):
    __tablename__ = "scrapbook_boards"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String(200), unique=True)
    description: Mapped[str] = mapped_column(Text, default="")
    is_public: Mapped[bool] = mapped_column(Boolean, default=False)
    created_by: Mapped[str] = mapped_column(String(200), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class SBItem(SBBase):
    __tablename__ = "scrapbook_items"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    # type: url, linkedin, image, screenshot, richtext, firehose_tap
    type: Mapped[str] = mapped_column(String(32), index=True)
    title: Mapped[str] = mapped_column(String(500), default="")
    url: Mapped[Optional[str]] = mapped_column(String(2000), nullable=True, index=True)
    note: Mapped[str] = mapped_column(Text, default="")          # legacy single note — kept in
    #   sync with notes[0] so older code paths, search and exports keep working.
    # Multiple personal notes: [{id, text, created_at}]. `note` mirrors the first entry.
    notes: Mapped[list] = mapped_column(JSONB, default=list)
    content_md: Mapped[str] = mapped_column(Text, default="")    # fetched/saved markdown body
    content_text: Mapped[str] = mapped_column(Text, default="")  # plain-text fallback / OCR sink
    thumbnail_path: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    file_path: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)  # for images/screenshots
    meta: Mapped[dict] = mapped_column(JSONB, default=dict)      # og:image, author, dimensions, etc.
    tags: Mapped[list] = mapped_column(ARRAY(String), default=list)
    board_id: Mapped[Optional[str]] = mapped_column(String(36), ForeignKey("scrapbook_boards.id", ondelete="SET NULL"), nullable=True, index=True)
    saved_by: Mapped[str] = mapped_column(String(200), default="", index=True)
    saved_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)
    is_public: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    # Scrape lifecycle (for LinkedIn): pending, scraping, done, failed, na
    scrape_status: Mapped[str] = mapped_column(String(16), default="na", index=True)
    scrape_error: Mapped[str] = mapped_column(Text, default="")
    last_scraped_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    # AI note generation (Phase 5b)
    ai_note: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    ai_note_status: Mapped[str] = mapped_column(String(16), default="pending", index=True)  # pending / running / done / failed / na
    ai_note_error: Mapped[str] = mapped_column(Text, default="")
    ai_note_generated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    board = relationship("SBBoard")


class SBChatSession(SBBase):
    __tablename__ = "scrapbook_chat_sessions"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_email: Mapped[str] = mapped_column(String(200), index=True)
    title: Mapped[str] = mapped_column(String(300), default="New chat")
    model: Mapped[str] = mapped_column(String(100), default="anthropic/claude-sonnet-4")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class SBChatMessage(SBBase):
    __tablename__ = "scrapbook_chat_messages"
    id: Mapped[int] = mapped_column(primary_key=True)
    session_id: Mapped[str] = mapped_column(String(36), ForeignKey("scrapbook_chat_sessions.id", ondelete="CASCADE"), index=True)
    role: Mapped[str] = mapped_column(String(16))  # user / assistant
    content: Mapped[str] = mapped_column(Text)
    cited_item_ids: Mapped[list] = mapped_column(ARRAY(String), default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class SBSetting(SBBase):
    """Key/value config (Apify actor id, secret name, etc.)."""
    __tablename__ = "scrapbook_settings"
    key: Mapped[str] = mapped_column(String(80), primary_key=True)
    value: Mapped[str] = mapped_column(Text, default="")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))


class SBFirehoseTap(SBBase):
    """Registered Firehose taps the Scrapbook is watching."""
    __tablename__ = "scrapbook_firehose_taps"
    tap_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(300), default="")
    tap_secret_name: Mapped[str] = mapped_column(String(200), default="")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    last_polled_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class SBFirehoseEvent(SBBase):
    """Buffered events drained from webhook_events for user review."""
    __tablename__ = "scrapbook_firehose_events"
    id: Mapped[int] = mapped_column(primary_key=True)
    event_uid: Mapped[str] = mapped_column(String(120), unique=True, index=True)  # SSE eventId or fingerprint
    tap_id: Mapped[str] = mapped_column(String(64), index=True)
    title: Mapped[str] = mapped_column(String(500), default="")
    url: Mapped[Optional[str]] = mapped_column(String(2000), nullable=True)
    snippet: Mapped[str] = mapped_column(Text, default="")     # short preview (added text / title)
    payload: Mapped[dict] = mapped_column(JSONB, default=dict)  # full enriched event
    matched_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)
    status: Mapped[str] = mapped_column(String(16), default="unread", index=True)  # unread / saved / dismissed
    saved_item_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)


# Create tables right now (separate engine — Console's init_db_app only handles console_db)
SBBase.metadata.create_all(_engine)

# Idempotent migrations for columns added after the table was first created
with _engine.begin() as _mig:
    from sqlalchemy import text as _sql
    _mig.execute(_sql("ALTER TABLE scrapbook_items ADD COLUMN IF NOT EXISTS ai_note JSONB;"))
    _mig.execute(_sql("ALTER TABLE scrapbook_items ADD COLUMN IF NOT EXISTS ai_note_status varchar(16) DEFAULT 'pending';"))
    _mig.execute(_sql("ALTER TABLE scrapbook_items ADD COLUMN IF NOT EXISTS ai_note_error text DEFAULT '';"))
    _mig.execute(_sql("ALTER TABLE scrapbook_items ADD COLUMN IF NOT EXISTS ai_note_generated_at timestamptz;"))
    _mig.execute(_sql("CREATE INDEX IF NOT EXISTS scrapbook_items_ai_note_status_idx ON scrapbook_items(ai_note_status);"))


# ---------------------------------------------------------------------------
# Defaults for the Apify scrape settings (Phase 4)
# ---------------------------------------------------------------------------

DEFAULT_APIFY_ACTOR = "apimaestro/linkedin-post-detail"
DEFAULT_APIFY_SECRET = "apify_main"
API_PROXY_CAPS = "http://127.0.0.1:18081/connectors"


def _get_setting(key: str, default: str = "") -> str:
    row = sb_session.get(SBSetting, key)
    return row.value if row else default


def _set_setting(key: str, value: str):
    row = sb_session.get(SBSetting, key)
    if row:
        row.value = value
    else:
        sb_session.add(SBSetting(key=key, value=value))
    sb_session.commit()

# Add FTS column + trigger + GIN index (idempotent). Can't use STORED generated
# column because to_tsvector('english', ...) isn't IMMUTABLE in Postgres.
with _engine.begin() as _conn:
    from sqlalchemy import text
    _conn.execute(text("ALTER TABLE scrapbook_items ADD COLUMN IF NOT EXISTS search_tsv tsvector;"))
    _conn.execute(text("""
        CREATE OR REPLACE FUNCTION scrapbook_items_tsv_update() RETURNS trigger AS $$
        BEGIN
          NEW.search_tsv :=
            setweight(to_tsvector('english', coalesce(NEW.title,'')), 'A') ||
            setweight(to_tsvector('english', coalesce(NEW.note,'')), 'B') ||
            setweight(to_tsvector('english', coalesce(NEW.url,'')), 'B') ||
            setweight(to_tsvector('english', coalesce(NEW.saved_by,'')), 'C') ||
            setweight(to_tsvector('english', coalesce(NEW.content_text,'')), 'C') ||
            setweight(to_tsvector('english', coalesce(NEW.content_md,'')), 'D') ||
            setweight(to_tsvector('english', array_to_string(coalesce(NEW.tags, ARRAY[]::varchar[]), ' ')), 'B');
          RETURN NEW;
        END $$ LANGUAGE plpgsql;
    """))
    _conn.execute(text("DROP TRIGGER IF EXISTS scrapbook_items_tsv_trg ON scrapbook_items;"))
    _conn.execute(text("""
        CREATE TRIGGER scrapbook_items_tsv_trg BEFORE INSERT OR UPDATE
        ON scrapbook_items FOR EACH ROW EXECUTE FUNCTION scrapbook_items_tsv_update();
    """))
    _conn.execute(text("CREATE INDEX IF NOT EXISTS scrapbook_items_search_idx ON scrapbook_items USING GIN (search_tsv);"))
    _conn.execute(text("CREATE INDEX IF NOT EXISTS scrapbook_items_tags_idx ON scrapbook_items USING GIN (tags);"))


# ---------------------------------------------------------------------------
# Flask blueprint
# ---------------------------------------------------------------------------

blueprint = Blueprint(
    "scrapbook",
    __name__,
    template_folder="../templates/scrapbook",
)


def _current_user():
    """Return (email, name) of the calling member; empty strings if missing."""
    email = request.headers.get("X-Auth-User-Email", "") or ""
    name = urllib.parse.unquote(request.headers.get("X-Auth-User-Name", "") or "")
    return email, name


@blueprint.teardown_request
def _remove_session(_exc):
    sb_session.remove()


# ---------- Pages ----------

@blueprint.route("/")
def index():
    return render_template("scrapbook/index.html")


# ---------- API: boards ----------

@blueprint.route("/api/boards", methods=["GET"])
def list_boards():
    rows = sb_session.query(SBBoard).order_by(SBBoard.created_at.desc()).all()
    return jsonify([{
        "id": b.id, "name": b.name, "description": b.description,
        "is_public": b.is_public, "created_by": b.created_by,
        "created_at": b.created_at.isoformat() if b.created_at else None,
    } for b in rows])


@blueprint.route("/api/boards", methods=["POST"])
def create_board():
    data = request.json or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "name required"}), 400
    email, who = _current_user()
    b = SBBoard(name=name, description=data.get("description", ""), is_public=bool(data.get("is_public")),
                created_by=who or email)
    sb_session.add(b)
    try:
        sb_session.commit()
    except Exception as e:
        sb_session.rollback()
        return jsonify({"error": str(e)}), 400
    return jsonify({"id": b.id})


@blueprint.route("/api/boards/<board_id>", methods=["PATCH"])
def update_board(board_id):
    b = sb_session.get(SBBoard, board_id)
    if not b:
        abort(404)
    data = request.json or {}
    for k in ("name", "description"):
        if k in data:
            setattr(b, k, data[k])
    if "is_public" in data:
        b.is_public = bool(data["is_public"])
    sb_session.commit()
    return jsonify({"ok": True})


@blueprint.route("/api/boards/<board_id>", methods=["DELETE"])
def delete_board(board_id):
    b = sb_session.get(SBBoard, board_id)
    if not b:
        abort(404)
    sb_session.delete(b)
    sb_session.commit()
    return jsonify({"ok": True})


# ---------- API: items ----------

def _finalize_item_create(it: SBItem):
    """Common tail of every create_*_item endpoint: commit, kick off AI-note
    generation in the background, return the item dict."""
    sb_session.commit()
    try:
        # _enqueue_note is defined later in the module — lookup at call time
        globals().get("_enqueue_note", lambda _id: None)(it.id)
    except Exception:
        pass
    return jsonify(_item_to_dict(it))


def _item_to_dict(it: SBItem):
    return {
        "id": it.id, "type": it.type, "title": it.title, "url": it.url,
        "note": it.note, "notes": it.notes or [], "content_md": it.content_md,
        # thumbnail_path holds either a local filesystem path (image/screenshot uploads)
        # or a remote URL (LinkedIn CDN, Firehose snapshot). Surface remote URLs verbatim;
        # only route local files through serve_file.
        "thumbnail_url": (
            it.thumbnail_path if (it.thumbnail_path and it.thumbnail_path.startswith(("http://", "https://", "data:")))
            else (url_for("scrapbook.serve_file", item_id=it.id, kind="thumb") if it.thumbnail_path else None)
        ),
        "file_url": (
            it.file_path if (it.file_path and it.file_path.startswith(("http://", "https://", "data:")))
            else (url_for("scrapbook.serve_file", item_id=it.id, kind="file") if it.file_path else None)
        ),
        "meta": it.meta or {}, "tags": it.tags or [],
        # Every image on the post, in order, for the card carousel. Served through our
        # own route so cached copies are used and expiring CDN urls stay hidden.
        "gallery_urls": [
            url_for("scrapbook.serve_gallery_image", item_id=it.id, idx=i)
            for i in range(len(((it.meta or {}).get("gallery") or [])))
        ],
        # True when the image is a local copy (cannot expire); False when we are
        # still hotlinking a signed CDN url that will eventually 403.
        "cached_image": bool(it.thumbnail_path and not str(it.thumbnail_path).startswith(("http://", "https://"))),
        # Prefer the cached avatar; fall back to LinkedIn's expiring url.
        "avatar_url": (url_for("scrapbook.serve_avatar", item_id=it.id)
                       if (it.meta or {}).get("author_profile_picture_cached")
                       else (it.meta or {}).get("author_profile_picture")),
        "board_id": it.board_id,
        "saved_by": it.saved_by,
        "saved_at": it.saved_at.isoformat() if it.saved_at else None,
        "is_public": it.is_public,
        "scrape_status": it.scrape_status,
        "scrape_error": it.scrape_error,
        "ai_note": it.ai_note,
        "ai_note_status": it.ai_note_status,
        "ai_note_error": it.ai_note_error,
        "ai_note_generated_at": it.ai_note_generated_at.isoformat() if it.ai_note_generated_at else None,
    }


@blueprint.route("/api/items", methods=["GET"])
def list_items():
    q = sb_session.query(SBItem)

    # Filters
    types = request.args.getlist("type")
    if types:
        q = q.filter(SBItem.type.in_(types))
    board_id = request.args.get("board_id")
    if board_id == "none":
        q = q.filter(SBItem.board_id.is_(None))
    elif board_id:
        q = q.filter(SBItem.board_id == board_id)
    tag = request.args.get("tag")
    if tag:
        q = q.filter(SBItem.tags.any(tag))
    search = (request.args.get("q") or "").strip()
    if search:
        from sqlalchemy import text as _sqltext, literal
        # FTS via raw column reference + ILIKE fallback for short / prefix tokens
        like = f"%{search}%"
        q = q.filter(
            or_(
                _sqltext("scrapbook_items.search_tsv @@ plainto_tsquery('english', :sq)").bindparams(sq=search),
                SBItem.title.ilike(like),
                SBItem.note.ilike(like),
                SBItem.url.ilike(like),
            )
        )

    # Date range
    date_from = request.args.get("from")
    date_to = request.args.get("to")
    if date_from:
        try: q = q.filter(SBItem.saved_at >= datetime.fromisoformat(date_from))
        except ValueError: pass
    if date_to:
        try: q = q.filter(SBItem.saved_at <= datetime.fromisoformat(date_to))
        except ValueError: pass

    # Sort
    sort = request.args.get("sort", "newest")
    if sort == "oldest":
        q = q.order_by(SBItem.saved_at.asc())
    elif sort == "title":
        q = q.order_by(func.lower(SBItem.title).asc())
    else:
        q = q.order_by(SBItem.saved_at.desc())

    limit = min(int(request.args.get("limit", 100)), 500)
    offset = int(request.args.get("offset", 0))
    total = q.count()
    rows = q.limit(limit).offset(offset).all()
    return jsonify({"total": total, "items": [_item_to_dict(r) for r in rows]})


@blueprint.route("/api/items/bulk", methods=["POST"])
def bulk_items():
    """Bulk ops: move to board, add/remove tags, delete, toggle public.
    Body: {ids: [...], op: 'move'|'add_tags'|'remove_tags'|'delete'|'public', value: ...}"""
    data = request.json or {}
    ids = data.get("ids") or []
    op = data.get("op")
    value = data.get("value")
    if not ids or not op:
        return jsonify({"error": "ids and op required"}), 400
    rows = sb_session.query(SBItem).filter(SBItem.id.in_(ids)).all()
    affected = 0
    if op == "move":
        for it in rows:
            it.board_id = value or None
            affected += 1
    elif op == "add_tags":
        new_tags = [t.strip() for t in (value or []) if t and t.strip()]
        for it in rows:
            merged = list({*(it.tags or []), *new_tags})
            it.tags = merged
            affected += 1
    elif op == "remove_tags":
        rm = set(value or [])
        for it in rows:
            it.tags = [t for t in (it.tags or []) if t not in rm]
            affected += 1
    elif op == "delete":
        for it in rows:
            for p in (it.file_path, it.thumbnail_path):
                if p and os.path.isfile(p):
                    try: os.remove(p)
                    except OSError: pass
            sb_session.delete(it)
            affected += 1
    elif op == "public":
        for it in rows:
            it.is_public = bool(value)
            affected += 1
    else:
        return jsonify({"error": f"unknown op {op}"}), 400
    sb_session.commit()
    return jsonify({"affected": affected})


@blueprint.route("/api/tags")
def list_all_tags():
    """Distinct tags across the whole scrapbook with counts (for sidebar / pickers)."""
    from sqlalchemy import text as _t
    rows = sb_session.execute(_t(
        "SELECT tag, count(*)::int AS c "
        "FROM scrapbook_items, unnest(tags) AS tag "
        "GROUP BY tag ORDER BY c DESC, tag ASC LIMIT 200"
    )).all()
    return jsonify([{"tag": r[0], "count": r[1]} for r in rows])


@blueprint.route("/api/items/<item_id>", methods=["GET"])
def get_item(item_id):
    it = sb_session.get(SBItem, item_id)
    if not it:
        abort(404)
    return jsonify(_item_to_dict(it))



# ---------- Personal notes (multiple per item) ----------

def _new_note(text: str) -> dict:
    return {"id": uuid.uuid4().hex[:12], "text": text.strip(),
            "created_at": datetime.now(timezone.utc).isoformat()}


def _sync_legacy_note(it):
    """`note` mirrors notes[0] — search, exports and older views still read it."""
    it.note = ((it.notes or [{}])[0].get("text") or "") if it.notes else ""


@blueprint.route("/api/items/<item_id>/notes", methods=["POST"])
def add_note(item_id):
    it = sb_session.get(SBItem, item_id)
    if not it:
        abort(404)
    text = ((request.json or {}).get("text") or "").strip()
    if not text:
        return jsonify({"error": "note text required"}), 400
    it.notes = list(it.notes or []) + [_new_note(text)]
    _sync_legacy_note(it)
    sb_session.commit()
    return jsonify(_item_to_dict(it))


@blueprint.route("/api/items/<item_id>/notes/<note_id>", methods=["PATCH"])
def edit_note(item_id, note_id):
    it = sb_session.get(SBItem, item_id)
    if not it:
        abort(404)
    text = ((request.json or {}).get("text") or "").strip()
    notes = list(it.notes or [])
    idx = next((i for i, n in enumerate(notes) if n.get("id") == note_id), None)
    if idx is None:
        return jsonify({"error": "note not found"}), 404
    if text:
        notes[idx] = {**notes[idx], "text": text}
    else:
        notes.pop(idx)          # clearing the text deletes the note
    it.notes = notes
    _sync_legacy_note(it)
    sb_session.commit()
    return jsonify(_item_to_dict(it))


@blueprint.route("/api/items/<item_id>/notes/<note_id>", methods=["DELETE"])
def delete_note(item_id, note_id):
    it = sb_session.get(SBItem, item_id)
    if not it:
        abort(404)
    it.notes = [n for n in (it.notes or []) if n.get("id") != note_id]
    _sync_legacy_note(it)
    sb_session.commit()
    return jsonify(_item_to_dict(it))


@blueprint.route("/api/items/<item_id>", methods=["PATCH"])
def update_item(item_id):
    it = sb_session.get(SBItem, item_id)
    if not it:
        abort(404)
    data = request.json or {}
    for k in ("title", "note", "url", "content_md", "board_id"):
        if k in data:
            setattr(it, k, data[k])
    if "note" in data:
        # Legacy single-note writes update the first entry of the notes list.
        notes = list(it.notes or [])
        txt = (data.get("note") or "").strip()
        if txt and notes:
            notes[0] = {**notes[0], "text": txt}
        elif txt:
            notes = [_new_note(txt)]
        else:
            notes = notes[1:] if notes else []
        it.notes = notes
        it.note = (notes[0]["text"] if notes else "")
    if "tags" in data:
        it.tags = [t.strip() for t in (data["tags"] or []) if t and t.strip()]
    if "is_public" in data:
        it.is_public = bool(data["is_public"])
    sb_session.commit()
    return jsonify(_item_to_dict(it))


@blueprint.route("/api/items/<item_id>", methods=["DELETE"])
def delete_item(item_id):
    it = sb_session.get(SBItem, item_id)
    if not it:
        abort(404)
    # Remove backing files
    for p in (it.file_path, it.thumbnail_path):
        if p and os.path.isfile(p):
            try:
                os.remove(p)
            except OSError:
                pass
    sb_session.delete(it)
    sb_session.commit()
    return jsonify({"ok": True})


# ---------- API: create item (per type) ----------

def _grant_site_read(path: str) -> None:
    """Make `path` readable by the `site` user so the public site can `send_file()` it.
    Files are written as `console:console` by default; site user is not in group console,
    so we add an explicit ACL entry. Best-effort — swallow errors."""
    try:
        os.chmod(path, 0o664)
        subprocess.run(["setfacl", "-m", "g:site:r", path],
                       check=False, capture_output=True, timeout=5)
    except Exception:
        pass


def _save_bytes(data: bytes, ext: str) -> str:
    """Save bytes to data dir keyed by content hash. Returns absolute path."""
    h = hashlib.sha256(data).hexdigest()[:24]
    ext = ext.lstrip(".") or "bin"
    path = os.path.join(DATA_DIR, f"{h}.{ext}")
    if not os.path.exists(path):
        with open(path, "wb") as f:
            f.write(data)
    _grant_site_read(path)
    return path


def _cache_remote_image(url: str, timeout: int = 20) -> Optional[str]:
    """Download a remote image into DATA_DIR and return the local path.

    LinkedIn's media URLs are SIGNED and expire after a few weeks, so hotlinking
    them means saved posts lose their pictures. We copy the bytes at save time.
    Returns None when the fetch fails (blocked/expired) — callers then fall back
    to the remote URL so behaviour is no worse than before.
    """
    if not url or not url.startswith(("http://", "https://")):
        return None
    try:
        r = requests.get(url, timeout=timeout, headers={
            "User-Agent": "Mozilla/5.0 (compatible; letaido-scrapbook/1.0)"})
        if not r.ok or not r.content:
            return None
        ctype = (r.headers.get("content-type") or "").lower()
        if "image" not in ctype and not re.search(r"\.(jpe?g|png|gif|webp)(\?|$)", url, re.I):
            return None
        ext = ({"image/jpeg": "jpg", "image/jpg": "jpg", "image/png": "png",
                "image/gif": "gif", "image/webp": "webp"}.get(ctype.split(";")[0].strip())
               or (re.search(r"\.(jpe?g|png|gif|webp)(\?|$)", url, re.I).group(1).lower()
                   if re.search(r"\.(jpe?g|png|gif|webp)(\?|$)", url, re.I) else "jpg"))
        return _save_bytes(r.content, ext)
    except Exception:  # noqa: BLE001
        return None



def _dedupe_paragraphs(md: str) -> str:
    """Drop repeated paragraphs the source page renders twice.

    Some sites emit a plain-text teaser of a paragraph and then the real, linked
    version (thestateofbrand.com does this), so a saved scrap shows the same sentences
    back to back. Compare on a normalised form — markdown links flattened to their text,
    punctuation and case removed — and keep the FIRST occurrence, except when a later
    copy is a strict superset of an earlier truncated one, in which case the richer copy
    wins. Short lines (headings, captions, list items) are never deduped.
    """
    if not md or len(md) < 200:
        return md

    def norm(s: str) -> str:
        s = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", s)
        s = re.sub(r"[^a-z0-9 ]", " ", s.lower())
        return " ".join(s.split())

    blocks = re.split(r"(\n\s*\n)", md)          # keep separators
    kept: list[str] = []
    seen: dict[str, int] = {}                      # normalised -> index in `kept`
    for block in blocks:
        if not block.strip() or block.strip().startswith(("#", "-", "*", ">", "|")):
            kept.append(block)
            continue
        n = norm(block)
        if len(n) < 60:                            # too short to judge safely
            kept.append(block)
            continue
        hit = seen.get(n)
        if hit is not None:
            continue                               # exact repeat — drop
        # truncated teaser vs full paragraph
        replaced = False
        for prev_n, idx in list(seen.items()):
            if n.startswith(prev_n[:80]) or prev_n.startswith(n[:80]):
                if len(n) > len(prev_n):           # this copy is richer — swap it in
                    kept[idx] = block
                    del seen[prev_n]
                    seen[n] = idx
                replaced = True
                break
        if replaced:
            continue
        seen[n] = len(kept)
        kept.append(block)
    out = "".join(kept)
    return re.sub(r"\n{3,}", "\n\n", out).strip()


def _fetch_url_content(url: str):
    """Use the web-fetch skill to fetch metadata + markdown.
    Returns dict with `fetch_ok` (bool) and `fetch_error` (str) so callers can decide."""
    try:
        out = subprocess.run(
            # --no-cache: /tmp/web-fetch-cache is agent-owned, and the console user
            # crashes in read_cache without it.
            ["python3", "/opt/letaido/agent/skills/web-fetch/scripts/fetch.py",
             "--no-cache", "--metadata", "--max-length", "20000", url],
            capture_output=True, text=True, timeout=25,
        )
        raw = out.stdout or ""
        err = (out.stderr or "").strip()
    except subprocess.TimeoutExpired:
        return {"title": url, "content_md": "", "meta": {"fetch_error": "timeout after 25s"}, "fetch_ok": False, "fetch_error": "timeout after 25s"}
    except Exception as e:
        return {"title": url, "content_md": "", "meta": {"fetch_error": str(e)}, "fetch_ok": False, "fetch_error": str(e)}

    # The fetch script exits 0 even on failure but writes "all sources failed" + the upstream status
    # to stderr/stdout. Detect this so we don't silently save an empty item.
    if (not raw.strip()) or ("all sources failed" in err) or ("all sources failed" in raw):
        # try to pluck the upstream status from the error blob (e.g. "status=403")
        m = re.search(r"status=(\d+)", err + raw)
        upstream = m.group(1) if m else ""
        msg = f"upstream blocked the fetch{' (HTTP '+upstream+')' if upstream else ''}"
        return {"title": url, "content_md": "", "meta": {"fetch_error": msg}, "fetch_ok": False, "fetch_error": msg}

    # Parse header section
    title = url
    meta = {}
    body_lines = []
    in_body = False
    seen_divider = False
    for line in raw.splitlines():
        if not in_body:
            if line.startswith("## "):
                title = line[3:].strip() or title
                continue
            if line.startswith("URL:") or line.startswith("Source:"):
                continue
            if line.strip() == "---":
                seen_divider = True
                in_body = True
                continue
            if ":" in line:
                k, _, v = line.partition(":")
                k = k.strip().lower()
                if k in ("published", "modified", "author", "site", "description"):
                    meta[k] = v.strip()
                    continue
            if line.strip() == "":
                # blank line before divider — keep waiting
                continue
        else:
            body_lines.append(line)
    if not seen_divider:
        # No divider found — whole stdout is the body sans the first lines
        body_lines = raw.splitlines()[2:]
    body = _dedupe_paragraphs("\n".join(body_lines).strip())
    return {
        "title": meta.get("description") and title or title,
        "content_md": body,
        "meta": meta,
        "fetch_ok": bool(body),
        "fetch_error": "" if body else "empty body",
    }


@blueprint.route("/api/items/url", methods=["POST"])
def create_url_item():
    data = request.json or {}
    url = (data.get("url") or "").strip()
    if not url:
        return jsonify({"error": "url required"}), 400
    email, who = _current_user()

    # Guard against accidental double-saves (double-click, a stuck dialog, an impatient
    # retry). Re-posting the same URL within a short window returns the existing item
    # instead of creating a second copy. `force: true` overrides for a deliberate re-add.
    if not data.get("force"):
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=10)
        dup = (sb_session.query(SBItem)
               .filter(SBItem.url == url, SBItem.saved_at >= cutoff)
               .order_by(SBItem.saved_at.desc()).first())
        if dup:
            return jsonify({"id": dup.id, "duplicate": True,
                            "message": "You saved this URL moments ago — showing the existing scrap."}), 200

    fetched = _fetch_url_content(url)
    fetch_ok = fetched.get("fetch_ok", True)
    fetch_err = fetched.get("fetch_error", "")
    allow_empty = bool(data.get("allow_empty"))
    if (not fetch_ok) and (not allow_empty):
        # Fail fast so the user knows. Front-end can offer "save anyway".
        return jsonify({
            "error": "fetch_failed",
            "message": fetch_err or "Could not retrieve page content",
            "hint": "This site blocks scrapers. Add `allow_empty: true` to save the URL anyway.",
        }), 422
    it = SBItem(
        type="url",
        title=(data.get("title") or fetched.get("title") or url)[:500],
        url=url,
        note=data.get("note", ""),
        content_md=fetched.get("content_md", ""),
        content_text=fetched.get("content_md", "")[:30000],
        meta=fetched.get("meta", {}),
        tags=[t.strip() for t in (data.get("tags") or []) if t.strip()],
        board_id=data.get("board_id") or None,
        is_public=bool(data.get("is_public")),
        saved_by=who or email,
        scrape_status=("done" if fetch_ok else "failed"),
        scrape_error=("" if fetch_ok else fetch_err),
        last_scraped_at=datetime.now(timezone.utc),
    )
    sb_session.add(it)
    return _finalize_item_create(it)


@blueprint.route("/api/items/linkedin", methods=["POST"])
def create_linkedin_item():
    data = request.json or {}
    url = (data.get("url") or "").strip()
    if not url or "linkedin.com" not in url:
        return jsonify({"error": "linkedin url required"}), 400
    email, who = _current_user()
    it = SBItem(
        type="linkedin",
        title=(data.get("title") or "LinkedIn post")[:500],
        url=url,
        note=data.get("note", ""),
        content_md=data.get("content_md", ""),  # optional manual paste
        content_text=data.get("content_md", "")[:30000],
        meta={},
        tags=[t.strip() for t in (data.get("tags") or []) if t.strip()],
        board_id=data.get("board_id") or None,
        is_public=bool(data.get("is_public")),
        saved_by=who or email,
        scrape_status="pending",  # picked up by daily Apify job or manual button
    )
    sb_session.add(it)
    return _finalize_item_create(it)


@blueprint.route("/api/items/richtext", methods=["POST"])
def create_richtext_item():
    data = request.json or {}
    body = (data.get("content_md") or "").strip()
    if not body and not (data.get("title") or "").strip():
        return jsonify({"error": "content required"}), 400
    email, who = _current_user()
    it = SBItem(
        type="richtext",
        title=(data.get("title") or body.splitlines()[0][:120] if body else "Note")[:500],
        note=data.get("note", ""),
        content_md=body,
        content_text=body[:30000],
        tags=[t.strip() for t in (data.get("tags") or []) if t.strip()],
        board_id=data.get("board_id") or None,
        is_public=bool(data.get("is_public")),
        saved_by=who or email,
        scrape_status="done",
    )
    sb_session.add(it)
    return _finalize_item_create(it)


@blueprint.route("/api/items/image", methods=["POST"])
def create_image_item():
    """Multipart upload OR JSON {data_url, ...} for paste-from-clipboard."""
    email, who = _current_user()
    kind = request.args.get("kind", "image")  # image or screenshot
    title = ""
    note = ""
    tags = []
    board_id = None
    is_public = False
    raw = None
    ext = "png"

    if request.files.get("file"):
        f = request.files["file"]
        raw = f.read()
        ext = (os.path.splitext(f.filename or "")[1] or ".png").lstrip(".")
        title = request.form.get("title") or f.filename or "Image"
        note = request.form.get("note", "")
        tags = [t.strip() for t in (request.form.get("tags", "")).split(",") if t.strip()]
        board_id = request.form.get("board_id") or None
        is_public = request.form.get("is_public") in ("1", "true", "on")
    else:
        data = request.json or {}
        durl = data.get("data_url") or ""
        if not durl.startswith("data:"):
            return jsonify({"error": "image data_url required"}), 400
        header, _, b64 = durl.partition(",")
        import base64
        raw = base64.b64decode(b64)
        if "image/" in header:
            ext = header.split("image/")[1].split(";")[0]
        title = data.get("title") or ("Screenshot" if kind == "screenshot" else "Image")
        note = data.get("note", "")
        tags = [t.strip() for t in (data.get("tags") or []) if t.strip()]
        board_id = data.get("board_id") or None
        is_public = bool(data.get("is_public"))

    path = _save_bytes(raw, ext)
    it = SBItem(
        type=kind if kind in ("image", "screenshot") else "image",
        title=title[:500],
        note=note,
        file_path=path,
        thumbnail_path=path,
        meta={"bytes": len(raw), "ext": ext},
        tags=tags,
        board_id=board_id,
        is_public=is_public,
        saved_by=who or email,
        scrape_status="done",
    )
    sb_session.add(it)
    return _finalize_item_create(it)


@blueprint.route("/api/items/firehose_tap", methods=["POST"])
def create_firehose_tap_item():
    """Stub for Phase 5 — stores tap reference; content backfilled when secret available."""
    data = request.json or {}
    tap_id = (data.get("tap_id") or "").strip()
    url = (data.get("url") or "").strip()
    if not tap_id and not url:
        return jsonify({"error": "tap_id or url required"}), 400
    email, who = _current_user()
    it = SBItem(
        type="firehose_tap",
        title=(data.get("title") or f"Firehose tap {tap_id}")[:500],
        url=url or None,
        note=data.get("note", ""),
        content_md=data.get("content_md", ""),
        content_text=data.get("content_md", "")[:30000],
        meta={"tap_id": tap_id},
        tags=[t.strip() for t in (data.get("tags") or []) if t.strip()],
        board_id=data.get("board_id") or None,
        is_public=bool(data.get("is_public")),
        saved_by=who or email,
        scrape_status="pending" if not data.get("content_md") else "done",
    )
    sb_session.add(it)
    return _finalize_item_create(it)


# ---------- File serving ----------

@blueprint.route("/file/<item_id>/avatar")
def serve_avatar(item_id):
    """Serve the locally cached author avatar (LinkedIn's own url expires)."""
    it = sb_session.get(SBItem, item_id)
    if not it:
        abort(404)
    path = (it.meta or {}).get("author_profile_picture_cached")
    if not path or not os.path.exists(path):
        abort(404)
    return send_file(path)


@blueprint.route("/file/<item_id>/gallery/<int:idx>")
def serve_gallery_image(item_id, idx):
    """Serve the nth cached gallery image. Falls back to a redirect to the source url
    when that frame could not be cached (better a possibly-expired image than a 404)."""
    it = sb_session.get(SBItem, item_id)
    if not it:
        abort(404)
    gallery = (it.meta or {}).get("gallery") or []
    if idx < 0 or idx >= len(gallery):
        abort(404)
    entry = gallery[idx] or {}
    path = entry.get("cached")
    if path and os.path.isfile(path):
        mime, _ = mimetypes.guess_type(path)
        return send_file(path, mimetype=mime or "image/jpeg")
    src = entry.get("src")
    if src:
        return redirect(src)
    abort(404)


@blueprint.route("/file/<item_id>/<kind>")
def serve_file(item_id, kind):
    it = sb_session.get(SBItem, item_id)
    if not it:
        abort(404)
    path = it.file_path if kind == "file" else it.thumbnail_path
    if not path or not os.path.isfile(path):
        abort(404)
    mime, _ = mimetypes.guess_type(path)
    return send_file(path, mimetype=mime or "application/octet-stream")


# ---------- Stats ----------

@blueprint.route("/api/stats")
def stats():
    total = sb_session.query(SBItem).count()
    by_type = dict(sb_session.query(SBItem.type, func.count(SBItem.id)).group_by(SBItem.type).all())
    pending = sb_session.query(SBItem).filter(SBItem.scrape_status == "pending").count()
    boards = sb_session.query(SBBoard).count()
    return jsonify({"total": total, "by_type": by_type, "pending_scrape": pending, "boards": boards})


# ---------- LinkedIn scrape (Phase 4) ----------

# Status of any in-progress manual scrape run, keyed by job id.
_scrape_jobs: dict[str, dict] = {}
_scrape_jobs_lock = threading.Lock()
MAX_SCRAPE_BATCH = 25  # urls per Apify invocation


def _apify_scrape_urls(urls: list[str], actor_id: str, secret_name: str, timeout: int = 240) -> list[dict]:
    """Call the LinkedIn-detail Apify actor synchronously. Returns the raw items list."""
    if not urls:
        return []
    payload = {
        "secret_name": secret_name,
        "args": {
            "actor_id": actor_id,
            "input": {"post_urls": urls},
            "timeout_secs": timeout,
            "wait_for_finish": min(timeout, 300),
        },
    }
    r = requests.post(
        f"{API_PROXY_CAPS}/invoke/apify.run_actor_sync_get_dataset_items",
        json=payload, timeout=timeout + 30,
    )
    try:
        d = r.json()
    except Exception:
        raise RuntimeError(f"apify non-json {r.status_code}: {r.text[:200]}")
    if d.get("status") != "ok":
        raise RuntimeError(d.get("error") or f"apify status={d.get('status')}")
    return d.get("result", {}).get("items", []) or []


def _apply_scrape_result(item: SBItem, row: dict):
    """Map one Apify dataset row onto an SBItem in-place.

    The apimaestro/linkedin-post-detail schema delivers:
      post.text, post.created_at, post.url, post.type
      author.name, author.headline, author.profile_url, author.profile_picture, author.followers
      media[] (images / videos), stats { total_reactions, reactions{}, comments, shares }
      job.job_title  (\"This post cannot be displayed\" when URL is unreachable)
    All fields are optional; we code defensively.
    """
    post = (row.get("post") or {}) if isinstance(row, dict) else {}
    author = row.get("author") or {}
    media = row.get("media") or []
    stats = row.get("stats") or {}
    job = row.get("job") or {}
    text_body = (post.get("text") or "").strip()
    failure_hint = (job.get("job_title") or "")

    if not text_body and "cannot be displayed" in failure_hint.lower():
        item.scrape_status = "failed"
        item.scrape_error = failure_hint
        item.last_scraped_at = datetime.now(timezone.utc)
        return

    # title — prefer first non-empty line of body, else author headline
    title_src = (text_body.split("\n", 1)[0] if text_body else "") or (author.get("name") or "") or "LinkedIn post"
    item.title = (title_src[:200]).strip() or "LinkedIn post"
    if text_body:
        item.content_md = text_body
        item.content_text = text_body[:30000]

    # Collect EVERY image in the post, not just the first — a carousel needs them all.
    # Previously only `thumb` survived and the rest were dropped on the floor.
    image_urls: list[str] = []
    for m in media:
        if not isinstance(m, dict):
            continue
        for k in ("image_url", "url", "thumbnail"):
            u = m.get(k)
            if u and isinstance(u, str) and u not in image_urls:
                image_urls.append(u)
                break
    thumb = image_urls[0] if image_urls else None
    if not thumb and author.get("profile_picture"):
        thumb = author["profile_picture"]

    item.meta = {
        "author_name": author.get("name"),
        "author_headline": author.get("headline"),
        "author_profile_url": author.get("profile_url"),
        "author_profile_picture": author.get("profile_picture"),
        "followers": author.get("followers"),
        "post_id": post.get("id"),
        "post_type": post.get("type"),
        "post_created_at": post.get("created_at"),
        "media_count": len(media),
        "image_urls": image_urls,
        "thumbnail_url": thumb,
        "stats": {
            "total_reactions": stats.get("total_reactions"),
            "comments": stats.get("comments"),
            "shares": stats.get("shares"),
        },
    }
    # Cache the image locally: the remote CDN url is signed and expires, which
    # would leave this card with a broken picture in a few weeks.
    cached = _cache_remote_image(thumb) if thumb else None
    item.thumbnail_path = cached or thumb
    if cached:
        item.meta = {**item.meta, "image_cached": True, "image_source_url": thumb}

    # Cache the rest of the gallery too — every LinkedIn CDN url is signed and expires,
    # so hotlinking would leave the carousel full of broken frames within weeks.
    if len(image_urls) > 1:
        gallery = []
        for u in image_urls:
            local = _cache_remote_image(u)
            gallery.append({"cached": local, "src": u})
        item.meta = {**item.meta, "gallery": gallery}

    # Same treatment for the author avatar (also a signed, expiring url).
    avatar = author.get("profile_picture")
    if avatar:
        av_path = _cache_remote_image(avatar)
        if av_path:
            item.meta = {**item.meta,
                         "author_profile_picture_cached": av_path,
                         "author_profile_picture_source": avatar}
    item.scrape_status = "done"
    item.scrape_error = ""
    item.last_scraped_at = datetime.now(timezone.utc)
    # newly-scraped LinkedIn items get a fresh AI note
    item.ai_note_status = "pending"
    item.ai_note_error = ""


def _run_scrape_batch(actor_id: str, secret_name: str) -> dict:
    """Pull all pending LinkedIn items, batch them, call Apify, persist.
    Synchronous — the caller (button handler or cron) blocks until done.
    Returns a summary dict."""
    pending = (sb_session.query(SBItem)
               .filter(SBItem.type == "linkedin", SBItem.scrape_status == "pending")
               .order_by(SBItem.saved_at.asc())
               .all())
    if not pending:
        return {"processed": 0, "succeeded": 0, "failed": 0, "message": "No pending LinkedIn items."}

    # mark all as scraping up-front
    for it in pending:
        it.scrape_status = "scraping"
    sb_session.commit()

    by_url = {it.url: it for it in pending if it.url}
    urls = list(by_url.keys())
    succeeded, failed = 0, 0
    last_error = ""

    for i in range(0, len(urls), MAX_SCRAPE_BATCH):
        chunk = urls[i:i + MAX_SCRAPE_BATCH]
        try:
            rows = _apify_scrape_urls(chunk, actor_id, secret_name)
        except Exception as e:
            last_error = str(e)
            for u in chunk:
                it = by_url.get(u)
                if it:
                    it.scrape_status = "failed"
                    it.scrape_error = last_error[:1000]
                    it.last_scraped_at = datetime.now(timezone.utc)
                    failed += 1
            sb_session.commit()
            continue

        # map rows back to items by URL (actor may strip query/fragment, so match
        # both raw `input` echo and `post.url`).
        for row in rows:
            input_url = (row.get("input") or "") if isinstance(row, dict) else ""
            it = by_url.get(input_url)
            if not it:
                # fallback — try to find by activity id in post.url
                purl = ((row.get("post") or {}).get("url")) or ""
                for u, candidate in by_url.items():
                    if purl and purl.split(":")[-1] in (u or ""):
                        it = candidate
                        break
            if not it:
                continue
            _apply_scrape_result(it, row)
            if it.scrape_status == "done":
                succeeded += 1
            elif it.scrape_status == "failed":
                failed += 1
        sb_session.commit()
        # Generate AI notes for the items that just got fresh content
        for it in pending:
            if it.scrape_status == "done" and it.ai_note_status == "pending":
                globals().get("_enqueue_note", lambda _id: None)(it.id)

    # Any still-marked-as-scraping after Apify returned 0 matching rows — fail them
    leftovers = (sb_session.query(SBItem)
                 .filter(SBItem.id.in_([it.id for it in pending]),
                         SBItem.scrape_status == "scraping")
                 .all())
    for it in leftovers:
        it.scrape_status = "failed"
        it.scrape_error = "No matching row returned by actor"
        it.last_scraped_at = datetime.now(timezone.utc)
        failed += 1
    if leftovers:
        sb_session.commit()

    return {
        "processed": len(pending),
        "succeeded": succeeded,
        "failed": failed,
        "actor": actor_id,
        "last_error": last_error,
    }


@blueprint.route("/api/scrape/run", methods=["POST"])
def scrape_run():
    """Kick off a scrape of all pending LinkedIn items in a background thread.
    Returns a job_id the client can poll."""
    actor_id = _get_setting("apify_actor_id", DEFAULT_APIFY_ACTOR) or DEFAULT_APIFY_ACTOR
    secret_name = _get_setting("apify_secret_name", DEFAULT_APIFY_SECRET) or DEFAULT_APIFY_SECRET
    pending = (sb_session.query(SBItem)
               .filter(SBItem.type == "linkedin", SBItem.scrape_status == "pending")
               .count())
    if not pending:
        return jsonify({"status": "done", "processed": 0, "message": "No pending LinkedIn items to scrape."})

    job_id = str(uuid.uuid4())
    with _scrape_jobs_lock:
        _scrape_jobs[job_id] = {"status": "running", "pending": pending,
                                "started_at": datetime.now(timezone.utc).isoformat()}

    def _bg():
        try:
            summary = _run_scrape_batch(actor_id, secret_name)
            with _scrape_jobs_lock:
                _scrape_jobs[job_id] = {"status": "done", **summary,
                                        "finished_at": datetime.now(timezone.utc).isoformat()}
        except Exception as e:
            with _scrape_jobs_lock:
                _scrape_jobs[job_id] = {"status": "error", "error": str(e),
                                        "finished_at": datetime.now(timezone.utc).isoformat()}
        finally:
            sb_session.remove()

    threading.Thread(target=_bg, daemon=True).start()
    return jsonify({"status": "started", "job_id": job_id, "pending": pending})


@blueprint.route("/api/scrape/run/<job_id>")
def scrape_run_status(job_id):
    with _scrape_jobs_lock:
        st = _scrape_jobs.get(job_id)
    if not st:
        return jsonify({"status": "unknown"}), 404
    return jsonify(st)


@blueprint.route("/api/settings", methods=["GET"])
def get_settings():
    return jsonify({
        "apify_actor_id": _get_setting("apify_actor_id", DEFAULT_APIFY_ACTOR) or DEFAULT_APIFY_ACTOR,
        "apify_secret_name": _get_setting("apify_secret_name", DEFAULT_APIFY_SECRET) or DEFAULT_APIFY_SECRET,
        "firehose_secret_name": _get_setting("firehose_secret_name", "firehose_main") or "firehose_main",
    })


@blueprint.route("/api/settings", methods=["PATCH"])
def update_settings():
    data = request.json or {}
    for k in ("apify_actor_id", "apify_secret_name", "firehose_secret_name"):
        if k in data:
            _set_setting(k, str(data[k]).strip())
    return jsonify({"ok": True})


# ---------- Firehose taps (Phase 5) ----------

# The live subscription was created against "scrapbook-firehose-ingest". The old
# ack-before-store drain advanced that cursor past ~3186 events, and cursors only move
# FORWARD (ack rejects a lower through_id), so "-v2" was added to re-read the backlog.
# We now drain BOTH names: v1 keeps up with whatever the existing subscription acks
# against, v2 covers the recovered backlog. Dedupe is by event_uid, so overlap is safe.
FIREHOSE_INGEST_CURSOR = "scrapbook-firehose-ingest-v2"
FIREHOSE_INGEST_CURSORS = ["scrapbook-firehose-ingest-v2", "scrapbook-firehose-ingest"]
API_PROXY_WEBHOOKS = "http://127.0.0.1:18081/webhooks/events"


def _firehose_invoke(cap_id: str, args: dict, secret_name: str = "firehose_main") -> dict:
    r = requests.post(
        f"{API_PROXY_CAPS}/invoke/{cap_id}",
        json={"secret_name": secret_name, "args": args},
        timeout=30,
    )
    try:
        d = r.json()
    except Exception:
        raise RuntimeError(f"firehose non-json {r.status_code}: {r.text[:200]}")
    if d.get("status") != "ok":
        raise RuntimeError(d.get("error") or f"status={d.get('status')}")
    return d.get("result", {})


@blueprint.route("/api/firehose/available")
def firehose_available_taps():
    """Pull live tap list from Firehose, mark which are already registered."""
    try:
        res = _firehose_invoke("firehose.list_taps", {})
    except Exception as e:
        return jsonify({"error": str(e), "taps": []}), 200
    registered = {t.tap_id for t in sb_session.query(SBFirehoseTap).all()}
    taps = [{"id": t["id"], "name": t.get("name") or t["id"],
             "is_registered": t["id"] in registered}
            for t in (res.get("taps") or [])]
    return jsonify({"taps": taps, "count": len(taps)})


@blueprint.route("/api/firehose/taps", methods=["GET"])
def list_firehose_taps():
    rows = sb_session.query(SBFirehoseTap).order_by(SBFirehoseTap.created_at.desc()).all()
    out = []
    for t in rows:
        unread = (sb_session.query(SBFirehoseEvent)
                  .filter(SBFirehoseEvent.tap_id == t.tap_id,
                          SBFirehoseEvent.status == "unread").count())
        out.append({
            "tap_id": t.tap_id, "name": t.name, "is_active": t.is_active,
            "tap_secret_name": t.tap_secret_name,
            "last_polled_at": t.last_polled_at.isoformat() if t.last_polled_at else None,
            "unread": unread,
        })
    return jsonify(out)


@blueprint.route("/api/firehose/taps", methods=["POST"])
def register_firehose_tap():
    data = request.json or {}
    tap_id = (data.get("tap_id") or "").strip()
    if not tap_id:
        return jsonify({"error": "tap_id required"}), 400
    if sb_session.get(SBFirehoseTap, tap_id):
        return jsonify({"error": "already registered"}), 400
    # The tap_token secret follows the firehose-tap-<short_id> convention.
    short = tap_id.split("-", 1)[0]
    secret_name = data.get("tap_secret_name") or f"firehose-tap-{short}"
    t = SBFirehoseTap(tap_id=tap_id, name=data.get("name", ""),
                      tap_secret_name=secret_name, is_active=True)
    sb_session.add(t)
    sb_session.commit()
    return jsonify({"ok": True, "tap_id": tap_id})


@blueprint.route("/api/firehose/taps/<tap_id>", methods=["PATCH"])
def update_firehose_tap(tap_id):
    t = sb_session.get(SBFirehoseTap, tap_id)
    if not t:
        return jsonify({"error": "not found"}), 404
    data = request.json or {}
    if "is_active" in data: t.is_active = bool(data["is_active"])
    if "name" in data: t.name = str(data["name"])[:300]
    sb_session.commit()
    return jsonify({"ok": True})


@blueprint.route("/api/firehose/taps/<tap_id>", methods=["DELETE"])
def delete_firehose_tap(tap_id):
    t = sb_session.get(SBFirehoseTap, tap_id)
    if not t:
        return jsonify({"error": "not found"}), 404
    sb_session.delete(t)
    sb_session.commit()
    return jsonify({"ok": True})


def _drain_firehose_events_once() -> dict:
    """Pull events from webhook_events queue and write into scrapbook_firehose_events.
    Idempotent on event_uid. Returns counts."""
    inserted, skipped, errors = 0, 0, 0
    last_error = ""
    for _cursor in FIREHOSE_INGEST_CURSORS:
        _i, _s, _e, _le = _drain_one_cursor(_cursor)
        inserted += _i; skipped += _s; errors += _e
        last_error = _le or last_error
    now = datetime.now(timezone.utc)
    sb_session.query(SBFirehoseTap).filter(SBFirehoseTap.is_active == True).update({SBFirehoseTap.last_polled_at: now})  # noqa: E712
    sb_session.commit()
    return {"inserted": inserted, "skipped": skipped, "errors": errors, "last_error": last_error}


def _drain_one_cursor(cursor_name: str) -> tuple:
    """Drain a single named cursor. Returns (inserted, skipped, errors, last_error)."""
    inserted, skipped, errors = 0, 0, 0
    last_error = ""
    while True:
        # READ WITHOUT ACKING. Acking up-front (the old behaviour) advanced the cursor
        # past events that were then dropped by a parsing bug — they were gone for good
        # and the drain reported a clean "inserted: 0". Only ack AFTER a successful store.
        try:
            r = requests.get(
                API_PROXY_WEBHOOKS,
                params={"cursor": cursor_name, "provider": "firehose",
                        "ack": "false", "limit": 100},
                timeout=15,
            )
            batch = r.json()
        except Exception as e:
            errors += 1
            last_error = str(e)
            break
        if not isinstance(batch, list) or not batch:
            break
        max_stored_id = 0
        for ev in batch:
            payload = ev.get("payload") or {}
            if not isinstance(payload, dict):
                skipped += 1; continue
            # The firehose payload carries {tap_id, document, event_id, matched_at} at the
            # TOP level. An earlier version expected a nested `data` envelope, so every
            # single event failed this check and was skipped.
            data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
            event_type = (ev.get("event_type") or data.get("event_type")
                          or payload.get("event_type") or "")
            if event_type and event_type != "update":
                skipped += 1; continue
            doc = data.get("document") or {}
            tap_id = data.get("tap_id") or payload.get("tap_id") or ""
            sse_id = (data.get("event_id") or payload.get("event_id")
                      or ev.get("event_id") or str(ev.get("id", "")))
            event_uid = f"{tap_id}:{sse_id}" if tap_id else f"unknown:{sse_id}"
            if not event_uid or event_uid == "unknown:":
                skipped += 1; continue
            # Skip if already stored
            existing = (sb_session.query(SBFirehoseEvent)
                        .filter(SBFirehoseEvent.event_uid == event_uid).first())
            if existing:
                skipped += 1; continue
            url = doc.get("url") or ""
            title = (doc.get("title") or "")[:500]
            # `added`/`summary` aren't present on these payloads; the readable text lives
            # in document.diff.chunks[] as inserted fragments.
            snippet = (doc.get("added") or doc.get("summary") or "")
            if not snippet:
                chunks = ((doc.get("diff") or {}).get("chunks") or [])
                snippet = " ".join(
                    str(c.get("text") or "") for c in chunks
                    if isinstance(c, dict) and c.get("typ") in (None, "ins"))
            snippet = (snippet or "")[:2000]
            matched_at_raw = data.get("matched_at") or doc.get("matched_at")
            matched_at = None
            if matched_at_raw:
                try:
                    matched_at = datetime.fromisoformat(matched_at_raw.replace("Z", "+00:00"))
                except Exception:
                    pass
            fe = SBFirehoseEvent(
                event_uid=event_uid, tap_id=tap_id, title=title or url or "Firehose event",
                url=url, snippet=snippet, payload=data, matched_at=matched_at,
                status="unread",
            )
            sb_session.add(fe)
            try:
                sb_session.commit()
                inserted += 1
                max_stored_id = max(max_stored_id, int(ev.get("id") or 0))
            except Exception as e:
                sb_session.rollback()
                errors += 1
                last_error = str(e)

        # Ack only what we actually persisted (or deliberately skipped). If a store failed
        # the cursor stays put so the next Refresh retries instead of losing the event.
        ack_through = max_stored_id if errors else max(
            (int(e.get("id") or 0) for e in batch), default=0)
        if ack_through:
            try:
                requests.post(f"{API_PROXY_WEBHOOKS}/ack",
                              json={"cursor": cursor_name,
                                    "through_id": ack_through}, timeout=15)
            except Exception as e:  # noqa: BLE001
                errors += 1
                last_error = f"ack failed: {e}"
                break
        if len(batch) < 100:
            break
    return inserted, skipped, errors, last_error


@blueprint.route("/api/firehose/drain", methods=["POST"])
def firehose_drain():
    """Drain the webhook_events queue. Manual button + hourly cron both hit this."""
    return jsonify(_drain_firehose_events_once())


@blueprint.route("/api/firehose/events")
def list_firehose_events():
    status = request.args.get("status", "unread")
    tap_id = request.args.get("tap_id")
    limit = min(int(request.args.get("limit", 50)), 200)
    q = sb_session.query(SBFirehoseEvent)
    if status and status != "all":
        q = q.filter(SBFirehoseEvent.status == status)
    if tap_id:
        q = q.filter(SBFirehoseEvent.tap_id == tap_id)
    q = q.order_by(SBFirehoseEvent.received_at.desc()).limit(limit)
    return jsonify([{
        "id": e.id, "event_uid": e.event_uid, "tap_id": e.tap_id,
        "title": e.title, "url": e.url, "snippet": e.snippet,
        "received_at": e.received_at.isoformat() if e.received_at else None,
        "matched_at": e.matched_at.isoformat() if e.matched_at else None,
        "status": e.status, "saved_item_id": e.saved_item_id,
    } for e in q.all()])


@blueprint.route("/api/firehose/events/<int:eid>/save", methods=["POST"])
def firehose_event_save(eid):
    """Promote a buffered event into a scrapbook item (type=firehose_tap)."""
    e = sb_session.get(SBFirehoseEvent, eid)
    if not e:
        return jsonify({"error": "not found"}), 404
    data = request.json or {}
    email, who = _current_user()
    body_md = e.snippet or ""
    if e.url and body_md and e.url not in body_md:
        body_md = f"[{e.url}]({e.url})\n\n{body_md}"
    elif e.url and not body_md:
        body_md = f"[{e.url}]({e.url})"
    it = SBItem(
        type="firehose_tap",
        title=(e.title or "Firehose event")[:500],
        url=e.url,
        note=data.get("note", ""),
        content_md=body_md,
        content_text=body_md[:30000],
        meta={"tap_id": e.tap_id, "event_uid": e.event_uid,
              "matched_at": e.matched_at.isoformat() if e.matched_at else None,
              "firehose_payload": e.payload},
        tags=[t.strip() for t in (data.get("tags") or []) if t.strip()],
        board_id=data.get("board_id") or None,
        is_public=bool(data.get("is_public")),
        saved_by=who or email or "firehose",
        scrape_status="done",
        last_scraped_at=datetime.now(timezone.utc),
    )
    sb_session.add(it)
    sb_session.flush()
    e.status = "saved"
    e.saved_item_id = it.id
    sb_session.commit()
    _enqueue_note(it.id)
    return jsonify({"ok": True, "item_id": it.id})


@blueprint.route("/api/firehose/events/<int:eid>/dismiss", methods=["POST"])
def firehose_event_dismiss(eid):
    e = sb_session.get(SBFirehoseEvent, eid)
    if not e:
        return jsonify({"error": "not found"}), 404
    e.status = "dismissed"
    sb_session.commit()
    return jsonify({"ok": True})


# ===========================================================================
# Phase 5b — AI structured notes per item
# ===========================================================================

from src.llm import console_openai_client  # noqa: E402
import base64  # noqa: E402
import re  # noqa: E402
try:
    from json_repair import repair_json as _repair_json  # noqa: E402
except Exception:  # graceful fallback if package not yet installed
    _repair_json = None

_llm_client = console_openai_client(app_slug="applications:scrapbook")
NOTE_MODEL = "anthropic/claude-haiku-4.5"
VISION_TYPES = {"image", "screenshot"}
NOTE_MAX_INPUT_CHARS = 18000

NOTE_SYSTEM_PROMPT = (
    "You read marketing/research inspiration items saved by a SaaS marketing team "
    "(Ahrefs) and extract a dense structured note. Every claim and data point MUST "
    "be backed by a verbatim quote from the source you were given. If the source is "
    "too thin to extract anything specific, return empty arrays for that field rather "
    "than inventing content. Return JSON ONLY, no markdown."
)

NOTE_USER_TEMPLATE = (
    "Source title: {title}\n"
    "Source URL: {url}\n"
    "Source type: {type}\n\n"
    "Source content:\n---\n{content}\n---\n\n"
    "Return a JSON object with exactly this shape:\n"
    "{{\n"
    '  "summary": "one short sentence describing what this item is about",\n'
    '  "bullets": ["key bullet point 1", "key bullet point 2", ...],   // 3-7 bullets\n'
    '  "claims": [{{"claim": "...", "quote": "verbatim snippet that supports it", "source_url": "{url}"}}, ...],\n'
    '  "data_points": [{{"metric": "e.g. CTR, MRR, ROI", "value": "e.g. 12%, $5M", "quote": "verbatim snippet", "source_url": "{url}"}}, ...],\n'
    '  "topics": ["short tag-like topic", ...]   // 2-6 topics\n'
    "}}\n\n"
    "Rules:\n"
    "- Every claim and data_point MUST include a verbatim quote substring from the source.\n"
    "- If no quantitative data points exist, return an empty array.\n"
    "- Keep bullets concrete and specific. Avoid generic platitudes.\n"
    "- Topics should be lowercase, hyphenated, brand/concept names preserved.\n"
    "- Output JSON only. No surrounding prose, no markdown code fences."
)

_note_jobs: dict[str, dict] = {}
_note_jobs_lock = threading.Lock()


def _build_note_messages_for_item(it: SBItem):
    """Return the chat-completions `messages` list, with vision multipart for image types."""
    is_image = it.type in VISION_TYPES and it.file_path and os.path.exists(it.file_path)
    title = (it.title or "")[:300]
    url = (it.url or "")
    note_user = NOTE_USER_TEMPLATE.format(
        title=title or "(no title)",
        url=url or "(no url)",
        type=it.type,
        content=((it.content_md or it.content_text or it.note or "(no text content; see image)")[:NOTE_MAX_INPUT_CHARS]),
    )
    messages = [{"role": "system", "content": NOTE_SYSTEM_PROMPT}]
    if is_image:
        try:
            with open(it.file_path, "rb") as f:
                raw = f.read(8 * 1024 * 1024)  # cap at 8MB
            mt = mimetypes.guess_type(it.file_path)[0] or "image/png"
            b64 = base64.b64encode(raw).decode()
            messages.append({
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": f"data:{mt};base64,{b64}"}},
                    {"type": "text", "text": note_user},
                ],
            })
        except Exception:
            # fall back to text-only
            messages.append({"role": "user", "content": note_user})
    else:
        messages.append({"role": "user", "content": note_user})
    return messages


def _parse_loose_json(txt: str) -> dict:
    """Best-effort JSON parse for LLM output that may contain unescaped quotes,
    trailing commas, single quotes, etc. Strategy:
      1. strip code fences, slice to outermost {...}
      2. try strict json.loads
      3. try json_repair if installed
      4. progressively truncate from the end (recover at last well-formed point)
      5. give up with raw fragment in 'summary'
    """
    txt = (txt or "").strip()
    if txt.startswith("```"):
        lines = txt.split("\n")
        if lines[0].startswith("```"): lines = lines[1:]
        if lines and lines[-1].startswith("```"): lines = lines[:-1]
        txt = "\n".join(lines).strip()
    if "{" in txt and "}" in txt:
        txt = txt[txt.index("{"):txt.rindex("}") + 1]
    # (1) strict
    try:
        return json.loads(txt)
    except Exception:
        pass
    # (2) json_repair package, if present
    if _repair_json is not None:
        try:
            return json.loads(_repair_json(txt))
        except Exception:
            pass
    # (3) crude trailing-comma fix + retry
    cleaned = re.sub(r",\s*([}\]])", r"\1", txt)
    try:
        return json.loads(cleaned)
    except Exception:
        pass
    # (4) bisect: peel chars off the end until parseable, then close braces/brackets
    open_braces = 0
    open_brackets = 0
    in_str = False
    esc = False
    best_end = -1
    for i, ch in enumerate(cleaned):
        if esc: esc = False; continue
        if ch == "\\" and in_str: esc = True; continue
        if ch == '"' and not esc: in_str = not in_str; continue
        if in_str: continue
        if ch == "{": open_braces += 1
        elif ch == "}": open_braces -= 1
        elif ch == "[": open_brackets += 1
        elif ch == "]": open_brackets -= 1
        # candidate cut points: after a value that closes a section
        if ch in "}]," and open_braces >= 0 and open_brackets >= 0:
            best_end = i
    if best_end > 0:
        candidate = cleaned[:best_end + 1].rstrip(",")
        # close any still-open brackets/braces
        # recount
        ob = candidate.count("{") - candidate.count("}")
        obr = candidate.count("[") - candidate.count("]")
        candidate += "]" * max(0, obr) + "}" * max(0, ob)
        try:
            return json.loads(candidate)
        except Exception:
            pass
    # (5) last resort: pull out summary via regex
    summary_m = re.search(r'"summary"\s*:\s*"([^"\\]*(?:\\.[^"\\]*)*)"', cleaned)
    return {"summary": (summary_m.group(1) if summary_m else cleaned[:400]), "_parse_failed": True}


def _validate_note(raw: str) -> dict:
    """Parse the LLM output (tolerant) and normalize the shape."""
    data = _parse_loose_json(raw or "")
    if not isinstance(data, dict):
        data = {"summary": str(data)[:400], "_parse_failed": True}
    out = {
        "summary": str(data.get("summary", ""))[:500],
        "bullets": [str(b)[:400] for b in (data.get("bullets") or []) if isinstance(b, (str, int, float))][:10],
        "claims": [],
        "data_points": [],
        "topics": [str(t)[:60] for t in (data.get("topics") or []) if isinstance(t, (str, int, float))][:8],
    }
    for c in (data.get("claims") or [])[:15]:
        if isinstance(c, dict) and c.get("claim"):
            out["claims"].append({
                "claim": str(c.get("claim", ""))[:600],
                "quote": str(c.get("quote", ""))[:600],
                "source_url": str(c.get("source_url", ""))[:1000],
            })
    for d in (data.get("data_points") or [])[:15]:
        if isinstance(d, dict) and (d.get("metric") or d.get("value")):
            out["data_points"].append({
                "metric": str(d.get("metric", ""))[:200],
                "value": str(d.get("value", ""))[:200],
                "quote": str(d.get("quote", ""))[:600],
                "source_url": str(d.get("source_url", ""))[:1000],
            })
    return out


def _generate_note_for_item(item_id: str):
    """Background-thread worker. Loads its own session."""
    s = _SessionLocal()
    try:
        it = s.get(SBItem, item_id)
        if not it:
            return
        # Skip if the item has no useful payload at all
        has_text = bool((it.content_md or it.content_text or it.note or it.title or "").strip())
        is_vision = it.type in VISION_TYPES and it.file_path and os.path.exists(it.file_path)
        if not has_text and not is_vision:
            it.ai_note_status = "na"
            it.ai_note_error = "No content to summarise yet"
            s.commit()
            return
        it.ai_note_status = "running"
        it.ai_note_error = ""
        s.commit()
        try:
            messages = _build_note_messages_for_item(it)
            # Ask provider to enforce JSON output where supported; fall back if model rejects.
            try:
                resp = _llm_client.chat.completions.create(
                    model=NOTE_MODEL, messages=messages, temperature=0.2, max_tokens=1500,
                    response_format={"type": "json_object"},
                )
            except Exception:
                resp = _llm_client.chat.completions.create(
                    model=NOTE_MODEL, messages=messages, temperature=0.2, max_tokens=1500,
                )
            raw = resp.choices[0].message.content or ""
            note = _validate_note(raw)
            note["model"] = NOTE_MODEL
            note["generated_at"] = datetime.now(timezone.utc).isoformat()
            it.ai_note = note
            it.ai_note_status = "done"
            it.ai_note_generated_at = datetime.now(timezone.utc)
            it.ai_note_error = ""
        except Exception as e:
            it.ai_note_status = "failed"
            it.ai_note_error = str(e)[:2000]
        s.commit()
    finally:
        s.close()


def _enqueue_note(item_id: str):
    """Fire-and-forget background note generation."""
    threading.Thread(target=_generate_note_for_item, args=(item_id,), daemon=True).start()


@blueprint.route("/api/items/<item_id>/note", methods=["POST"])
def regenerate_note(item_id):
    it = sb_session.get(SBItem, item_id)
    if not it:
        return jsonify({"error": "not found"}), 404
    it.ai_note_status = "pending"
    it.ai_note_error = ""
    sb_session.commit()
    _enqueue_note(item_id)
    return jsonify({"ok": True, "status": "pending"})


@blueprint.route("/api/admin/refresh_acls", methods=["POST"])
def refresh_acls():
    """Walk DATA_DIR and grant `site` user read permission on every file.
    Also ensures the site user can traverse the parent dir chain (Console runs
    as `console`, which owns those dirs).
    One-shot fix for older files saved before _grant_site_read existed."""
    fixed, errors = 0, 0
    # Walk up from DATA_DIR to /home/console/http; site must traverse the whole chain.
    # /home and /home/console are already o+x (world-traversable); the gated ones are
    # /home/console/http, /home/console/http/default, /home/console/http/default/data,
    # plus the legacy applications/ if anyone still references it.
    parent_steps = [
        "/home/console/http",
        "/home/console/http/default",
        os.path.dirname(DATA_DIR),                       # .../data
        os.path.join(os.path.dirname(os.path.dirname(DATA_DIR)), "applications"),
        DATA_DIR,                                        # .../data/scrapbook
    ]
    for parent in parent_steps:
        if not os.path.isdir(parent):
            continue
        try:
            subprocess.run(["setfacl", "-m", "g:site:x", parent],
                           check=False, capture_output=True, timeout=5)
        except Exception as e:
            errors += 1
    # Make scrapbook dir itself readable + new files inherit site:r
    try:
        subprocess.run(["setfacl", "-m", "g:site:rx", DATA_DIR],
                       check=False, capture_output=True, timeout=5)
        subprocess.run(["setfacl", "-d", "-m", "g:site:r", DATA_DIR],
                       check=False, capture_output=True, timeout=5)
    except Exception:
        errors += 1
    # Also normalize any DB rows still holding the `applications/..` form
    normalized_rows = 0
    bad_prefix = "/home/console/http/default/applications/../data/scrapbook/"
    good_prefix = DATA_DIR + "/"
    for col in (SBItem.file_path, SBItem.thumbnail_path):
        rows = sb_session.query(SBItem).filter(col.like(bad_prefix + "%")).all()
        for r in rows:
            if r.file_path and r.file_path.startswith(bad_prefix):
                r.file_path = good_prefix + r.file_path[len(bad_prefix):]
            if r.thumbnail_path and r.thumbnail_path.startswith(bad_prefix):
                r.thumbnail_path = good_prefix + r.thumbnail_path[len(bad_prefix):]
            normalized_rows += 1
    sb_session.commit()
    for name in os.listdir(DATA_DIR):
        p = os.path.join(DATA_DIR, name)
        if not os.path.isfile(p):
            continue
        try:
            _grant_site_read(p)
            fixed += 1
        except Exception:
            errors += 1
    return jsonify({"fixed": fixed, "errors": errors, "parent_acls_set": len(parent_steps), "normalized_rows": normalized_rows})


@blueprint.route("/api/notes/backfill", methods=["POST"])
def notes_backfill():
    """Kick off note generation for every item lacking a done note.
    Background. Returns a count and a job_id the UI can poll."""
    pending = (sb_session.query(SBItem)
               .filter(SBItem.ai_note_status.in_(["pending", "failed", "running"]) |
                       SBItem.ai_note.is_(None))
               .order_by(SBItem.saved_at.desc())
               .all())
    ids = [it.id for it in pending]
    if not ids:
        return jsonify({"status": "done", "processed": 0, "message": "All items already have notes."})

    job_id = str(uuid.uuid4())
    with _note_jobs_lock:
        _note_jobs[job_id] = {"status": "running", "total": len(ids), "done": 0,
                              "started_at": datetime.now(timezone.utc).isoformat()}

    def _bulk():
        for i, iid in enumerate(ids, 1):
            try:
                _generate_note_for_item(iid)
            except Exception:
                pass
            with _note_jobs_lock:
                if job_id in _note_jobs:
                    _note_jobs[job_id]["done"] = i
        with _note_jobs_lock:
            _note_jobs[job_id]["status"] = "done"
            _note_jobs[job_id]["finished_at"] = datetime.now(timezone.utc).isoformat()

    threading.Thread(target=_bulk, daemon=True).start()
    return jsonify({"status": "started", "job_id": job_id, "total": len(ids)})


@blueprint.route("/api/notes/backfill/<job_id>")
def notes_backfill_status(job_id):
    with _note_jobs_lock:
        st = _note_jobs.get(job_id)
    if not st:
        return jsonify({"status": "unknown"}), 404
    return jsonify(st)


# ===========================================================================
# Phase 3 — AI tab (RAG over scrapbook items)
# ===========================================================================

MODEL_OPTIONS = [
    {"id": "anthropic/claude-sonnet-4.6", "label": "Claude Sonnet 4.6 (reasoning)"},
    {"id": "openai/gpt-5", "label": "GPT-5"},
    {"id": "google/gemini-2.5-pro", "label": "Gemini 2.5 Pro (huge context)"},
    {"id": "google/gemini-3-flash-preview", "label": "Gemini 3 Flash (fast)"},
]
DEFAULT_MODEL = MODEL_OPTIONS[0]["id"]

PRESET_PROMPTS = [
    {"id": "trends", "label": "Find trends",
     "prompt": "Analyse my saved items and identify recurring themes, emerging trends, and notable patterns. Group by topic and cite the items."},
    {"id": "ideate", "label": "Ideate content",
     "prompt": "Based on the saved items, brainstorm 10 content ideas (blog posts, social, video) Ahrefs marketing could create. For each idea, cite the source items that inspired it."},
    {"id": "weekly", "label": "Summarize last 7 days",
     "prompt": "Give a concise digest of everything saved in the past 7 days. Group by theme; cite each item."},
    {"id": "competitors", "label": "Competitor moves",
     "prompt": "What are competitors and the wider SEO industry doing based on these saved items? Highlight tactics, launches, and threats."},
]


# Per-job storage (background thread → polling)
_chat_jobs: dict = {}
_chat_jobs_lock = threading.Lock()


def _select_rag_items(scope: dict, max_items: int = 60, max_chars_per_item: int = 1800):
    """Pull items matching scope filters, ordered newest first.
    scope keys: board_id, types, tags, date_from, date_to, q (search), limit.
    Returns list of SBItem rows.
    """
    q = sb_session.query(SBItem)
    if scope.get("board_id"):
        bid = scope["board_id"]
        if bid == "none":
            q = q.filter(SBItem.board_id.is_(None))
        else:
            q = q.filter(SBItem.board_id == bid)
    if scope.get("types"):
        q = q.filter(SBItem.type.in_(scope["types"]))
    if scope.get("tags"):
        for t in scope["tags"]:
            q = q.filter(SBItem.tags.any(t))
    if scope.get("date_from"):
        try: q = q.filter(SBItem.saved_at >= datetime.fromisoformat(scope["date_from"]))
        except ValueError: pass
    if scope.get("date_to"):
        try: q = q.filter(SBItem.saved_at <= datetime.fromisoformat(scope["date_to"]))
        except ValueError: pass
    if scope.get("q"):
        from sqlalchemy import text as _t
        s = scope["q"]
        like = f"%{s}%"
        q = q.filter(or_(
            _t("scrapbook_items.search_tsv @@ plainto_tsquery('english', :sq)").bindparams(sq=s),
            SBItem.title.ilike(like),
            SBItem.note.ilike(like),
            SBItem.content_md.ilike(like),
        ))
    q = q.order_by(SBItem.saved_at.desc())
    return q.limit(min(max_items, 200)).all()


def _format_rag_context(items, max_chars_per_item: int = 1800) -> str:
    """Format items as a system context block the model can cite."""
    parts = []
    for it in items:
        body = it.content_md or it.content_text or it.note or ""
        body = body.strip()[:max_chars_per_item]
        tags = ", ".join(it.tags or [])
        saved_at = it.saved_at.isoformat() if it.saved_at else ""
        parts.append(
            f"--- ITEM id={it.id} type={it.type} saved_at={saved_at} saved_by={it.saved_by or 'unknown'} ---\n"
            f"TITLE: {it.title or '(untitled)'}\n"
            f"URL: {it.url or '(none)'}\n"
            f"TAGS: {tags}\n"
            f"NOTE: {it.note or ''}\n"
            f"CONTENT:\n{body}"
        )
    return "\n\n".join(parts)


SYSTEM_PROMPT = (
    "You are the Scrapbook AI for an Ahrefs marketing team. You answer questions "
    "grounded ONLY in the SCRAPBOOK CONTEXT below — a curated set of items the user saved "
    "(URLs, LinkedIn posts, notes, screenshots, etc.).\n\n"
    "Rules:\n"
    "1. Cite every claim you make with [item:UUID] referencing item IDs from the context. "
    "You can cite multiple: [item:abc][item:def].\n"
    "2. If the context doesn't cover the question, say so clearly and suggest what to save next.\n"
    "3. Be concise. Prefer bullet lists and clear structure over walls of prose.\n"
    "4. When ideating, ground each idea in 1-3 specific items.\n"
    "5. Never invent facts that aren't in the context.\n"
)


def _extract_cited_ids(text: str, valid_ids: set) -> list:
    import re
    found = re.findall(r"\[item:([0-9a-f-]{8,40})\]", text)
    seen = []
    for fid in found:
        if fid in valid_ids and fid not in seen:
            seen.append(fid)
    return seen


# ---------- Sessions ----------

@blueprint.route("/api/ai/models")
def ai_models():
    return jsonify({"models": MODEL_OPTIONS, "default": DEFAULT_MODEL, "presets": PRESET_PROMPTS})


@blueprint.route("/api/ai/sessions", methods=["GET"])
def ai_list_sessions():
    email, _ = _current_user()
    rows = sb_session.query(SBChatSession).filter(SBChatSession.user_email == (email or ""))\
            .order_by(SBChatSession.updated_at.desc()).all()
    return jsonify([{
        "id": s.id, "title": s.title, "model": s.model,
        "updated_at": s.updated_at.isoformat() if s.updated_at else None,
    } for s in rows])


@blueprint.route("/api/ai/sessions", methods=["POST"])
def ai_create_session():
    email, _ = _current_user()
    data = request.json or {}
    s = SBChatSession(
        user_email=email or "",
        title=data.get("title", "New chat"),
        model=data.get("model", DEFAULT_MODEL),
    )
    sb_session.add(s)
    sb_session.commit()
    return jsonify({"id": s.id, "title": s.title, "model": s.model})


@blueprint.route("/api/ai/sessions/<sid>", methods=["PATCH"])
def ai_update_session(sid):
    email, _ = _current_user()
    s = sb_session.get(SBChatSession, sid)
    if not s or s.user_email != (email or ""):
        abort(404)
    data = request.json or {}
    if "title" in data: s.title = data["title"][:300]
    if "model" in data: s.model = data["model"]
    s.updated_at = datetime.now(timezone.utc)
    sb_session.commit()
    return jsonify({"ok": True})


@blueprint.route("/api/ai/sessions/<sid>", methods=["DELETE"])
def ai_delete_session(sid):
    email, _ = _current_user()
    s = sb_session.get(SBChatSession, sid)
    if not s or s.user_email != (email or ""):
        abort(404)
    sb_session.delete(s)
    sb_session.commit()
    return jsonify({"ok": True})


@blueprint.route("/api/ai/sessions/<sid>/messages")
def ai_session_messages(sid):
    email, _ = _current_user()
    s = sb_session.get(SBChatSession, sid)
    if not s or s.user_email != (email or ""):
        abort(404)
    msgs = sb_session.query(SBChatMessage).filter(SBChatMessage.session_id == sid)\
            .order_by(SBChatMessage.created_at.asc()).all()
    return jsonify({
        "session": {"id": s.id, "title": s.title, "model": s.model},
        "messages": [{
            "id": m.id, "role": m.role, "content": m.content,
            "cited_item_ids": m.cited_item_ids or [],
            "created_at": m.created_at.isoformat() if m.created_at else None,
        } for m in msgs],
    })


# ---------- Ask (background thread + polling) ----------

@blueprint.route("/api/ai/ask", methods=["POST"])
def ai_ask():
    data = request.json or {}
    sid = data.get("session_id")
    user_msg = (data.get("message") or "").strip()
    if not sid or not user_msg:
        return jsonify({"error": "session_id and message required"}), 400
    email, _ = _current_user()
    s = sb_session.get(SBChatSession, sid)
    if not s or s.user_email != (email or ""):
        return jsonify({"error": "session not found"}), 404

    scope = data.get("scope") or {}
    model = data.get("model") or s.model or DEFAULT_MODEL

    # Persist user message immediately
    um = SBChatMessage(session_id=sid, role="user", content=user_msg, cited_item_ids=[])
    sb_session.add(um)

    # Auto-title from first user message
    if s.title in ("", "New chat"):
        s.title = user_msg[:80]
    s.model = model
    s.updated_at = datetime.now(timezone.utc)

    # Pull recent dialog (prior to this message) for context — last 8 turns
    prior_msgs = sb_session.query(SBChatMessage).filter(SBChatMessage.session_id == sid)\
        .order_by(SBChatMessage.created_at.desc()).limit(16).all()
    prior_msgs = list(reversed(prior_msgs))

    sb_session.commit()

    # Pull RAG items now (in request thread — DB cheap)
    items = _select_rag_items(scope)
    context_text = _format_rag_context(items)
    valid_ids = {it.id for it in items}
    item_summary_for_ui = [{
        "id": it.id, "title": it.title or "(untitled)",
        "type": it.type, "url": it.url,
    } for it in items]

    # Build messages
    sys_msg = SYSTEM_PROMPT + (
        f"\n\nSCRAPBOOK CONTEXT — {len(items)} items (most-recent first):\n\n" +
        (context_text if context_text else "(no items match the current scope — tell the user)")
    )
    messages = [{"role": "system", "content": sys_msg}]
    for m in prior_msgs:
        messages.append({"role": m.role, "content": m.content})

    job_id = str(uuid.uuid4())
    with _chat_jobs_lock:
        _chat_jobs[job_id] = {"status": "running", "session_id": sid,
                              "context_items": item_summary_for_ui}

    def _run():
        try:
            resp = _llm_client.chat.completions.create(
                model=model,
                messages=messages,
                timeout=180,
            )
            answer = resp.choices[0].message.content or ""
            cited = _extract_cited_ids(answer, valid_ids)
            with session_scope() as ss:
                am = SBChatMessage(session_id=sid, role="assistant",
                                    content=answer, cited_item_ids=cited)
                ss.add(am)
                # touch session
                ses = ss.get(SBChatSession, sid)
                if ses:
                    ses.updated_at = datetime.now(timezone.utc)
                ss.flush()
                msg_id = am.id
            with _chat_jobs_lock:
                _chat_jobs[job_id] = {
                    "status": "done",
                    "session_id": sid,
                    "message_id": msg_id,
                    "answer": answer,
                    "cited_item_ids": cited,
                    "context_items": item_summary_for_ui,
                    "context_count": len(items),
                }
        except Exception as e:
            with _chat_jobs_lock:
                _chat_jobs[job_id] = {"status": "error", "error": str(e), "session_id": sid}

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"job_id": job_id, "context_count": len(items)})


@blueprint.route("/api/ai/ask/<job_id>")
def ai_ask_status(job_id):
    with _chat_jobs_lock:
        job = _chat_jobs.get(job_id)
    if not job:
        return jsonify({"error": "not found"}), 404
    # Pop terminal jobs after the client sees them
    if job["status"] in ("done", "error"):
        with _chat_jobs_lock:
            _chat_jobs.pop(job_id, None)
    return jsonify(job)



# ===========================================================================
# Spec parity (lou-linehan/scrapbook-spec) — the other four sections
#
# Scraps gaps (markdown import, media gallery + transcription, scrap trends),
# Topic research (Trending, Topics), Monitoring (Reddit Radar, Growth Scanner),
# Write (Ideas, Example finder, Ahrefs weaver) and Publish (Blog Refresh Engine)
# live in helper modules and are registered onto this same blueprint.
#
# Content gap (spec workflow 06) is deliberately NOT built here — it duplicates
# the Keyword Research Hub's Content Gap tab.
# ===========================================================================

from applications import _scrapbook_models as _sb_models  # noqa: E402  (defines the new tables)
from applications._scrapbook_routes import register as _sb_register  # noqa: E402

# create_all for the newly declared models on the shared cross engine
SBBase.metadata.create_all(_engine)

# create_all() only creates missing TABLES, never missing columns on an existing one, and
# the agent user can't ALTER these (console owns them) — so additive columns are applied
# here, idempotently, at import time.
def _ensure_columns():
    from sqlalchemy import text as _sql
    stmts = [
        "ALTER TABLE scrapbook_items ADD COLUMN IF NOT EXISTS notes JSONB NOT NULL DEFAULT '[]'::jsonb",
    ]
    try:
        with _engine.begin() as c:
            for s in stmts:
                c.execute(_sql(s))
    except Exception as exc:  # noqa: BLE001
        print(f"[scrapbook] column ensure skipped: {exc}")


_ensure_columns()

# Backfill: existing single notes become the first entry of the new list.
def _backfill_notes():
    from sqlalchemy import text as _sql
    try:
        with _engine.begin() as c:
            c.execute(_sql("""
                UPDATE scrapbook_items
                   SET notes = jsonb_build_array(jsonb_build_object(
                         'id', md5(id || '-n0'), 'text', note,
                         'created_at', to_char(saved_at, 'YYYY-MM-DD"T"HH24:MI:SSOF')))
                 WHERE note <> '' AND (notes IS NULL OR notes = '[]'::jsonb)
            """))
    except Exception as exc:  # noqa: BLE001
        print(f"[scrapbook] notes backfill skipped: {exc}")


_backfill_notes()

_sb_register(blueprint, {
    "SBItem": SBItem,
    "SBFirehoseTap": SBFirehoseTap,
    "item_to_dict": _item_to_dict,
    "finalize_item_create": _finalize_item_create,
    "save_bytes": _save_bytes,
    "DATA_DIR": DATA_DIR,
})
