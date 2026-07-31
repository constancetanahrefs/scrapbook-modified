"""Obsidian-Web-Clipper markdown import for LinkedIn / X post clips.

Spec: workflows/01-posts.md. The parsing rules here are the spec's, verbatim in intent:

1. detect platform from clip markers
2. detect author (never from the title/filename — those are truncated post text)
3. strip clip chrome and cut the reply thread
4. parse comments from the RAW body, before stripping
5. flatten inline markdown to plain text
6. derive published_at from the LinkedIn `activity` / X `status` snowflake id
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Optional

# --- markers -------------------------------------------------------------

LI_MARKERS = ("## feed post", "linkedin.com")
X_MARKERS = ("## conversation", "post your reply", "x.com", "twitter.com")

# Where the reply thread starts — cut at the EARLIEST of these.
THREAD_CUTS = ("most relevant", "most recent", "[send]", "post your reply", "discover more")

CHROME_LINE_RE = re.compile(
    r"^\s*(?:"
    r"[\d,\.]+\s*[KkMm]?\s*(?:likes?|comments?|reposts?|reactions?|views?|replies|retweets?|bookmarks?)"
    r"|(?:like|comment|repost|send|share|follow|following|subscribe|show more|more)\s*$"
    r"|see (?:previous|more) replies?"
    r"|see translation"
    r"|post your reply"
    r"|discover more"
    r"|\d+\s*(?:h|d|w|mo|y)\s*(?:•.*)?$"                                             # "3d • Edited"
    r"|·+|—+|\*\*\*+"
    r")\s*$", re.I)

# A standalone URL line, or an X byline ("Name @handle · Mar 4") — clip chrome, not prose.
BARE_URL_RE = re.compile(r"^\s*<?https?://\S+>?\s*$")
X_BYLINE_RE = re.compile(r"^\s*\S.{0,70}@[A-Za-z0-9_]{2,15}\s*(?:·|\|)\s*\S+")

PROFILE_LINE_RE = re.compile(r"^\s*\[?!?\[?[^\]]*\]?\((https?://(?:www\.)?(?:linkedin\.com/in/|x\.com/|twitter\.com/)[^)]+)\)\s*$", re.I)
VIEW_PROFILE_RE = re.compile(r"View\s+(.+?)(?:['’]s)?\s+(?:profile|graphic link)", re.I)
MD_LINK_RE = re.compile(r"\[([^\]]*)\]\(([^)]+)\)")
MD_IMG_RE = re.compile(r"!\[([^\]]*)\]\(([^)]+)\)")
LI_ACTIVITY_RE = re.compile(r"(?:urn:li:activity:|activity[-:])(\d{15,25})")
X_STATUS_RE = re.compile(r"(?:x|twitter)\.com/[^/]+/status/(\d{15,25})")
IN_SLUG_RE = re.compile(r"linkedin\.com/in/([A-Za-z0-9\-_%]+)")

X_EPOCH_MS = 1288834974657


# --- helpers -------------------------------------------------------------

def detect_platform(raw: str, filename: str = "") -> str:
    low = (raw + " " + filename).lower()
    li_hits = sum(1 for m in LI_MARKERS if m in low)
    x_hits = sum(1 for m in X_MARKERS if m in low)
    if li_hits > x_hits:
        return "linkedin"
    if x_hits > li_hits:
        return "x"
    return "linkedin" if li_hits else "x" if x_hits else "linkedin"


def flatten_markdown(text: str) -> str:
    """Post bodies render as PLAIN TEXT — drop images, unwrap links/emphasis, strip headings."""
    out = []
    for line in text.splitlines():
        s = MD_IMG_RE.sub("", line)                     # drop images entirely
        s = MD_LINK_RE.sub(r"\1", s)                    # [text](url) -> text
        s = re.sub(r"^#{1,6}\s*", "", s)                # headings
        s = re.sub(r"^\s*>\s?", "", s)                  # blockquote markers
        s = re.sub(r"^\s*(?:[-*_]\s*){3,}$", "", s)     # horizontal rules
        s = re.sub(r"\*\*([^*]+)\*\*", r"\1", s)
        s = re.sub(r"(?<!\*)\*([^*]+)\*(?!\*)", r"\1", s)
        s = re.sub(r"__([^_]+)__", r"\1", s)
        s = re.sub(r"(?<!_)_([^_]+)_(?!_)", r"\1", s)
        s = re.sub(r"`([^`]*)`", r"\1", s)
        out.append(s.rstrip())
    body = "\n".join(out)
    body = re.sub(r"\n{3,}", "\n\n", body)
    return body.strip()


def snowflake_date(raw: str, platform: str) -> Optional[datetime]:
    """LinkedIn activity id: ms = id >> 22. X status id: ms = (id >> 22) + X_EPOCH_MS."""
    try:
        if platform == "linkedin":
            m = LI_ACTIVITY_RE.search(raw)
            if not m:
                return None
            ms = int(m.group(1)) >> 22
        else:
            m = X_STATUS_RE.search(raw)
            if not m:
                return None
            ms = (int(m.group(1)) >> 22) + X_EPOCH_MS
        dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
        if 2006 <= dt.year <= datetime.now(timezone.utc).year + 1:   # sanity-gate the year
            return dt
    except Exception:
        return None
    return None


def _explicit_date(raw: str) -> Optional[datetime]:
    for key in ("published", "created", "date"):
        m = re.search(rf"^{key}:\s*(\S+)", raw, re.I | re.M)
        if m:
            try:
                val = m.group(1).strip().strip('"')
                dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
                return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            except Exception:
                continue
    return None


def detect_author(raw: str, platform: str) -> tuple[str, str]:
    """Return (author_name, author_headline). Never the clip title/filename."""
    lines = raw.splitlines()
    if platform == "linkedin":
        # The "View X's profile" line is the POSTER.
        for i, line in enumerate(lines):
            m = VIEW_PROFILE_RE.search(line)
            if m:
                name = MD_LINK_RE.sub(r"\1", m.group(1)).strip(" *_[]")
                if name and name.lower() not in ("your", "my"):
                    return name, _headline_after(lines, i)
        # Fall back to a /in/<slug>-derived name.
        m = IN_SLUG_RE.search(raw)
        if m:
            slug = re.sub(r"-[0-9a-f]{6,}$", "", m.group(1))
            pretty = " ".join(p.capitalize() for p in re.split(r"[-_]+", slug) if p)
            return pretty or "Unknown author", ""
        return "Unknown author", ""

    # X: first [Name](x.com/handle) link that is not an @handle and not a /status/ link.
    for i, line in enumerate(lines):
        for text_, href in MD_LINK_RE.findall(line):
            t = text_.strip()
            if not t or t.startswith("@") or "/status/" in href:
                continue
            if re.search(r"(?:x|twitter)\.com/[^/)]+/?$", href):
                # X clips carry no author headline — don't mistake post prose for one.
                return t, ""
    m = re.search(r"(?:x|twitter)\.com/([A-Za-z0-9_]{2,15})/?", raw)
    return (m.group(1) if m else "Unknown author"), ""


def _headline_after(lines: list[str], idx: int) -> str:
    for line in lines[idx + 1: idx + 4]:
        s = flatten_markdown(line).strip()
        if not s or len(s) >= 200:
            continue
        if (CHROME_LINE_RE.match(s) or PROFILE_LINE_RE.match(line)
                or BARE_URL_RE.match(s) or X_BYLINE_RE.match(s)):
            continue
        return s
    return ""


def parse_comments(raw: str) -> list[dict]:
    """Parse replies from the RAW body: the profile link PRECEDING the prose is the author."""
    cut = _thread_cut_index(raw)
    if cut is None:
        return []
    tail = raw[cut:]
    comments: list[dict] = []
    pending_author = ""
    buf: list[str] = []

    def _flush():
        nonlocal pending_author, buf
        text = flatten_markdown("\n".join(buf)).strip()
        # Cut a comment at any chrome line that slipped into its buffer.
        keep = []
        for ln in text.splitlines():
            if CHROME_LINE_RE.match(ln.strip()):
                break
            keep.append(ln)
        text = "\n".join(keep).strip()
        if pending_author and text:
            comments.append({"author": pending_author, "text": text[:4000]})
        pending_author, buf = "", []

    for line in tail.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if CHROME_LINE_RE.match(flatten_markdown(stripped)):
            continue
        m = PROFILE_LINE_RE.match(line)
        if not m:
            # "[**Name**](url)" at the start of a line also introduces a commenter
            lead = re.match(r"^\s*\[\*?\*?([^\]*]+)\*?\*?\]\((https?://[^)]+)\)\s*(.*)$", line)
            if lead and ("linkedin.com/in/" in lead.group(2) or "x.com/" in lead.group(2)):
                _flush()
                pending_author = lead.group(1).strip()
                if lead.group(3).strip():
                    buf.append(lead.group(3))
                continue
        if m:
            _flush()
            href = m.group(1)
            name = MD_LINK_RE.sub(r"\1", stripped).strip(" *_[]()")
            slug = IN_SLUG_RE.search(href) or re.search(r"(?:x|twitter)\.com/([A-Za-z0-9_]+)", href)
            if not name or name.startswith("http"):
                name = " ".join(p.capitalize() for p in re.split(r"[-_]+", slug.group(1))) if slug else "Unknown"
            pending_author = name
            continue
        if pending_author:
            buf.append(line)
    _flush()
    return comments[:100]


def _thread_cut_index(raw: str) -> Optional[int]:
    low = raw.lower()
    idxs = [low.find(c) for c in THREAD_CUTS]
    idxs = [i for i in idxs if i >= 0]
    return min(idxs) if idxs else None


def strip_chrome(raw: str) -> str:
    """Remove the clip header, standalone profile-link lines, reaction counts, and the thread."""
    cut = _thread_cut_index(raw)
    body = raw[:cut] if cut is not None else raw

    lines = body.splitlines()
    # Drop YAML frontmatter and the clip header (## Feed post / ## Conversation)
    if lines and lines[0].strip() == "---":
        for i in range(1, len(lines)):
            if lines[i].strip() == "---":
                lines = lines[i + 1:]
                break
    out = []
    for line in lines:
        s = line.strip()
        if not s:
            out.append("")
            continue
        flat = flatten_markdown(s).strip()
        if re.match(r"^##?\s*(feed post|conversation|post)\s*$", s, re.I):
            continue
        if PROFILE_LINE_RE.match(line) or BARE_URL_RE.match(s) or BARE_URL_RE.match(flat):
            continue
        if X_BYLINE_RE.match(flat):
            continue
        if VIEW_PROFILE_RE.search(s):
            continue
        if CHROME_LINE_RE.match(flat):
            continue
        out.append(line)
    return "\n".join(out)


def extract_media(raw: str) -> list[dict]:
    media = []
    seen = set()
    for _alt, href in MD_IMG_RE.findall(raw):
        href = href.split(" ")[0].strip()
        if not href.startswith("http") or href in seen:
            continue
        seen.add(href)
        is_video = bool(re.search(r"\.(mp4|mov|webm|m3u8)(\?|$)", href, re.I)) or "/video" in href
        media.append({"type": "video" if is_video else "image", "url": href, "cached_url": None})
    return media[:30]


def canonical_url(raw: str, platform: str) -> str:
    if platform == "linkedin":
        m = re.search(r"https?://(?:www\.)?linkedin\.com/(?:feed/update|posts)/[^\s)\]\"']+", raw)
        if m:
            return m.group(0).rstrip(".,)")
        m = LI_ACTIVITY_RE.search(raw)
        if m:
            return f"https://www.linkedin.com/feed/update/urn:li:activity:{m.group(1)}/"
    else:
        m = re.search(r"https?://(?:www\.)?(?:x|twitter)\.com/[^/\s)]+/status/\d+", raw)
        if m:
            return m.group(0).rstrip(".,)")
    m = re.search(r"^source:\s*(\S+)", raw, re.I | re.M)
    return m.group(1).strip() if m else ""


def parse_clip(raw: str, filename: str = "") -> dict:
    """Parse one clip into the saved-post shape. Never raises on a weird clip."""
    raw = raw.replace("\r\n", "\n")
    platform = detect_platform(raw, filename)
    author_name, author_headline = detect_author(raw, platform)
    comments = parse_comments(raw)                       # from the RAW body, before stripping
    body = flatten_markdown(strip_chrome(raw))
    # The author's headline is metadata, not post prose — don't let it head the body.
    if author_headline:
        lines = body.splitlines()
        while lines and lines[0].strip() in (author_headline, author_name):
            lines.pop(0)
        body = "\n".join(lines).strip()
    published = _explicit_date(raw) or snowflake_date(raw, platform)
    title = (body.split("\n")[0][:180] if body else (filename or "Untitled clip"))
    return {
        "platform": platform,
        "author_name": author_name,
        "author_headline": author_headline,
        "post_url": canonical_url(raw, platform),
        "content": body,
        "media": extract_media(raw),
        "comments": comments,
        "comments_count": len(comments),
        "published_at": published,
        "title": title,
        "source_filename": filename,
    }
