"""Scrapbook models for the Topic-research / Monitoring / Write / Publish sections.

Lives in `console_site_db` alongside the existing scrapbook_* tables so one engine
serves the whole app. Shapes follow data-model.md; names are prefixed `scrapbook_`
to stay in one namespace.

Everything ships EMPTY — these are shapes, not seed data (spec: "Empty-state contract").
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    Boolean, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint, Index,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.db_cross import CrossBase as SBBase


def _now():
    return datetime.now(timezone.utc)


def _uid():
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Shared
# ---------------------------------------------------------------------------

class SBConfig(SBBase):
    """The shared settings object — flat key → arbitrary JSON."""
    __tablename__ = "scrapbook_config"
    key: Mapped[str] = mapped_column(String(80), primary_key=True)
    value: Mapped[dict] = mapped_column(JSONB, default=dict)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, onupdate=_now)


# ---------------------------------------------------------------------------
# Scraps — media gallery
# ---------------------------------------------------------------------------

class SBMedia(SBBase):
    """Images/video captured with posts or uploaded directly (+ video transcripts)."""
    __tablename__ = "scrapbook_media"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uid)
    type: Mapped[str] = mapped_column(String(16), index=True)          # image | video
    url: Mapped[str] = mapped_column(String(2000), default="")
    cached_path: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    title: Mapped[str] = mapped_column(String(500), default="")
    transcript: Mapped[str] = mapped_column(Text, default="")
    transcript_status: Mapped[str] = mapped_column(String(16), default="none", index=True)  # none|running|done|failed
    transcript_error: Mapped[str] = mapped_column(Text, default="")
    source_item_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, index=True)

    __table_args__ = (UniqueConstraint("url", "source_item_id", name="uq_sb_media_url_item"),)


# ---------------------------------------------------------------------------
# Topic research — research sessions (shared shape) + trending bank
# ---------------------------------------------------------------------------

class SBResearchSession(SBBase):
    __tablename__ = "scrapbook_research_sessions"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tab: Mapped[str] = mapped_column(String(32), index=True)   # trending | topics | growth
    filters: Mapped[dict] = mapped_column(JSONB, default=dict)
    status: Mapped[str] = mapped_column(String(16), default="running", index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    summary: Mapped[dict] = mapped_column(JSONB, default=dict)


class SBResearchResult(SBBase):
    __tablename__ = "scrapbook_research_results"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    session_id: Mapped[int] = mapped_column(
        ForeignKey("scrapbook_research_sessions.id", ondelete="CASCADE"), index=True)
    keyword: Mapped[str] = mapped_column(Text, index=True)     # stored lowercased
    volume: Mapped[int] = mapped_column(Integer, default=0)
    difficulty: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    traffic_potential: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    cpc_cents: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    parent_topic: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    position: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    ranking_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    competitors: Mapped[list] = mapped_column(JSONB, default=list)
    growth_3m: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    growth_6m: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    growth_12m: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    source: Mapped[str] = mapped_column(Text, default="")
    is_new: Mapped[bool] = mapped_column(Boolean, default=True)

    __table_args__ = (UniqueConstraint("session_id", "keyword", name="uq_sb_research_sess_kw"),)


class SBTrendingSeed(SBBase):
    __tablename__ = "scrapbook_trending_seeds"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    seed: Mapped[str] = mapped_column(Text, unique=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    added_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)


class SBTrendingKeyword(SBBase):
    """The DURABLE keyword bank. Scans merge into it; nothing is ever wiped."""
    __tablename__ = "scrapbook_trending_keywords"
    keyword: Mapped[str] = mapped_column(Text, primary_key=True)
    country: Mapped[str] = mapped_column(String(8), primary_key=True)
    volume: Mapped[int] = mapped_column(Integer, default=0)
    difficulty: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    traffic_potential: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    cpc_cents: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    parent_topic: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_seed: Mapped[str] = mapped_column(Text, default="")
    growth_3m: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    growth_6m: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    growth_12m: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    blog_position: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    blog_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, index=True)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, index=True)


class SBTrendingScan(SBBase):
    __tablename__ = "scrapbook_trending_scans"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    scan_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, index=True)
    country: Mapped[str] = mapped_column(String(8), default="us")
    growth_period: Mapped[str] = mapped_column(String(16), default="months_3")
    seeds_used: Mapped[list] = mapped_column(JSONB, default=list)
    keywords_found: Mapped[int] = mapped_column(Integer, default=0)
    new_keywords: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(16), default="completed")
    note: Mapped[str] = mapped_column(Text, default="")
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


# ---------------------------------------------------------------------------
# Topic research — Topics (semantic clustering)
# ---------------------------------------------------------------------------

class SBTopicPage(SBBase):
    __tablename__ = "scrapbook_topic_pages"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    domain: Mapped[str] = mapped_column(String(300), index=True)
    url: Mapped[str] = mapped_column(Text, index=True)
    title: Mapped[str] = mapped_column(Text, default="")
    chars: Mapped[int] = mapped_column(Integer, default=0)
    page_vector: Mapped[list] = mapped_column(JSONB, default=list)
    distance: Mapped[Optional[float]] = mapped_column(Float, nullable=True)   # cosine distance to site centre
    bucket: Mapped[str] = mapped_column(String(16), default="")               # core|near|mid|far
    cluster_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)
    traffic: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    refdomains: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    ur: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    __table_args__ = (UniqueConstraint("domain", "url", name="uq_sb_topic_page"),)


class SBTopicCluster(SBBase):
    __tablename__ = "scrapbook_topic_clusters"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    domain: Mapped[str] = mapped_column(String(300), index=True)
    label: Mapped[str] = mapped_column(Text, default="")
    size: Mapped[int] = mapped_column(Integer, default=0)
    centroid: Mapped[list] = mapped_column(JSONB, default=list)
    sample_urls: Mapped[list] = mapped_column(JSONB, default=list)
    avg_distance: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)


class SBTopicScanState(SBBase):
    __tablename__ = "scrapbook_topic_scan_state"
    domain: Mapped[str] = mapped_column(String(300), primary_key=True)
    status: Mapped[str] = mapped_column(String(16), default="idle")
    step: Mapped[str] = mapped_column(Text, default="")
    concentration: Mapped[dict] = mapped_column(JSONB, default=dict)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, onupdate=_now)


# ---------------------------------------------------------------------------
# Monitoring — Reddit Radar
# ---------------------------------------------------------------------------

class SBRedditPost(SBBase):
    __tablename__ = "scrapbook_reddit_posts"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)   # reddit fullname/id
    subreddit: Mapped[str] = mapped_column(String(120), index=True)
    title: Mapped[str] = mapped_column(Text, default="")
    url: Mapped[str] = mapped_column(Text, default="")
    permalink: Mapped[str] = mapped_column(Text, default="")
    body: Mapped[str] = mapped_column(Text, default="")
    author: Mapped[str] = mapped_column(String(200), default="")
    score: Mapped[int] = mapped_column(Integer, default=0)
    num_comments: Mapped[int] = mapped_column(Integer, default=0)
    created_utc: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    matched_query: Mapped[str] = mapped_column(Text, default="")
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, index=True)


class SBRadarReport(SBBase):
    __tablename__ = "scrapbook_radar_reports"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    week_start: Mapped[str] = mapped_column(String(20), index=True)  # ISO date of period start
    summary_md: Mapped[str] = mapped_column(Text, default="")
    stats: Mapped[dict] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, index=True)


# ---------------------------------------------------------------------------
# Monitoring — Growth Scanner
# ---------------------------------------------------------------------------

class SBCategory(SBBase):
    __tablename__ = "scrapbook_categories"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uid)
    name: Mapped[str] = mapped_column(String(300), default="")
    primary_seed: Mapped[str] = mapped_column(Text, default="")
    related_seeds: Mapped[list] = mapped_column(JSONB, default=list)
    anchors: Mapped[list] = mapped_column(JSONB, default=list)               # cluster labels = the fence
    relevant_topic_labels: Mapped[list] = mapped_column(JSONB, default=list)  # LLM-vetted
    excluded_topics: Mapped[list] = mapped_column(JSONB, default=list)        # user-removed, persist
    strip_brands: Mapped[bool] = mapped_column(Boolean, default=True)
    snapshots: Mapped[list] = mapped_column(JSONB, default=list)             # keep last ~12
    discovery_mode: Mapped[str] = mapped_column(String(40), default="parent_topic_v2")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, onupdate=_now)


# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------

class SBIdea(SBBase):
    __tablename__ = "scrapbook_ideas"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    headline: Mapped[str] = mapped_column(Text, default="")
    angle: Mapped[str] = mapped_column(Text, default="")
    keyword_metrics: Mapped[list] = mapped_column(JSONB, default=list)
    source_item_ids: Mapped[list] = mapped_column(JSONB, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, index=True)


class SBExampleDoc(SBBase):
    __tablename__ = "scrapbook_example_docs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    title: Mapped[str] = mapped_column(Text, default="")
    url: Mapped[str] = mapped_column(Text, default="")
    content: Mapped[str] = mapped_column(Text, default="")
    tags: Mapped[list] = mapped_column(ARRAY(String), default=list)
    embedding: Mapped[Optional[list]] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, index=True)


class SBWeaverRun(SBBase):
    __tablename__ = "scrapbook_weaver_runs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    input_hash: Mapped[str] = mapped_column(String(64), index=True)
    data_points: Mapped[list] = mapped_column(JSONB, default=list)
    output_md: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, index=True)


# ---------------------------------------------------------------------------
# Publish — Blog Refresh Engine (moved out)
# ---------------------------------------------------------------------------
# The Blog Refresh Engine is now a standalone app. Its model + table live in
# applications/_bre_core.py (table `bre_articles`). The old `scrapbook_articles`
# table is left untouched in the DB; nothing in Scrapbook reads it any more.


Index("ix_sb_trending_growth3", SBTrendingKeyword.growth_3m)
Index("ix_sb_trending_volume", SBTrendingKeyword.volume)
