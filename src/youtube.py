from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, Dict, Any
import requests
import streamlit as st


@dataclass
class YouTubeChannelInfo:
    channel_title: Optional[str]
    date_created: Optional[date]  # from snippet.publishedAt


def _publishedat_to_date(published_at: str | None) -> Optional[date]:
    if not published_at:
        return None
    # e.g. "2012-03-01T00:00:00Z"
    dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
    return dt.date()


def fetch_channel_info(channel_id: str) -> YouTubeChannelInfo:
    api_key = st.secrets["youtube"]["api_key"]
    url = "https://www.googleapis.com/youtube/v3/channels"
    params = {"part": "snippet", "id": channel_id, "key": api_key}

    r = requests.get(url, params=params, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"YouTube API error {r.status_code}: {r.text[:400]}")

    data: Dict[str, Any] = r.json()
    items = data.get("items", [])
    if not items:
        return YouTubeChannelInfo(channel_title=None, date_created=None)

    snippet = items[0].get("snippet", {}) or {}
    title = snippet.get("title")
    published_at = snippet.get("publishedAt")

    return YouTubeChannelInfo(
        channel_title=title,
        date_created=_publishedat_to_date(published_at),
    )
