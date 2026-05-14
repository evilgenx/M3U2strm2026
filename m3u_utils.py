import logging
import re
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any
from dataclasses import dataclass
from enum import Enum
from core import _normalize_unicode, _ascii
import requests
from core import (
    sanitize_title,
    canonical_movie_key,
    canonical_tv_key,
    make_cache_key,
    extract_year,
)
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from core import SQLiteCache


@dataclass
class VODEntry:
    raw_title: str
    safe_title: str
    url: str
    category: "Category"
    group: Optional[str] = None
    year: Optional[int] = None


class Category(Enum):
    MOVIE = "movie"
    TVSHOW = "tvshow"
    DOCUMENTARY = "documentary"
    REPLAY = "replay"


# TMDb-related code has been removed. All entries pass through keyword filtering in parse_m3u.
# No API-based country filtering is performed.


def parse_m3u(
    m3u_source: str,
    tv_keywords: List[str],
    doc_keywords: List[str],
    movie_keywords: List[str],
    replay_keywords: List[str],
    ignore_keywords: Dict[str, List[str]],
) -> List[VODEntry]:
    movie_keywords = {k.strip().lower() for k in movie_keywords}
    tv_keywords = {k.strip().lower() for k in tv_keywords}
    doc_keywords = {k.strip().lower() for k in doc_keywords}
    replay_keywords = {k.strip().lower() for k in replay_keywords}
    entries: List[VODEntry] = []
    cur_title, cur_group = None, None
    seen_groups = set()
    
    # Determine if source is a URL or a local file
    if m3u_source.lower().startswith(("http://", "https://")):
        logging.info(f"Downloading M3U from URL: {m3u_source}")
        try:
            from urllib.parse import urlparse, unquote
            session = requests.Session()
            response = session.get(m3u_source, stream=True, timeout=30)
            response.raise_for_status()
            lines_iter = response.iter_lines(decode_unicode=True)
            # Extract filename from URL for hint detection
            parsed = urlparse(m3u_source)
            path_part = unquote(parsed.path)
            m3u_filename = Path(path_part).name.lower() if path_part else ""
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to download M3U from URL {m3u_source}: {e}")
            return []
    else:
        # Local file
        path = Path(m3u_source)
        if not path.exists():
            logging.error(f"M3U file not found: {m3u_source}")
            return []
        logging.info(f"Reading M3U from local file: {m3u_source}")
        m3u_filename = path.name.lower()
        lines_iter = path.open("r", encoding="utf-8", errors="ignore")
    
    # Process lines with abort-safety for HTTP streaming disconnects
    parsed_count = 0
    try:
        for line in lines_iter:
            line = line.strip()
            if not line:
                continue
            if line.startswith("#EXTINF:"):
                if "," in line:
                    cur_title = line.rsplit(",", 1)[-1].strip()
                else:
                    cur_title = line
                m = re.search(r'group-title="([^"]+)"', line, flags=re.IGNORECASE)
                if m:
                    cur_group = m.group(1).strip().lower()
                    seen_groups.add(cur_group)
                else:
                    cur_group = None
            elif cur_title and line.startswith(("http://", "https://")):
                # Check URL patterns first - most reliable indicator
                url_lower = line.lower()
                if "/movie/" in url_lower or "/movies/" in url_lower:
                    cat = Category.MOVIE
                    logging.debug(f"URL-based classification: MOVIE (URL contains '/movie/' or '/movies/') - {cur_title}")
                elif "/series/" in url_lower:
                    cat = Category.TVSHOW
                    logging.debug(f"URL-based classification: TVSHOW (URL contains '/series/') - {cur_title}")
                else:
                    # Fall back to original logic
                    cat = Category.MOVIE
                    group_lower = (cur_group or "").strip().lower()
                    if group_lower == "doc":
                        cat = Category.DOCUMENTARY
                    elif group_lower == "docs":
                        cat = Category.TVSHOW
                    elif group_lower in movie_keywords:
                        cat = Category.MOVIE
                    elif group_lower in tv_keywords:
                        cat = Category.TVSHOW
                    elif group_lower in doc_keywords:
                        cat = Category.DOCUMENTARY
                    elif group_lower in replay_keywords:
                        cat = Category.REPLAY
                # Filename hint detection (secondary fallback)
                if "movie" in m3u_filename and cat != Category.TVSHOW:
                    cat = Category.MOVIE
                    logging.debug(f"Filename hint classification: MOVIE (filename contains 'movie') - {cur_title}")
                elif "series" in m3u_filename and cat != Category.MOVIE:
                    cat = Category.TVSHOW
                    logging.debug(f"Filename hint classification: TVSHOW (filename contains 'series') - {cur_title}")
                if cat not in (
                    Category.MOVIE,
                    Category.DOCUMENTARY,
                    Category.TVSHOW,
                    Category.REPLAY,
                ):
                    if re.search(r"[Ss]\d{1,2}\s*[Ee]\d{1,2}", cur_title):
                        cat = Category.TVSHOW
                    elif re.search(r"\(\d{4}\)\s*$", cur_title) or re.search(
                        r"[-–]\s*\d{4}\s*$", cur_title
                    ):
                        cat = Category.MOVIE
                title_norm = _ascii(_normalize_unicode(cur_title.lower()))
                skip = False
                if cat == Category.TVSHOW:
                    for kw in ignore_keywords.get("tvshows", []):
                        if kw.lower() in title_norm:
                            logging.debug(f"Skipping ignored TV show: {cur_title}")
                            skip = True
                            break
                elif cat == Category.MOVIE:
                    for kw in ignore_keywords.get("movies", []):
                        if kw.lower() in title_norm:
                            logging.debug(f"Skipping ignored Movie: {cur_title}")
                            skip = True
                            break
                elif cat == Category.DOCUMENTARY:
                    for kw in ignore_keywords.get("documentaries", []):
                        if kw.lower() in title_norm:
                            logging.debug(f"Skipping ignored Documentary: {cur_title}")
                            skip = True
                            break
                if skip:
                    cur_title, cur_group = None, None
                    continue
                year = extract_year(cur_title)
                entries.append(
                    VODEntry(
                        raw_title=cur_title,
                        safe_title=sanitize_title(cur_title),
                        url=line,
                        category=cat,
                        group=cur_group,
                        year=year,
                    )
                )
                cur_title, cur_group = None, None
                parsed_count += 1
    except (requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ConnectionError,
            requests.exceptions.StreamConsumedError) as e:
        logging.critical(
            f"M3U stream connection lost after parsing {parsed_count} entries: {e}. "
            "Aborting to prevent partial-cleanup data loss."
        )
        return entries
    
    # Close file if we opened it (local file)
    if not m3u_source.lower().startswith(("http://", "https://")):
        if hasattr(lines_iter, 'close'):
            lines_iter.close()
    
    cat_counts: Dict[str, int] = {}
    for e in entries:
        cat_counts[e.category.value] = cat_counts.get(e.category.value, 0) + 1
    logging.info(
        f"M3U media scan complete - Movies: {cat_counts.get('movie', 0)}, "
        f"TV Episodes: {cat_counts.get('tvshow', 0)}, "
        f"Documentaries: {cat_counts.get('documentary', 0)}, "
        f"Replays: {cat_counts.get('replay', 0)}"
    )
    return entries






def split_by_market_filter(
    entries: List[VODEntry],
    allowed_movie_countries: List[str] = None,
    allowed_tv_countries: List[str] = None,
    api_key: str = None,
    ignore_keywords: Dict[str, List[str]] = None,
    max_workers: int = None,
    max_retries: int = 5,
    cache: "SQLiteCache" = None,
) -> Tuple[List[VODEntry], List[VODEntry]]:
    """
    Pass-through filter that accepts all entries.
    TMDb-based country filtering has been removed — keyword filtering is handled in parse_m3u.
    All entries are returned as 'allowed' with an empty exclusion list.
    """
    logging.info("TMDb filtering removed — all entries accepted (keyword filtering done in parse_m3u)")
    return entries, []