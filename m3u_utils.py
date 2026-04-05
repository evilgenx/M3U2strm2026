import logging, re, time, random
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any
from dataclasses import dataclass
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, as_completed
from core import _normalize_unicode, _ascii
import requests
from tqdm import tqdm
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


class TMDbRateLimitError(Exception):
    pass


def _tmdb_get(url: str, api_key: str) -> Optional[dict]:
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 429:
            raise TMDbRateLimitError("TMDb rate limit hit (429 Too Many Requests)")
        resp.raise_for_status()
        return resp.json()
    except TMDbRateLimitError:
        raise
    except Exception as e:
        logging.error(f"TMDb request failed for {url}: {e}")
        return None


def _movie_tmdb_lookup(
    title: str, year: Optional[int], allowed_countries: List[str], api_key: str
) -> bool:
    base_url = "https://api.themoviedb.org/3/search/movie"
    params = {"api_key": api_key, "query": title.strip(), "language": "en-US"}
    if year:
        params["year"] = year
    try:
        resp = requests.get(base_url, params=params, timeout=10)
        if resp.status_code == 429:
            raise TMDbRateLimitError("TMDb rate limit hit (429 Too Many Requests)")
        resp.raise_for_status()
        data = resp.json()
    except TMDbRateLimitError:
        raise
    except Exception as e:
        logging.error(f"TMDb request failed for {title} ({year}): {e}")
        return False
    if not data.get("results") and year:
        logging.debug(f"TMDb: No match for '{title}' ({year}), retrying without year")
        params.pop("year", None)
        try:
            resp = requests.get(base_url, params=params, timeout=10)
            if resp.status_code == 429:
                raise TMDbRateLimitError("TMDb rate limit hit (429 Too Many Requests)")
            resp.raise_for_status()
            data = resp.json()
        except TMDbRateLimitError:
            raise
        except Exception as e:
            logging.error(f"TMDb retry (no year) failed for {title}: {e}")
            return False
    if not data.get("results"):
        logging.debug(f"TMDb: No movie match for '{title}' ({year})")
        return False
    best = data["results"][0]
    movie_id = best.get("id")
    if not movie_id:
        logging.debug(f"TMDb: No ID for movie '{title}' ({year})")
        return False
    lang = best.get("original_language", "").lower()
    if lang == "ja":
        logging.debug(f"TMDb: Excluding '{title}' ({year}) - original language Japanese")
        return False
    if lang == "en" and not allowed_countries:
        logging.debug(f"TMDb: Movie '{title}' allowed by English language (no country filter)")
        return True
    release_url = f"https://api.themoviedb.org/3/movie/{movie_id}/release_dates"
    try:
        releases = requests.get(release_url, params={"api_key": api_key}, timeout=10).json()
    except Exception as e:
        logging.error(f"TMDb release info failed for {title} ({year}): {e}")
        return False
    results = releases.get("results", [])
    countries = {r.get("iso_3166_1") for r in results if isinstance(r, dict) and "iso_3166_1" in r}
    if any(c in allowed_countries for c in countries):
        logging.debug(f"TMDb: Movie '{title}' allowed by release country: {countries}")
        return True
    if lang == "en":
        logging.debug(f"TMDb: Movie '{title}' allowed by English language fallback (no allowed country match)")
        return True
    logging.debug(f"TMDb: Excluding movie '{title}' ({year}) - no allowed country match")
    return False


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
            response = requests.get(m3u_source, stream=True, timeout=30)
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
    
    # Process lines
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
    allowed_movie_countries: List[str],
    allowed_tv_countries: List[str],
    api_key: str,
    ignore_keywords: Dict[str, List[str]] = None,
    max_workers: int = None,
    max_retries: int = 5,
    cache: "SQLiteCache" = None,
) -> Tuple[List[VODEntry], List[VODEntry]]:
    """
    Filter entries based on ignore keywords and TMDB for movies/documentaries.
    TV shows bypass TMDB filtering (always allowed).
    
    Parameters:
    - allowed_movie_countries: used for TMDB movie filtering
    - allowed_tv_countries: ignored (TV shows bypass TMDB)
    - api_key: TMDB API key
    - cache: ignored (no longer used)
    - max_retries: retry attempts for TMDB rate limits
    """
    if max_workers is None:
        max_workers = 10
    
    logging.info(f"Filtering using {max_workers} CPU workers (TMDB for movies/docs only)")
    
    allowed, excluded = [], []
    ignore_keywords = ignore_keywords or {}
    
    stats = {
        "movies_checked": 0, "movies_allowed": 0, "movies_excluded": 0,
        "tv_checked": 0, "tv_allowed": 0, "tv_excluded": 0,
        "docs_checked": 0, "docs_allowed": 0, "docs_excluded": 0,
        "ignored": 0,
        "allowed_total": 0,
    }

    def with_retry(fn, *args, **kwargs):
        delay = 1
        for attempt in range(max_retries):
            try:
                return fn(*args, **kwargs)
            except TMDbRateLimitError:
                logging.warning(f"TMDb rate limit hit, retrying in {delay:.1f}s...")
                time.sleep(delay + random.uniform(0, 1.0))
                delay = min(delay * 2, 30)
        logging.error(f"Max retries exceeded for {fn.__name__} with args={args}")
        return False

    def process_entry(e: VODEntry) -> Tuple[VODEntry, bool, str]:
        ignore_list = []
        if e.category == Category.MOVIE:
            ignore_list = ignore_keywords.get("movies", [])
        elif e.category == Category.TVSHOW:
            ignore_list = ignore_keywords.get("tvshows", [])
        elif e.category == Category.DOCUMENTARY:
            ignore_list = ignore_keywords.get("documentaries", [])
        
        # Check if entry should be ignored by keywords
        if any(word.lower() in e.raw_title.lower() for word in ignore_list):
            logging.debug(f"Ignored by keyword: {e.raw_title}")
            return (e, False, "ignored")
        
        if e.category == Category.MOVIE:
            year = extract_year(e.raw_title)
            title_clean = sanitize_title(e.raw_title)
            title_clean = re.sub(r"\s*\(\d{4}\)\s*", "", title_clean)
            title_clean = re.sub(r"\s*-\s*\d{4}$", "", title_clean).strip()
            ok = with_retry(_movie_tmdb_lookup, title_clean, year, allowed_movie_countries, api_key)
            return (e, ok, "movie")
        elif e.category == Category.TVSHOW:
            # TV shows bypass TMDB filtering (always allowed)
            return (e, True, "tv")
        elif e.category == Category.DOCUMENTARY:
            year = extract_year(e.raw_title)
            title_clean = sanitize_title(e.raw_title)
            title_clean = re.sub(r"\s*\(\d{4}\)\s*", "", title_clean)
            title_clean = re.sub(r"\s*-\s*\d{4}$", "", title_clean).strip()
            ok = with_retry(_movie_tmdb_lookup, title_clean, year, allowed_movie_countries, api_key)
            return (e, ok, "doc")
        else:
            # Unknown category - exclude
            return (e, False, "other")

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(process_entry, e) for e in entries]
        for f in tqdm(as_completed(futures), total=len(futures), desc="Filtering", unit="entry"):
            e, ok, kind = f.result()
            if kind == "ignored":
                excluded.append(e)
                stats["ignored"] += 1
            elif kind == "movie":
                stats["movies_checked"] += 1
                if ok:
                    allowed.append(e)
                    stats["movies_allowed"] += 1
                    stats["allowed_total"] += 1
                else:
                    excluded.append(e)
                    stats["movies_excluded"] += 1
            elif kind == "tv":
                stats["tv_checked"] += 1
                if ok:
                    allowed.append(e)
                    stats["tv_allowed"] += 1
                    stats["allowed_total"] += 1
                else:
                    excluded.append(e)
                    stats["tv_excluded"] += 1
            elif kind == "doc":
                stats["docs_checked"] += 1
                if ok:
                    allowed.append(e)
                    stats["docs_allowed"] += 1
                    stats["allowed_total"] += 1
                else:
                    excluded.append(e)
                    stats["docs_excluded"] += 1
            else:
                excluded.append(e)

    logging.info("Filter statistics (TMDB for movies/docs only, TV shows bypassed):")
    logging.info(
        f"  Movies: {stats['movies_checked']} checked, "
        f"{stats['movies_allowed']} allowed, {stats['movies_excluded']} excluded"
    )
    logging.info(
        f"  TV Shows: {stats['tv_checked']} checked, "
        f"{stats['tv_allowed']} allowed, {stats['tv_excluded']} excluded"
    )
    logging.info(
        f"  Documentaries: {stats['docs_checked']} checked, "
        f"{stats['docs_allowed']} allowed, {stats['docs_excluded']} excluded"
    )
    logging.info(f"  Ignored by keywords: {stats['ignored']}")
    logging.info(f"  Total: {stats['allowed_total']} allowed, {len(excluded)} excluded")
    return allowed, excluded
