import logging, re, time, random
from pathlib import Path
from typing import List, Optional, Tuple, Dict
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


def parse_m3u(
    path: Path,
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
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
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
                elif group_lower in replay_keywords:
                    cat = Category.REPLAY
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


def _tv_has_allowed_network(
    title: str, allowed_countries: List[str], api_key: str, year: Optional[int] = None
) -> bool:
    query = re.sub(r"[Ss]\d{1,2}\s*[Ee]\d{1,2}.*", "", title)
    query = re.sub(r"\s*\(\d{4}\)\s*", "", query)
    query = re.sub(r"\s*-\s*\d{4}$", "", query)
    query = re.sub(r"\((US|UK|CA|AU|NZ|FR|DE|IT)\)", "", query, flags=re.IGNORECASE)
    query = query.replace("&", "and")
    query = re.sub(r"\s+", " ", query).strip()
    search_url = f"https://api.themoviedb.org/3/search/tv?api_key={api_key}&query={query}"
    data = _tmdb_get(search_url, api_key)
    if not data or not data.get("results"):
        logging.debug(f"TMDb: No TV match for '{query}' - excluded by default")
        return False
    results = data["results"]
    if year:
        filtered = [r for r in results if r.get("first_air_date", "").startswith(str(year))]
        if filtered:
            results = filtered
    preferred = [r for r in results if any(c in allowed_countries for c in r.get("origin_country", []))]
    if preferred:
        best = max(preferred, key=lambda r: r.get("popularity", 0))
    else:
        best = max(results, key=lambda r: r.get("popularity", 0))
    tid = best.get("id")
    if not tid:
        logging.debug(f"TMDb: No ID for TV show '{query}' - excluded by default")
        return False
    lang = best.get("original_language", "").lower()
    if lang == "ja":
        logging.debug(f"TMDb: Excluding TV show '{query}' - original language Japanese")
        return False
    if lang == "en" and not allowed_countries:
        logging.debug(f"TMDb: TV show '{query}' allowed by English language (no country filter)")
        return True
    show_url = f"https://api.themoviedb.org/3/tv/{tid}?api_key={api_key}"
    show = _tmdb_get(show_url, api_key)
    if not show:
        logging.debug(f"TMDb: No details for TV show '{query}' - allowing by default")
        return True
    for network in show.get("networks", []):
        origin_countries = network.get("origin_country", [])
        if any(c in allowed_countries for c in origin_countries):
            logging.debug(f"TMDb: TV show '{query}' allowed by network country: {origin_countries}")
            return True
    prod_country_codes = [
        c.get("iso_3166_1") for c in show.get("production_countries", []) if isinstance(c, dict)
    ]
    if any(c in allowed_countries for c in prod_country_codes):
        logging.debug(f"TMDb: TV show '{query}' allowed by production country")
        return True
    origin_countries = show.get("origin_country", [])
    if any(c in allowed_countries for c in origin_countries):
        logging.debug(f"TMDb: TV show '{query}' allowed by origin country")
        return True
    logging.debug(f"TMDb: Excluding TV show '{query}' - no match for allowed countries")
    return False


def split_by_market_filter(
    entries: List[VODEntry],
    allowed_movie_countries: List[str],
    allowed_tv_countries: List[str],
    api_key: str,
    ignore_keywords: Dict[str, List[str]] = None,
    max_workers: int = None,
    max_retries: int = 5,
) -> Tuple[List[VODEntry], List[VODEntry]]:
    if max_workers is None:
        max_workers = 10
    logging.info(f"Filtering using {max_workers} CPU workers")
    allowed, excluded = [], []
    ignore_keywords = ignore_keywords or {}
    stats = {
        "movies_checked": 0, "movies_allowed": 0, "movies_excluded": 0,
        "tv_checked": 0, "tv_allowed": 0, "tv_excluded": 0,
        "docs_checked": 0, "docs_allowed": 0, "docs_excluded": 0,
        "ignored": 0,
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
            year = extract_year(e.raw_title)
            base_title = re.sub(r"[Ss]\d{1,2}[Ee]\d{1,2}.*", "", e.raw_title)
            base_title = re.sub(r"\s*\(\d{4}\)\s*", "", base_title)
            base_title = re.sub(r"\s*-\s*\d{4}$", "", base_title).strip()
            ok = with_retry(_tv_has_allowed_network, base_title, allowed_tv_countries, api_key)
            return (e, ok, "tv")
        elif e.category == Category.DOCUMENTARY:
            year = extract_year(e.raw_title)
            title_clean = sanitize_title(e.raw_title)
            title_clean = re.sub(r"\s*\(\d{4}\)\s*", "", title_clean)
            title_clean = re.sub(r"\s*-\s*\d{4}$", "", title_clean).strip()
            ok = with_retry(_movie_tmdb_lookup, title_clean, year, allowed_movie_countries, api_key)
            return (e, ok, "doc")
        else:
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
                (allowed if ok else excluded).append(e)
                stats["movies_allowed" if ok else "movies_excluded"] += 1
            elif kind == "tv":
                stats["tv_checked"] += 1
                (allowed if ok else excluded).append(e)
                stats["tv_allowed" if ok else "tv_excluded"] += 1
            elif kind == "doc":
                stats["docs_checked"] += 1
                (allowed if ok else excluded).append(e)
                stats["docs_allowed" if ok else "docs_excluded"] += 1
            else:
                excluded.append(e)

    logging.info("Filter statistics:")
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
    logging.info(f"  Total: {len(allowed)} allowed, {len(excluded)} excluded")
    return allowed, excluded
