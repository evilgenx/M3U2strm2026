import logging
import os
import re
import sqlite3
import threading
import unicodedata
from pathlib import Path
from typing import Dict, Optional, Tuple

YEAR_PATTERN = re.compile(r"(\(\d{4}\)).*$")
YEAR_IN_PARENTHESES = re.compile(r"\((\d{4})\)")
YEAR_IN_FOLDER = re.compile(r"\((\d{4})\)")

EPISODE_PATTERNS = [
    re.compile(r"[Ss](\d{1,2})[Ee](\d{1,2})"),
    re.compile(r"(\d{1,2})x(\d{2})", re.IGNORECASE),
]

VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".mpg", ".mpeg", ".m4v", ".webm"}


def strip_after_year(text: str) -> str:
    return YEAR_PATTERN.sub(r"\1", text)


def _ascii(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")


FRACTION_MAP = {
    "½": "1/2",
    "⅓": "1/3",
    "⅔": "2/3",
    "¼": "1/4",
    "¾": "3/4",
}

SYMBOL_MAP = {
    "·": " ",
    "–": "-",
    "—": "-",
    "“": '"', "”": '"', "‘": "'", "’": "'",
    "…": "...",
    "Æ": "AE",
    "æ": "ae",
}


def _normalize_unicode(text: str) -> str:
    for k, v in FRACTION_MAP.items():
        text = text.replace(k, v)
    for k, v in SYMBOL_MAP.items():
        text = text.replace(k, v)
    return unicodedata.normalize("NFKC", text)


def sanitize_title(title: str) -> str:
    original = title
    t = _normalize_unicode(title.strip())
    t = _ascii(t)
    t = re.sub(r"^\s*(\d+[kK]|[0-9]{3,4}[pP]):\s*", "", t)
    t = t.replace("&", "and")
    t = re.sub(r"[{}()]?tt\d+[{}()]?", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\bimdb\b", "", t, flags=re.IGNORECASE)
    t = t.replace("-", " ").replace("_", " ").replace(".", " ")
    t = re.sub(r"[^\w\s():]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    t = re.sub(r"\((\d{4})\)\s*\(\1\)", r"(\1)", t)
    t = re.sub(r"\s*\(\d{4}\)\s*$", "", t)
    t = re.sub(r"\s*-\s*\d{4}\s*$", "", t)
    t = re.sub(r"\s+\d{4}\s*$", "", t)
    logging.debug(f"sanitize_title: '{original}' -> '{t}'")
    return t.strip()


def make_cache_key(title: str, category: str = None) -> str:
    key = re.sub(r"[^a-z0-9]+", "", title.lower())
    if category:
        return f"{category}:{key}"
    return key


def extract_year(text: str) -> Optional[str]:
    m = re.search(r"\((\d{4})\)", text)
    if m:
        return m.group(1)
    m = re.search(r"-\s*(\d{4})$", text)
    if m:
        return m.group(1)
    return None


def canonical_movie_key(title_with_year: str) -> str:
    t = sanitize_title(title_with_year)
    year = extract_year(title_with_year)
    if year:
        t = f"{t} {year}"
    key = make_cache_key(t)
    return key


def canonical_tv_key(show_with_year: str, season: int, episode: int) -> str:
    show = sanitize_title(show_with_year)
    show_no_year = re.sub(r"\s*\(\d{4}\)\s*", "", show)
    comp = f"{show_no_year} s{season:02d}e{episode:02d}"
    key = make_cache_key(comp)
    return key


import json
import time
from typing import Any, Dict, Optional, Tuple

class SQLiteCache:
    def __init__(self, db_path: Path):
        # Use check_same_thread=False to allow connection sharing across threads
        self.conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._lock = threading.RLock()
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.ensure_tables()

    def ensure_tables(self):
        with self._lock:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS existing_media (
                    key TEXT PRIMARY KEY,
                    category TEXT
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS strm_cache (
                    key TEXT PRIMARY KEY,
                    url TEXT,
                    path TEXT,
                    allowed INTEGER
                )
            """)
            
            cols = [row[1] for row in self.conn.execute("PRAGMA table_info(strm_cache)")]
            if "allowed" not in cols:
                self.conn.execute("ALTER TABLE strm_cache ADD COLUMN allowed INTEGER")
            
            # Create indexes for better query performance
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_strm_cache_allowed 
                ON strm_cache(allowed)
            """)
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_strm_cache_url_path 
                ON strm_cache(url, path)
            """)
            
            # Optimize database settings for better performance
            self.conn.execute("PRAGMA synchronous = NORMAL;")
            self.conn.execute("PRAGMA journal_size_limit = 67108864;")  # 64MB
            self.conn.execute("PRAGMA cache_size = -2000;")  # 2MB cache
            self.conn.execute("PRAGMA mmap_size = 268435456;")  # 256MB mmap
            
            self.conn.commit()

    def replace_existing_media(self, entries: Dict[str, str]):
        with self._lock:
            self.conn.execute("DELETE FROM existing_media")
            self.conn.executemany(
                "INSERT INTO existing_media (key, category) VALUES (?, ?)",
                ((k, v) for k, v in entries.items()),
            )
            self.conn.commit()

    def existing_media_dict(self) -> Dict[str, str]:
        with self._lock:
            return {
                row[0]: row[1]
                for row in self.conn.execute("SELECT key, category FROM existing_media")
            }

    def strm_cache_dict(self) -> Dict[str, Dict[str, Optional[str]]]:
        with self._lock:
            d: Dict[str, Dict[str, Optional[str]]] = {}
            for key, url, path, allowed in self.conn.execute(
                "SELECT key, url, path, allowed FROM strm_cache"
            ):
                d[key] = {"url": url, "path": path, "allowed": allowed}
            return d

    def replace_strm_cache(self, cache: Dict[str, Dict[str, Optional[str]]]):
        """Replace entire cache with new data (legacy method)."""
        with self._lock:
            self.conn.execute("DELETE FROM strm_cache")
            rows = [
                (k, v.get("url"), v.get("path"), v.get("allowed"))
                for k, v in cache.items()
            ]
            self.conn.executemany(
                "INSERT OR REPLACE INTO strm_cache (key, url, path, allowed) VALUES (?, ?, ?, ?)", rows
            )
            self.conn.commit()

    def sync_strm_cache(self, new_cache: Dict[str, Dict[str, Optional[str]]]):
        """
        Synchronize cache incrementally by updating only changed entries.
        More efficient than replace_strm_cache for large datasets.
        """
        with self._lock:
            # Get current cache from database
            current = {}
            for key, url, path, allowed in self.conn.execute(
                "SELECT key, url, path, allowed FROM strm_cache"
            ):
                current[key] = {"url": url, "path": path, "allowed": allowed}
            
            # Find entries to update or insert
            to_update = []
            for key, new_data in new_cache.items():
                old_data = current.get(key)
                if old_data is None:
                    # New entry
                    to_update.append((key, new_data.get("url"), new_data.get("path"), new_data.get("allowed")))
                else:
                    # Check if any field changed
                    if (old_data.get("url") != new_data.get("url") or
                        old_data.get("path") != new_data.get("path") or
                        old_data.get("allowed") != new_data.get("allowed")):
                        to_update.append((key, new_data.get("url"), new_data.get("path"), new_data.get("allowed")))
            
            # Find entries to delete (present in current but not in new_cache)
            to_delete = [key for key in current if key not in new_cache]
            
            # Execute updates in a single transaction
            if to_update:
                self.conn.executemany(
                    "INSERT OR REPLACE INTO strm_cache (key, url, path, allowed) VALUES (?, ?, ?, ?)",
                    to_update
                )
            
            if to_delete:
                placeholders = ','.join(['?'] * len(to_delete))
                self.conn.execute(f"DELETE FROM strm_cache WHERE key IN ({placeholders})", to_delete)
            
            self.conn.commit()
            
            # Return statistics for logging
            return {
                "updated": len(to_update),
                "deleted": len(to_delete),
                "total": len(new_cache)
            }

    def update_strm(
        self, key: str, url: str, path: Optional[str], allowed: Optional[int]
    ):
        with self._lock:
            self.conn.execute(
                "INSERT OR REPLACE INTO strm_cache (key, url, path, allowed) VALUES (?, ?, ?, ?)",
                (key, url, path, allowed),
            )
            self.conn.commit()


    def close(self):
        with self._lock:
            self.conn.close()


def _extract_season_episode(name: str) -> Optional[Tuple[int, int]]:
    m = re.search(r"[Ss](\d{1,2})[Ee](\d{1,2})", name)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = re.search(r"(\d{1,2})x(\d{2})", name, re.IGNORECASE)
    if m:
        logging.debug(f"Matched 1x01 in: {name}")
        return int(m.group(1)), int(m.group(2))
    m = re.search(r"[Ss](\d{1,2})[Ee](\d{1,2})\s*[-–]\s*[Ee](\d{1,2})", name)
    if m:
        logging.debug(f"Matched multi-episode in: {name}")
        return int(m.group(1)), int(m.group(2))
    return None


def build_existing_media_cache(root: Path) -> Dict[str, str]:
    existing: Dict[str, str] = {}
    tv_count = 0
    movie_count = 0
    doc_count = 0
    try:
        root = root.resolve()
    except Exception as e:
        logging.error(f"Failed to resolve directory {root}: {e}")
        return existing
    tv_dirs = ["tv shows", "tv_shows", "series", "tv", "television"]
    movie_dirs = ["movies", "films", "film", "movie"]
    doc_dirs = ["documentaries", "documentary", "docs"]
    for dirpath, _, filenames in os.walk(str(root), followlinks=True):
        for fname in filenames:
            p = Path(dirpath) / fname
            if p.suffix.lower() not in VIDEO_EXTS:
                continue
            path_lower = str(p).lower()
            name = p.stem
            is_doc = any(d in path_lower for d in doc_dirs)
            if is_doc:
                key = make_cache_key(sanitize_title(name))
                existing[key] = "DOCUMENTARY"
                doc_count += 1
                continue
            season_ep = _extract_season_episode(name)
            if season_ep:
                season, episode = season_ep
                show_folder = None
                for parent in p.parents:
                    if YEAR_IN_FOLDER.search(parent.name):
                        show_folder = parent.name
                        break
                    parent_lower = parent.name.lower()
                    if any(tv_dir in parent_lower for tv_dir in tv_dirs):
                        show_folder = parent.name
                        break
                if not show_folder:
                    show_folder = p.parent.name
                if re.match(r"^season\s+\d+$", show_folder.lower()):
                    show_folder = p.parent.parent.name
                show = show_folder
                key = canonical_tv_key(show, season, episode)
                existing[key] = "TVEPISODE"
                tv_count += 1
                continue
            is_movie = any(d in path_lower for d in movie_dirs)
            parent_name = p.parent.name
            parent_has_year = YEAR_IN_FOLDER.search(parent_name) is not None
            file_has_year = YEAR_IN_PARENTHESES.search(name) is not None
            if is_movie or parent_has_year or file_has_year:
                if parent_has_year:
                    title_with_year = parent_name
                elif file_has_year:
                    title_with_year = strip_after_year(name)
                else:
                    title_with_year = name
                key = canonical_movie_key(title_with_year)
                existing[key] = "MOVIE"
                movie_count += 1
                continue
            key = canonical_movie_key(name)
            existing[key] = "MOVIE"
            movie_count += 1
    logging.info(
        f"Local media scan complete - Movies: {movie_count}, TV Episodes: {tv_count}, Documentaries: {doc_count}"
    )
    return existing
