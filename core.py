import logging
import os
import re
import sqlite3
import threading
import time
import unicodedata
from collections import OrderedDict
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
    # Keep Unicode characters intact — strip only bytes that are illegal in filenames
    t = re.sub(r'[\\/:*?"<>|\x00-\x1f]+', " ", t)
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


def make_cache_key(title: str, category: Optional[str] = None) -> str:
    key = re.sub(r"[^\w]+", "", title.lower(), flags=re.UNICODE)
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
from typing import Any, Dict, Optional, Tuple


class _TimedCache:
    """Simple TTL-aware in-memory cache backed by OrderedDict for LRU eviction."""

    def __init__(self, maxsize: int = 512, ttl_seconds: float = 60.0):
        self._maxsize = maxsize
        self._ttl = ttl_seconds
        self._data: OrderedDict[str, Tuple[float, Any]] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            if key not in self._data:
                return None
            ts, value = self._data[key]
            if time.monotonic() - ts > self._ttl:
                del self._data[key]
                return None
            # Move to end (most-recently-used)
            self._data.move_to_end(key)
            return value

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            if key in self._data:
                self._data.move_to_end(key)
            self._data[key] = (time.monotonic(), value)
            while len(self._data) > self._maxsize:
                self._data.popitem(last=False)  # evict LRU

    def invalidate(self) -> None:
        with self._lock:
            self._data.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)


class SQLiteCache:
    # Pool size for concurrent reads (WAL mode supports multiple readers)
    _POOL_SIZE = 3

    def __init__(self, db_path: Path, mem_cache_ttl: float = 120.0):
        # Ensure the parent directory exists before creating the database
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path

        # ------------------------------------------------------------------
        # Connection pool for concurrent reads
        # ------------------------------------------------------------------
        self._pool: list[sqlite3.Connection] = []
        self._pool_lock = threading.Lock()
        self._pool_semaphore = threading.BoundedSemaphore(self._POOL_SIZE)

        for _ in range(self._POOL_SIZE):
            conn = sqlite3.connect(str(db_path), check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA query_only=ON;")  # read-only for pool connections
            self._pool.append(conn)

        # Single dedicated write connection (serialized via self._lock)
        self._write_conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._write_conn.execute("PRAGMA journal_mode=WAL;")

        self._lock = threading.RLock()  # write serialization

        # ------------------------------------------------------------------
        # In-memory read caches (TTL-based)
        # ------------------------------------------------------------------
        self._strm_cache_mem = _TimedCache(maxsize=256, ttl_seconds=mem_cache_ttl)
        self._existing_media_mem = _TimedCache(maxsize=256, ttl_seconds=mem_cache_ttl)
        self._cache_version = 0  # version tag for consistency across the two caches
        self._mem_cache_ttl = mem_cache_ttl

        self.ensure_tables()

    # ------------------------------------------------------------------
    # Connection pool context manager for reads
    # ------------------------------------------------------------------
    def _acquire_read_conn(self) -> sqlite3.Connection:
        """Acquire a read-only connection from the pool (blocking up to 5s)."""
        acquired = self._pool_semaphore.acquire(timeout=5.0)
        if not acquired:
            raise RuntimeError("Timed out waiting for a read connection from the pool")
        with self._pool_lock:
            return self._pool.pop()

    def _release_read_conn(self, conn: sqlite3.Connection) -> None:
        """Return a read connection to the pool."""
        with self._pool_lock:
            self._pool.append(conn)
        self._pool_semaphore.release()

    def ensure_tables(self):
        with self._lock:
            self._write_conn.execute("""
                CREATE TABLE IF NOT EXISTS existing_media (
                    key TEXT PRIMARY KEY,
                    category TEXT
                )
            """)
            self._write_conn.execute("""
                CREATE TABLE IF NOT EXISTS strm_cache (
                    key TEXT PRIMARY KEY,
                    url TEXT,
                    path TEXT,
                    allowed INTEGER
                )
            """)

            cols = [row[1] for row in self._write_conn.execute("PRAGMA table_info(strm_cache)")]
            if "allowed" not in cols:
                self._write_conn.execute("ALTER TABLE strm_cache ADD COLUMN allowed INTEGER")

            # Create indexes for better query performance
            self._write_conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_strm_cache_allowed
                ON strm_cache(allowed)
            """)
            self._write_conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_strm_cache_url_path
                ON strm_cache(url, path)
            """)

            # Optimize database settings for better performance
            self._write_conn.execute("PRAGMA synchronous = NORMAL;")
            self._write_conn.execute("PRAGMA journal_size_limit = 67108864;")  # 64MB
            self._write_conn.execute("PRAGMA cache_size = -2000;")  # 2MB cache
            self._write_conn.execute("PRAGMA mmap_size = 268435456;")  # 256MB mmap

            self._write_conn.commit()

    # ------------------------------------------------------------------
    # Memory cache management
    # ------------------------------------------------------------------
    def invalidate_memory_cache(self) -> None:
        """Clear all in-memory caches (call after any write operation)."""
        self._strm_cache_mem.invalidate()
        self._existing_media_mem.invalidate()
        with self._lock:
            self._cache_version += 1

    # ------------------------------------------------------------------
    # existing_media table
    # ------------------------------------------------------------------
    def replace_existing_media(self, entries: Dict[str, str]):
        with self._lock:
            self._write_conn.execute("DELETE FROM existing_media")
            self._write_conn.executemany(
                "INSERT INTO existing_media (key, category) VALUES (?, ?)",
                ((k, v) for k, v in entries.items()),
            )
            self._write_conn.commit()
        self.invalidate_memory_cache()

    def existing_media_dict(self) -> Dict[str, str]:
        # Check memory cache first
        cache_version = self._cache_version
        cached = self._existing_media_mem.get(str(cache_version))
        if cached is not None:
            return cached

        conn = self._acquire_read_conn()
        try:
            result = {
                row[0]: row[1]
                for row in conn.execute("SELECT key, category FROM existing_media")
            }
        finally:
            self._release_read_conn(conn)

        self._existing_media_mem.set(str(cache_version), result)
        return result

    # ------------------------------------------------------------------
    # strm_cache table
    # ------------------------------------------------------------------
    def strm_cache_dict(self) -> Dict[str, Dict[str, Optional[str]]]:
        # Check memory cache first
        cache_version = self._cache_version
        cached = self._strm_cache_mem.get(str(cache_version))
        if cached is not None:
            return cached

        conn = self._acquire_read_conn()
        try:
            d: Dict[str, Dict[str, Optional[str]]] = {}
            for key, url, path, allowed in conn.execute(
                "SELECT key, url, path, allowed FROM strm_cache"
            ):
                d[key] = {"url": url, "path": path, "allowed": allowed}
        finally:
            self._release_read_conn(conn)

        self._strm_cache_mem.set(str(cache_version), d)
        return d

    def replace_strm_cache(self, cache: Dict[str, Dict[str, Optional[str]]]):
        """Replace entire cache with new data (legacy method)."""
        with self._lock:
            self._write_conn.execute("DELETE FROM strm_cache")
            rows = [
                (k, v.get("url"), v.get("path"), v.get("allowed"))
                for k, v in cache.items()
            ]
            self._write_conn.executemany(
                "INSERT OR REPLACE INTO strm_cache (key, url, path, allowed) VALUES (?, ?, ?, ?)", rows
            )
            self._write_conn.commit()
        self.invalidate_memory_cache()

    def sync_strm_cache(self, new_cache: Dict[str, Dict[str, Optional[str]]]):
        """
        Synchronize cache incrementally by updating only changed entries.
        More efficient than replace_strm_cache for large datasets.
        """
        with self._lock:
            # Get current cache from database
            current = {}
            for key, url, path, allowed in self._write_conn.execute(
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
                self._write_conn.executemany(
                    "INSERT OR REPLACE INTO strm_cache (key, url, path, allowed) VALUES (?, ?, ?, ?)",
                    to_update
                )

            if to_delete:
                placeholders = ','.join(['?'] * len(to_delete))
                self._write_conn.execute(f"DELETE FROM strm_cache WHERE key IN ({placeholders})", to_delete)

            self._write_conn.commit()
            self.invalidate_memory_cache()

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
            self._write_conn.execute(
                "INSERT OR REPLACE INTO strm_cache (key, url, path, allowed) VALUES (?, ?, ?, ?)",
                (key, url, path, allowed),
            )
            self._write_conn.commit()
        self.invalidate_memory_cache()

    # ------------------------------------------------------------------
    # Maintenance: WAL checkpoint, integrity check, optimize
    # ------------------------------------------------------------------
    def maintenance(self) -> Dict[str, Any]:
        """
        Run periodic database maintenance:
        - WAL checkpoint (TRUNCATE)
        - Integrity check
        - OPTIMIZE
        Returns a dict with diagnostic results.
        """
        result: Dict[str, Any] = {
            "wal_checkpoint": None,
            "integrity": None,
            "optimize": None,
            "db_size_bytes": None,
            "wal_size_bytes": None,
            "row_counts": {},
        }

        with self._lock:
            # WAL checkpoint
            try:
                cur = self._write_conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                result["wal_checkpoint"] = dict(
                    zip(["busy", "log", "checkpointed"], cur.fetchall())
                )
            except Exception as e:
                result["wal_checkpoint"] = {"error": str(e)}

            # Integrity check
            try:
                cur = self._write_conn.execute("PRAGMA integrity_check")
                rows = cur.fetchall()
                result["integrity"] = "ok" if len(rows) == 1 and rows[0][0] == "ok" else rows
            except Exception as e:
                result["integrity"] = {"error": str(e)}

            # OPTIMIZE
            try:
                self._write_conn.execute("PRAGMA optimize")
                result["optimize"] = True
            except Exception as e:
                result["optimize"] = {"error": str(e)}

            # Database file sizes
            if self.db_path.exists():
                result["db_size_bytes"] = self.db_path.stat().st_size
            wal_path = Path(str(self.db_path) + "-wal")
            if wal_path.exists():
                result["wal_size_bytes"] = wal_path.stat().st_size

            # Row counts
            for table in ("existing_media", "strm_cache"):
                cur = self._write_conn.execute(f"SELECT COUNT(*) FROM {table}")
                result["row_counts"][table] = cur.fetchone()[0]

            self._write_conn.commit()

        logging.info(
            "DB maintenance: integrity=%s, db_size=%s, wal_size=%s, rows=%s",
            result["integrity"],
            result["db_size_bytes"],
            result["wal_size_bytes"],
            result["row_counts"],
        )
        return result

    # ------------------------------------------------------------------
    # Backup & restore
    # ------------------------------------------------------------------
    def backup(self, destination: Path, max_backups: int = 3) -> Path:
        """
        Perform an atomic online backup using sqlite3's built-in backup API.
        Rotates backups: destination -> .bak.1 -> .bak.2 -> ...
        """
        import shutil

        # Rotate existing backups
        for i in range(max_backups - 1, 0, -1):
            old = Path(str(destination) + f".{i}" if i > 1 else str(destination))
            new = Path(str(destination) + f".{i + 1}")
            if old.exists():
                try:
                    shutil.move(str(old), str(new))
                except Exception as e:
                    logging.warning(f"Failed to rotate backup {old} -> {new}: {e}")

        # Rename current backup to .1 (first rotation slot)
        if destination.exists():
            bak1 = Path(str(destination) + ".1")
            try:
                shutil.move(str(destination), str(bak1))
            except Exception as e:
                logging.warning(f"Failed to rotate backup {destination} -> {bak1}: {e}")

        # Run online backup from the write connection
        destination.parent.mkdir(parents=True, exist_ok=True)
        backup_conn = sqlite3.connect(str(destination))

        with self._lock:
            self._write_conn.backup(backup_conn)
            backup_conn.execute("PRAGMA optimize")
            backup_conn.close()

        backup_size = destination.stat().st_size if destination.exists() else 0
        logging.info(f"Cache backup complete: {destination} ({backup_size} bytes)")
        return destination

    def restore_from_backup(self, source: Path) -> bool:
        """
        Restore the database from a backup file.
        WARNING: This replaces the current database entirely.
        Returns True on success, False on failure.
        """
        if not source.exists():
            logging.error(f"Backup file not found: {source}")
            return False

        import shutil

        with self._lock:
            # Close all connections
            self._release_all_read_conns()
            self._write_conn.close()

            try:
                # Replace database with backup
                shutil.copy2(str(source), str(self.db_path))
                logging.info(f"Restored database from backup: {source}")
            except Exception as e:
                logging.error(f"Failed to restore from backup: {e}")
                return False
            finally:
                # Re-open connections
                self._write_conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
                self._write_conn.execute("PRAGMA journal_mode=WAL;")
                with self._pool_lock:
                    self._pool = [
                        self._make_read_conn()
                        for _ in range(self._POOL_SIZE)
                    ]
                self.ensure_tables()

        self.invalidate_memory_cache()
        return True

    def _make_read_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA query_only=ON;")
        return conn

    def _release_all_read_conns(self) -> None:
        """Close all pooled read connections (used before restore)."""
        with self._pool_lock:
            for conn in self._pool:
                try:
                    conn.close()
                except Exception:
                    pass
            self._pool.clear()
            # Reset semaphore
            for _ in range(self._POOL_SIZE):
                try:
                    self._pool_semaphore.acquire(blocking=False)
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Database statistics (for API/monitoring)
    # ------------------------------------------------------------------
    def stats(self) -> Dict[str, Any]:
        """Return a dictionary of database statistics."""
        conn = self._acquire_read_conn()
        try:
            row_counts = {}
            for table in ("existing_media", "strm_cache"):
                cur = conn.execute(f"SELECT COUNT(*) FROM {table}")
                row_counts[table] = cur.fetchone()[0]

            cur = conn.execute("SELECT COUNT(*) FROM strm_cache WHERE allowed = 1")
            allowed_count = cur.fetchone()[0]
            cur = conn.execute("SELECT COUNT(*) FROM strm_cache WHERE allowed = 0")
            excluded_count = cur.fetchone()[0]
            cur = conn.execute("SELECT COUNT(*) FROM strm_cache WHERE path IS NOT NULL AND allowed = 1")
            with_path_count = cur.fetchone()[0]
        finally:
            self._release_read_conn(conn)

        db_size = self.db_path.stat().st_size if self.db_path.exists() else 0
        wal_path = Path(str(self.db_path) + "-wal")
        wal_size = wal_path.stat().st_size if wal_path.exists() else 0

        return {
            "db_path": str(self.db_path),
            "db_size_bytes": db_size,
            "wal_size_bytes": wal_size,
            "row_counts": row_counts,
            "strm_cache_allowed": allowed_count,
            "strm_cache_excluded": excluded_count,
            "strm_cache_with_path": with_path_count,
            "mem_cache_size": len(self._strm_cache_mem) + len(self._existing_media_mem),
        }

    def close(self):
        with self._lock:
            # Close pooled read connections
            with self._pool_lock:
                for conn in self._pool:
                    try:
                        conn.close()
                    except Exception:
                        pass
                self._pool.clear()
            self._write_conn.close()


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
    if not root.exists():
        logging.warning("Media directory not found, skipping: %s", root)
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