import logging
import os
import re
import shutil
from pathlib import Path
from typing import Any, Dict, Optional, TYPE_CHECKING
from core import extract_year

if TYPE_CHECKING:
    from m3u_utils import VODEntry


def write_strm_file(base_dir: Path, relative_path: Path, url: str) -> Path:
    target = base_dir / relative_path
    if target.exists():
        try:
            old = target.read_text(encoding="utf-8", errors="ignore").strip()
            if old.strip().lower() == url.strip().lower():
                logging.debug(f"STRM unchanged, skip: {target}")
                return target
        except Exception as e:
            logging.warning(f"Error reading existing STRM {target}: {e}")
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        with target.open("w", encoding="utf-8") as f:
            f.write(url.strip() + "\n")
        logging.info(f"STRM written: {target}")
    except Exception as e:
        logging.error(f"Failed to write STRM {target}: {e}")
        raise
    return target


def cleanup_strm_tree(base_dir: Path, cache: Dict[str, Dict[str, Any]]):
    if not base_dir.exists():
        return
    if not cache:
        logging.warning("Cache is empty — skipping cleanup to avoid deleting everything.")
        return
    valid_paths = {Path(p) for d in cache.values() if (p := d.get("path"))}
    removed_files = 0
    removed_dirs = 0
    protected_roots = {"Movies", "TV Shows", "Documentaries"}
    for dirpath, _, filenames in os.walk(base_dir, topdown=False):
        dirp = Path(dirpath)
        for strm_file in [f for f in filenames if f.endswith(".strm")]:
            strm_path = dirp / strm_file
            if strm_path not in valid_paths:
                try:
                    strm_path.unlink()
                    removed_files += 1
                    logging.debug(f"Removed orphan STRM: {strm_path}")
                except Exception as e:
                    logging.error(f"Failed to remove orphan STRM {strm_path}: {e}")
        if dirp.name in protected_roots:
            continue
        try:
            items = list(dirp.iterdir())
            files = [f for f in items if f.is_file()]
            subdirs = [f for f in items if f.is_dir()]
            if not files and not subdirs:
                dirp.rmdir()
                removed_dirs += 1
                logging.debug(f"Removed empty directory: {dirp}")
            elif files and all(f.suffix.lower() == ".nfo" for f in files) and not subdirs:
                shutil.rmtree(dirp)
                removed_dirs += 1
                logging.debug(f"Removed NFO-only directory: {dirp}")
        except Exception as e:
            logging.error(f"Error checking directory {dirp}: {e}")
    if removed_files or removed_dirs:
        logging.info(f"Cleanup complete: removed {removed_files} orphan STRMs and {removed_dirs} directories")


def movie_strm_path(base_dir: Path, entry: "VODEntry") -> Path:
    title_clean = entry.safe_title
    year = entry.year or extract_year(entry.raw_title)
    if year:
        folder = f"{title_clean} ({year})"
        fn = f"{title_clean} ({year})"
    else:
        folder = title_clean
        fn = title_clean
    return base_dir / "Movies" / folder / f"{fn}.strm"


def tv_strm_path(base_dir: Path, entry: "VODEntry", season: int, episode: int) -> Path:
    series_clean = entry.safe_title
    year = entry.year or extract_year(entry.raw_title)
    if year:
        folder = f"{series_clean} ({year})"
        fn = f"{series_clean} ({year}) S{season:02d}E{episode:02d}"
    else:
        folder = series_clean
        fn = f"{series_clean} S{season:02d}E{episode:02d}"
    return base_dir / "TV Shows" / folder / f"Season {season:02d}" / f"{fn}.strm"


def doc_strm_path(base_dir: Path, entry: "VODEntry") -> Path:
    title_clean = entry.safe_title
    year = entry.year or extract_year(entry.raw_title)
    if year:
        folder = f"{title_clean} ({year})"
        fn = f"{title_clean} ({year})"
    else:
        folder = title_clean
        fn = title_clean
    return base_dir / "Documentaries" / folder / f"{fn}.strm"
