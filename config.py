import os
from dataclasses import dataclass
from pathlib import Path
import json
from typing import List, Dict, Optional


@dataclass
class Config:
    m3u: List[str]
    sqlite_cache_file: Path
    log_file: Path
    output_dir: Path
    existing_media_dirs: List[Path]
    dry_run: bool = False
    max_workers: Optional[int] = None
    write_non_us_report: bool = True
    tv_group_keywords: List[str] = None
    doc_group_keywords: List[str] = None
    movie_group_keywords: List[str] = None
    replay_group_keywords: List[str] = None
    ignore_keywords: Dict[str, List[str]] = None


def _coerce_bool(val, default=False) -> bool:
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.strip().lower() == "true"
    return default


def load_config(path: Path) -> Config:
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    mw = data.get("max_workers")
    if isinstance(mw, str) and mw.lower() == "max":
        mw = os.cpu_count() or 8
    if "existing_media_dirs" in data:
        existing_dirs = [Path(p) for p in data["existing_media_dirs"]]
    elif "existing_media_dir" in data:
        existing_dirs = [Path(data["existing_media_dir"])]
    else:
        raise KeyError("Config missing 'existing_media_dir' or 'existing_media_dirs'")
    raw_m3u = data["m3u"]
    m3u_list: List[str] = (
        [s.strip() for s in raw_m3u.split(",") if s.strip()]
        if isinstance(raw_m3u, str)
        else list(raw_m3u)
    )
    return Config(
        m3u=m3u_list,
        sqlite_cache_file=Path(data["sqlite_cache_file"]),
        log_file=Path(data["log_file"]),
        output_dir=Path(data["output_dir"]),
        existing_media_dirs=existing_dirs,
        dry_run=_coerce_bool(data.get("dry_run", False)),
        max_workers=mw,
        write_non_us_report=_coerce_bool(data.get("write_non_us_report", True)),
        tv_group_keywords=data.get("tv_group_keywords", []),
        doc_group_keywords=data.get("doc_group_keywords", []),
        movie_group_keywords=data.get("movie_group_keywords", []),
        replay_group_keywords=data.get("replay_group_keywords", []),
        ignore_keywords=data.get("ignore_keywords", {}),
    )
