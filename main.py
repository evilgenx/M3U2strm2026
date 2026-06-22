import logging
import re
import time
import concurrent.futures
from pathlib import Path
from collections import defaultdict
import config
from core import (
    SQLiteCache,
    build_existing_media_cache,
    canonical_movie_key,
    canonical_tv_key,
    make_cache_key,
    sanitize_title,
    extract_year,
)
from m3u_utils import (
    parse_m3u,
    split_by_market_filter,
    Category,
    VODEntry,
)
from strm_utils import (
    write_strm_file,
    cleanup_strm_tree,
    movie_strm_path,
    tv_strm_path,
    doc_strm_path,
)
import display


def write_excluded_report(path: Path, excluded, allowed_count: int, enabled: bool):
    if not enabled:
        logging.info("Excluded report skipped (write_non_us_report = false)")
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    movies = [e.raw_title for e in excluded if e.category == Category.MOVIE]
    shows = [e.raw_title for e in excluded if e.category == Category.TVSHOW]
    grouped_shows = defaultdict(list)
    for title in shows:
        base = re.sub(r"[sS]\d{1,2}\s*[eE]\d{1,2}.*", "", title).strip()
        grouped_shows[base].append(title)

    with path.open("w", encoding="utf-8") as f:
        f.write("=== Excluded Entries Report ===\n\n")
        f.write(f"Total allowed: {allowed_count}\n")
        f.write(f"Total excluded: {len(excluded)}\n\n")
        f.write("--- Movies ---\n")
        for m in sorted(movies):
            f.write(f"{m}\n")
        f.write(f"\nTotal movies excluded: {len(movies)}\n\n")
        f.write("--- TV Shows ---\n")
        for base, eps in sorted(grouped_shows.items()):
            f.write(f"{base} — {len(eps)} episodes excluded\n")
        f.write(f"\nTotal shows excluded: {len(grouped_shows)}\n")
        f.write("=== End of Report ===\n")

    logging.info(f"Excluded entries written: {path}")
    display.info(f"Excluded report written: {path}")


def run_pipeline():
    start_time = time.monotonic()

    cfg = config.load_config(Path(__file__).parent / "config.json")

    # ------------------------------------------------------------------
    # Logging: file handler only — console is handled by rich
    # ------------------------------------------------------------------
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    # Suppress noisy third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

    file_handler = display.setup_file_log(cfg.log_file, logging.INFO)
    logger.addHandler(file_handler)

    # ------------------------------------------------------------------
    # Banner + config
    # ------------------------------------------------------------------
    display.show_banner(cfg)

    if cfg.dry_run:
        display.show_dry_run_warning()

    # ------------------------------------------------------------------
    # Aliases
    # ------------------------------------------------------------------
    m3u_sources = cfg.m3u
    output_dir = cfg.output_dir          # root output/ dir (logs, cache, excluded report)
    strm_dir = cfg.strm_output_dir       # output/strm/ (Movies, TV Shows, Documentaries)
    db_path = cfg.sqlite_cache_file
    ignore_keywords = cfg.ignore_keywords or {}
    write_non_us_report = cfg.write_non_us_report

    # ------------------------------------------------------------------
    # Step 1: Scan media directories
    # ------------------------------------------------------------------
    display.rule("Starting media scan")

    progress = display.ProgressManager()
    with progress:
        scan_task = progress.add_task("Scanning media directories", total=len(cfg.existing_media_dirs))
        cache = SQLiteCache(db_path)
        existing = {}

        for d in cfg.existing_media_dirs:
            dir_media = build_existing_media_cache(Path(d))
            existing.update(dir_media)
            progress.update(scan_task, advance=1, description=f"Scanned: {d} ({len(dir_media)} items)")

        progress.complete(scan_task, description=f"{len(existing)} existing media items found")

    cache.replace_existing_media(existing)
    existing_keys = set(existing.keys())
    display.info(f"Cached {len(existing)} existing media entries")

    # ------------------------------------------------------------------
    # Step 2: Parse M3U playlist(s)
    # ------------------------------------------------------------------
    display.rule("Parsing M3U playlist(s)")

    all_entries: list[VODEntry] = []
    with display.ProgressManager() as parse_progress:
        source_count = len(m3u_sources)
        parse_task = parse_progress.add_task(
            f"Parsing M3U source 1/{source_count}",
            total=source_count,
        )
        for i, source in enumerate(m3u_sources, 1):
            parse_progress.update(
                parse_task,
                description=f"Parsing M3U source {i}/{source_count}: {source}",
            )
            source_entries = parse_m3u(
                source,
                tv_keywords=cfg.tv_group_keywords,
                doc_keywords=cfg.doc_group_keywords,
                movie_keywords=cfg.movie_group_keywords,
                replay_keywords=cfg.replay_group_keywords,
                ignore_keywords=cfg.ignore_keywords,
            )
            all_entries.extend(source_entries)
            display.info(f"  [{i}/{source_count}] {source}: {len(source_entries)} entries")
            parse_progress.update(parse_task, advance=1)

        total_before_dedup = len(all_entries)
        parse_progress.complete(parse_task, description=f"Total: {total_before_dedup} entries across {source_count} source(s)")

    entries = all_entries
    display.info(f"Combined: {len(entries)} entries from {source_count} source(s)")

    # ------------------------------------------------------------------
    # Step 3: Deduplicate
    # ------------------------------------------------------------------
    unique_entries: dict[str, VODEntry] = {}
    for e in entries:
        if e.category == Category.MOVIE:
            key = canonical_movie_key(e.raw_title)
        elif e.category == Category.TVSHOW:
            m = re.search(r"[sS](\d{1,2})\s*[eE](\d{1,2})", e.raw_title)
            if m:
                season, episode = int(m.group(1)), int(m.group(2))
                base = re.sub(r"[sS]\d{1,2}\s*[eE]\d{1,2}.*", "", e.raw_title).strip()
                key = canonical_tv_key(base, season, episode)
            else:
                key = make_cache_key(e.raw_title)
        elif e.category == Category.DOCUMENTARY:
            key = canonical_movie_key(e.raw_title)
        else:
            key = make_cache_key(e.raw_title)
        unique_entries[key] = e

    entries = list(unique_entries.values())
    logging.info("Deduplicated playlist entries: %d -> %d unique", len(entries), len(unique_entries))
    display.info(f"Deduplicated: {len(entries)} unique entries")

    # ------------------------------------------------------------------
    # Step 4: Check cache & existing
    # ------------------------------------------------------------------
    strm_cache = cache.strm_cache_dict()
    logging.debug("Loaded %d entries from strm_cache", len(strm_cache))

    to_check: list[VODEntry] = []
    reused_allowed: list[VODEntry] = []
    reused_excluded: list[VODEntry] = []

    for e in entries:
        key = _entry_key(e)
        if key in existing_keys:
            reused_allowed.append(e)
            logging.debug(f"Reusing local-existing result for {e.raw_title}")
            continue

        cached = strm_cache.get(key)
        if cached and cached.get("allowed") is not None:
            if cached["allowed"] == 1:
                reused_allowed.append(e)
                logging.debug(f"Reusing cached allowed result for {e.raw_title}")
            else:
                reused_excluded.append(e)
                logging.debug(f"Reusing cached excluded result for {e.raw_title}")
        else:
            logging.debug("CACHE MISS: raw_title=%r key=%s cached_entry=%s", e.raw_title, key, strm_cache.get(key))
            to_check.append(e)

    display.info(f"Cache hits: {len(reused_allowed)} allowed + {len(reused_excluded)} excluded")
    display.info(f"Entries to process: {len(to_check)}")

    # ------------------------------------------------------------------
    # Step 5: Filter (ignore keywords)
    # ------------------------------------------------------------------
    allowed, excluded = split_by_market_filter(
        to_check,
        ignore_keywords=cfg.ignore_keywords,
    )
    allowed.extend(reused_allowed)
    excluded.extend(reused_excluded)

    write_excluded_report(strm_dir / "excluded_entries.txt", excluded, len(allowed), write_non_us_report)

    # ------------------------------------------------------------------
    # Step 6: Write STRM files with progress bar
    # ------------------------------------------------------------------
    display.rule("Writing STRM files")

    strm_cache = cache.strm_cache_dict()
    new_cache = strm_cache.copy()

    # Categorized counts for the summary table
    counts: dict[str, dict[str, int]] = defaultdict(lambda: {"written": 0, "skipped": 0, "excluded": 0})

    def process_entry(e):
        """Process a single allowed entry — thread-safe."""
        key = None
        rel_path = None

        logging.debug(
            "PROCESS START: raw_title=%r, safe_title=%r, category=%s, year=%s, url=%s",
            getattr(e, "raw_title", None),
            getattr(e, "safe_title", None),
            getattr(e, "category", None),
            getattr(e, "year", None),
            getattr(e, "url", None),
        )

        if not e.year:
            e.year = extract_year(e.raw_title)
            if e.year:
                logging.debug("Extracted year=%s from raw_title %r", e.year, e.raw_title)

        ignore = ignore_keywords.get("tvshows" if e.category == Category.TVSHOW else "movies", [])
        if any(word.lower() in e.raw_title.lower() for word in ignore):
            logging.debug("Ignored by keyword: %s", e.raw_title)
            return None

        try:
            if e.category == Category.MOVIE:
                key = canonical_movie_key(e.raw_title)
                logging.debug(f"Key built for {e.raw_title} (MOVIE): {key}")
                rel_path = movie_strm_path(strm_dir, e)
                cat = "movies"
            elif e.category == Category.TVSHOW:
                base = re.sub(r"[sS]\d{1,2}\s*[eE]\d{1,2}.*", "", e.raw_title).strip()
                m = re.search(r"[sS](\d{1,2})\s*[eE](\d{1,2})", e.raw_title)
                if m:
                    season, episode = int(m.group(1)), int(m.group(2))
                    key = canonical_tv_key(base, season, episode)
                    logging.debug(f"Key built for {e.raw_title} (TVSHOW S{season:02d}E{episode:02d}): {key}")
                    rel_path = tv_strm_path(
                        strm_dir,
                        VODEntry(
                            raw_title=base,
                            safe_title=sanitize_title(base),
                            url=e.url,
                            category=e.category,
                            year=e.year,
                        ),
                        season,
                        episode,
                    )
                else:
                    key = make_cache_key(e.raw_title)
                    logging.debug(f"Key built for {e.raw_title} (TVSHOW no S/E): {key}")
                    rel_path = tv_strm_path(strm_dir, e, 1, 1)
                cat = "tv"
            elif e.category == Category.DOCUMENTARY:
                key = canonical_movie_key(e.raw_title)
                logging.debug(f"Key built for {e.raw_title} (DOC): {key}")
                rel_path = doc_strm_path(strm_dir, e)
                cat = "docs"
            else:
                logging.warning("Unknown category %s for entry %r", e.category, e.raw_title)
                return None

            if not key:
                logging.error("No cache key generated for %r", e.raw_title)
                return None

            abs_path = rel_path
            url = e.url

            if key in existing_keys:
                logging.debug("Skip existing media: %s", e.raw_title)
                return {
                    "action": "skipped_existing",
                    "key": key,
                    "cat": cat,
                    "cache_entry": {"url": e.url, "path": None, "allowed": 1},
                }

            cached = strm_cache.get(key)
            if cached:
                cached_path = Path(cached.get("path") or "").resolve() if cached.get("path") else None
                if cached.get("url") == url and cached.get("path") and cached_path == abs_path.resolve():
                    logging.debug("Skip cached (unchanged): %s", e.raw_title)
                    return {
                        "action": "skipped_cached",
                        "key": key,
                        "cat": cat,
                        "cache_entry": {
                            "url": cached.get("url"),
                            "path": cached.get("path"),
                            "allowed": cached.get("allowed", 1),
                        },
                    }

            if not cfg.dry_run:
                write_strm_file(strm_dir, rel_path, url)
                logging.info("STRM written: %s", abs_path)
            else:
                logging.info("DRY RUN — would write: %s", abs_path)

            return {
                "action": "written",
                "key": key,
                "cat": cat,
                "cache_entry": {"url": url, "path": str(abs_path.resolve()), "allowed": 1},
            }
        except Exception as ex:
            logging.error(
                "Error processing entry %r (category=%s, year=%s): %s",
                e.raw_title,
                getattr(e, "category", None),
                getattr(e, "year", None),
                ex,
                exc_info=True,
            )
            return None

    progress = display.ProgressManager()
    with progress:
        write_task = progress.add_task("Processing entries", total=len(allowed))
        written_count = 0
        skipped_count = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=cfg.max_workers) as executor:
            futures = {executor.submit(process_entry, e): e for e in allowed}
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result is None:
                    progress.update(write_task, advance=1)
                    continue

                action = result["action"]
                cat = result.get("cat", "unknown")
                new_cache[result["key"]] = result["cache_entry"]

                if action in ("skipped_existing", "skipped_cached"):
                    skipped_count += 1
                    counts[cat]["skipped"] += 1
                elif action == "written":
                    written_count += 1
                    counts[cat]["written"] += 1

                progress.update(
                    write_task,
                    advance=1,
                    description=f"Processing ({written_count} new, {skipped_count} skipped)",
                )

        progress.complete(write_task, description=f"Done: {written_count} written, {skipped_count} skipped")

    # ------------------------------------------------------------------
    # Step 7: Cache excluded entries
    # ------------------------------------------------------------------
    for e in excluded:
        key = _entry_key(e)
        new_cache[key] = {"url": e.url, "path": None, "allowed": 0}
        if e.category == Category.MOVIE:
            counts["movies"]["excluded"] += 1
        elif e.category == Category.TVSHOW:
            counts["tv"]["excluded"] += 1
        elif e.category == Category.DOCUMENTARY:
            counts["docs"]["excluded"] += 1
        else:
            counts["unknown"]["excluded"] += 1

    # ------------------------------------------------------------------
    # Step 8: Sync cache
    # ------------------------------------------------------------------
    display.rule("Syncing cache")
    sync_stats = cache.sync_strm_cache(new_cache)
    logging.info(
        f"Cache sync complete: {sync_stats['updated']} entries updated, "
        f"{sync_stats['deleted']} deleted, {sync_stats['total']} total"
    )
    display.info(f"Cache: {sync_stats['updated']} updated, {sync_stats['deleted']} deleted")

    # ------------------------------------------------------------------
    # Step 9: Cleanup orphan STRMs
    # ------------------------------------------------------------------
    display.rule("Cleaning orphan STRMs")
    logging.info("Cleaning up orphan STRMs...")
    cleanup_strm_tree(strm_dir, new_cache)

    # ------------------------------------------------------------------
    # Step 10: Final summary
    # ------------------------------------------------------------------
    elapsed = time.monotonic() - start_time

    display.render_summary_table(
        movies_written=counts["movies"]["written"],
        movies_skipped=counts["movies"]["skipped"],
        movies_excluded=counts["movies"]["excluded"],
        tv_written=counts["tv"]["written"],
        tv_skipped=counts["tv"]["skipped"],
        tv_excluded=counts["tv"]["excluded"],
        docs_written=counts["docs"]["written"],
        docs_skipped=counts["docs"]["skipped"],
        docs_excluded=counts["docs"]["excluded"],
        unknown_written=counts["unknown"]["written"],
        unknown_skipped=counts["unknown"]["skipped"],
        unknown_excluded=counts["unknown"]["excluded"],
        elapsed=elapsed,
    )

    # Show output tree
    display.render_strm_tree(strm_dir)

    logging.info(
        f"VOD/Strm process complete: {written_count} STRMs written, "
        f"{skipped_count} skipped, {len(excluded)} excluded"
    )


def _entry_key(e: VODEntry) -> str:
    """Extract a cache key for a VODEntry (reusable helper)."""
    if e.category == Category.MOVIE:
        return canonical_movie_key(e.raw_title)
    elif e.category == Category.TVSHOW:
        m = re.search(r"[sS](\d{1,2})\s*[eE](\d{1,2})", e.raw_title)
        if m:
            season, episode = int(m.group(1)), int(m.group(2))
            base = re.sub(r"[sS]\d{1,2}\s*[eE]\d{1,2}.*", "", e.raw_title).strip()
            return canonical_tv_key(base, season, episode)
        else:
            return make_cache_key(e.raw_title)
    elif e.category == Category.DOCUMENTARY:
        return canonical_movie_key(e.raw_title)
    else:
        return make_cache_key(e.raw_title)


if __name__ == "__main__":
    run_pipeline()