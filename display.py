"""
Rich terminal display layer for M3U2STRM 2026.

Provides progress bars, summary tables, banners, and tree views
without touching the core pipeline logic.
"""

import logging
import os
import sys
from pathlib import Path
from typing import List, Optional, Dict, TYPE_CHECKING

from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table
from rich.text import Text
from rich.tree import Tree

if TYPE_CHECKING:
    from config import Config

# ---------------------------------------------------------------------------
# Global console – respects M3U2STRM_PLAIN env var
# ---------------------------------------------------------------------------

_PLAIN = os.environ.get("M3U2STRM_PLAIN", "").strip().lower() in ("1", "true", "yes")

console = Console(
    highlight=False,
    force_terminal=not _PLAIN,
    force_interactive=not _PLAIN,
    no_color=_PLAIN,
    width=None if not _PLAIN else 120,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _style(text: str, *style_args: str) -> str:
    """Return a styled string, or plain text when in plain mode."""
    if _PLAIN:
        return text
    return f"[{' '.join(style_args)}]{text}[/{' '.join(style_args)}]"


def _dim(text: str) -> str:
    return _style(text, "dim")


def rule(title: str = "") -> None:
    """Print a horizontal rule with optional title."""
    if _PLAIN:
        if title:
            console.print(f"--- {title} ---")
        else:
            console.print("-" * 60)
    else:
        from rich.rule import Rule

        console.print(Rule(title))


# ---------------------------------------------------------------------------
# Banner
# ---------------------------------------------------------------------------

def show_banner(cfg: "Config") -> None:
    """Display the M3U2STRM 2026 header and configuration summary."""
    banner = Panel(
        Text.from_markup(
            _style("M3U2STRM 2026\n", "bold bright_cyan")
            + _dim("IPTV VOD → STRM Sync Tool")
        ),
        border_style="bright_cyan",
        padding=(1, 4),
    )
    console.print()
    console.print(banner)

    cfg_lines: list[str] = []
    if len(cfg.m3u) == 1:
        cfg_lines.append(f"  M3U       : {cfg.m3u[0]}")
    else:
        cfg_lines.append(f"  M3U       : {len(cfg.m3u)} sources")
        for i, src in enumerate(cfg.m3u, 1):
            cfg_lines.append(f"    [{i}]     : {src}")
    cfg_lines.append(f"  Output    : {cfg.output_dir}")
    cfg_lines.append(f"  Movie dirs: {len(cfg.movie_media_dirs)}")
    cfg_lines.append(f"  TV dirs   : {len(cfg.tv_media_dirs)}")
    cfg_lines.append(f"  Workers   : {cfg.max_workers}")
    if cfg.dry_run:
        cfg_lines.append(f"  Mode      : {_style('DRY RUN', 'bold yellow')} (no files written)")

    console.print(
        Panel(
            "\n".join(cfg_lines),
            title="Configuration",
            border_style="dim blue",
            padding=(1, 2),
        )
    )


def show_dry_run_warning() -> None:
    """Warn user that dry-run mode is active."""
    console.print(
        Panel(
            _style(
                "DRY RUN MODE — No .strm files will be written to disk.",
                "bold yellow",
            ),
            border_style="yellow",
        )
    )


# ---------------------------------------------------------------------------
# Progress context manager
# ---------------------------------------------------------------------------

class ProgressManager:
    """Thread-safe progress bar manager for pipeline stages."""

    def __init__(self):
        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=None),
            TaskProgressColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=console,
            expand=True,
            transient=False,
        )
        self._tasks: Dict[str, TaskID] = {}

    def __enter__(self) -> "ProgressManager":
        self._progress.start()
        return self

    def __exit__(self, *args):
        self._progress.stop()

    def add_task(self, name: str, total: Optional[int] = None) -> str:
        """Register a new progress task; returns its task id."""
        tid = self._progress.add_task(description=name, total=total)
        self._tasks[name] = tid
        return name

    def update(self, name: str, *, advance: int = 0, total: Optional[int] = None, description: Optional[str] = None) -> None:
        """Update an existing task's progress."""
        tid = self._tasks.get(name)
        if tid is None:
            return
        kwargs: dict = {}
        if advance:
            kwargs["advance"] = advance
        if total is not None:
            kwargs["total"] = total
        if description is not None:
            kwargs["description"] = description
        if kwargs:
            self._progress.update(tid, **kwargs)

    def complete(self, name: str, description: Optional[str] = None) -> None:
        """Mark a task as finished."""
        tid = self._tasks.get(name)
        if tid is None:
            return
        update_kwargs: dict = {"completed": self._progress.tasks[tid].total or 0}
        if description:
            update_kwargs["description"] = description
        self._progress.update(tid, **update_kwargs)


# ---------------------------------------------------------------------------
# Summary table
# ---------------------------------------------------------------------------

def render_summary_table(
    *,
    movies_written: int = 0,
    movies_skipped: int = 0,
    movies_excluded: int = 0,
    tv_written: int = 0,
    tv_skipped: int = 0,
    tv_excluded: int = 0,
    docs_written: int = 0,
    docs_skipped: int = 0,
    docs_excluded: int = 0,
    unknown_written: int = 0,
    unknown_skipped: int = 0,
    unknown_excluded: int = 0,
    orphans_removed: int = 0,
    empty_dirs_removed: int = 0,
    elapsed: Optional[float] = None,
) -> None:
    """Print a rich summary table at the end of a pipeline run."""

    table = Table(
        title="Run Summary",
        title_style="bold",
        border_style="bright_blue",
        show_footer=True,
    )
    table.add_column("Category", style="bold", footer="Total")
    table.add_column("Written", justify="right", style="green", footer=str(
        movies_written + tv_written + docs_written + unknown_written
    ))
    table.add_column("Skipped", justify="right", style="yellow", footer=str(
        movies_skipped + tv_skipped + docs_skipped + unknown_skipped
    ))
    table.add_column("Excluded", justify="right", style="red", footer=str(
        movies_excluded + tv_excluded + docs_excluded + unknown_excluded
    ))

    if movies_written or movies_skipped or movies_excluded:
        table.add_row("Movies", str(movies_written), str(movies_skipped), str(movies_excluded))
    if tv_written or tv_skipped or tv_excluded:
        table.add_row("TV Shows", str(tv_written), str(tv_skipped), str(tv_excluded))
    if docs_written or docs_skipped or docs_excluded:
        table.add_row("Documentaries", str(docs_written), str(docs_skipped), str(docs_excluded))
    if unknown_written or unknown_skipped or unknown_excluded:
        table.add_row("Other", str(unknown_written), str(unknown_skipped), str(unknown_excluded))

    console.print()
    console.print(table)

    if orphans_removed or empty_dirs_removed:
        clean_msg = f"Cleanup: {orphans_removed} orphan STRMs, {empty_dirs_removed} empty dirs removed"
        console.print(_dim(clean_msg))

    if elapsed is not None:
        console.print(_dim(f"Finished in {elapsed:.1f}s"))


# ---------------------------------------------------------------------------
# Tree view of output directory
# ---------------------------------------------------------------------------

def render_strm_tree(output_dir: Path, max_items_per_category: int = 6) -> None:
    """Print a tree view of the STRM output directory."""
    if not output_dir.exists():
        return

    tree = Tree(f"[bold]{output_dir}[/bold]")

    for category_dir in ["Movies", "TV Shows", "Documentaries"]:
        cat_path = output_dir / category_dir
        if not cat_path.is_dir():
            continue

        items = sorted(
            [p for p in cat_path.iterdir() if p.is_dir() and not p.name.startswith(".")],
            key=lambda x: x.name.lower(),
        )
        if not items:
            tree.add(f"{category_dir} {_dim('(empty)')}")
            continue

        cat_branch = tree.add(f"{category_dir} [dim]({len(items)})[/dim]")
        for item in items[:max_items_per_category]:
            strm_files = list(item.rglob("*.strm"))
            count = len(strm_files)
            cat_branch.add(f"{item.name} [dim]({count} strm{'s' if count != 1 else ''})[/dim]")

        remaining = len(items) - max_items_per_category
        if remaining > 0:
            cat_branch.add(_dim(f"... and {remaining} more"))

    console.print()
    console.print(tree)


# ---------------------------------------------------------------------------
# Quick status prints
# ---------------------------------------------------------------------------

def info(msg: str) -> None:
    """Print an informational message (rich-aware)."""
    console.print(_dim(msg))


def success(msg: str) -> None:
    """Print a success message."""
    console.print(_style(f"✓ {msg}", "green"))


def warn(msg: str) -> None:
    """Print a warning message."""
    console.print(_style(f"⚠ {msg}", "yellow"))


def error(msg: str) -> None:
    """Print an error message."""
    console.print(_style(f"✗ {msg}", "bold red"))


def step_header(step: int, title: str) -> None:
    """Print a numbered step header."""
    console.print()
    console.print(_style(f"  [{step}] {title}", "bold"))


# ---------------------------------------------------------------------------
# File log handler – keep writing to logfile
# ---------------------------------------------------------------------------

def setup_file_log(log_path: Path, level: int) -> logging.Handler:
    """Create a file log handler.

    Raises an OSError with a descriptive message if the log directory
    cannot be created or the log file is not writable (e.g. permission
    denied on a bind-mounted host directory).
    """
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise OSError(
            f"Cannot create log directory {log_path.parent}: {exc}\n"
            f"Check that the parent directory is writable by uid={os.getuid()}"
        ) from exc

    if not os.access(str(log_path.parent), os.W_OK):
        raise PermissionError(
            f"Log directory {log_path.parent} is not writable "
            f"by uid={os.getuid()}.  "
            "Verify the host directory permissions and the container user."
        )

    try:
        handler = logging.FileHandler(str(log_path), mode="a", encoding="utf-8")
    except OSError as exc:
        raise OSError(
            f"Cannot open log file {log_path}: {exc}\n"
            f"Ensure the log directory is writable by uid={os.getuid()}"
        ) from exc

    handler.setLevel(level)
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    )
    return handler
