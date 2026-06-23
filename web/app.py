"""
M3U2strm2026 Web Admin — Flask application.

Routes:
    /               Dashboard with status, last-run summary, trigger, scheduler
    /config         Config editor form
    /logs           Log viewer
    /browse         STRM output tree browser
    /api/run        POST — trigger pipeline run
    /api/status     GET  — current status JSON
"""

import json
import logging
import os
import sys
import threading
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path

import io
from flask import (
    Flask,
    Response,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)

# ---------------------------------------------------------------------------
# Ensure the project root is on sys.path so we can import main, config, etc.
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# ---------------------------------------------------------------------------
# Import project modules — suppress Rich terminal output via env var
# (M3U2STRM_PLAIN=1 must be set before importing display)
# ---------------------------------------------------------------------------
os.environ.setdefault("M3U2STRM_PLAIN", "1")

from config import load_config, Config  # noqa: E402
from core import SQLiteCache  # noqa: E402
from main import run_pipeline  # noqa: E402
from strm_utils import (  # noqa: E402
    cleanup_strm_tree,
    movie_strm_path,
    tv_strm_path,
    doc_strm_path,
)
from m3u_utils import Category, VODEntry  # noqa: E402

# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------
app = Flask(__name__)

# ---------------------------------------------------------------------------
# Paths (overrideable via env)
# ---------------------------------------------------------------------------
CONFIG_PATH = Path(os.environ.get("CONFIG_FILE", _PROJECT_ROOT / "config.json"))
OUTPUT_DIR = _PROJECT_ROOT / "output"
LOG_PATH = OUTPUT_DIR / "logs" / "M3U2Strm.log"
CACHE_PATH = OUTPUT_DIR / "caches.db"

# ---------------------------------------------------------------------------
# Pipeline run state (in-memory)
# ---------------------------------------------------------------------------
_pipeline_lock = threading.Lock()
_pipeline_state = {
    "running": False,
    "last_run_start": None,        # ISO timestamp
    "last_run_end": None,          # ISO timestamp
    "last_elapsed": None,          # seconds
    "last_summary": None,          # dict: counts per category
    "error": None,                 # last error message
    "current_progress": "",        # progress description
}


def _run_pipeline_background(dry_run: bool = False) -> None:
    """
    Execute the pipeline in a background thread, updating _pipeline_state.
    Sets M3U2STRM_PLAIN=1 to suppress Rich output and redirects stdout/stderr
    to capture log-like output.
    """
    global _pipeline_state

    with _pipeline_lock:
        if _pipeline_state["running"]:
            return  # already running
        _pipeline_state["running"] = True
        _pipeline_state["error"] = None
        _pipeline_state["last_summary"] = None
        _pipeline_state["current_progress"] = "Starting pipeline..."
        _pipeline_state["last_run_start"] = datetime.now().isoformat()

    def _runner() -> None:
        global _pipeline_state

        old_stdout = sys.stdout
        old_stderr = sys.stderr
        captured = io.StringIO()

        try:
            sys.stdout = captured
            sys.stderr = captured

            # -----------------------------------------------------------------
            # Pre-create all output directories.
            # If config.json is missing, copy from the example template.
            # -----------------------------------------------------------------
            if not CONFIG_PATH.exists():
                example = CONFIG_PATH.with_name("config.json.example")
                if example.exists():
                    import shutil
                    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(example, CONFIG_PATH)
                    app.logger.info(
                        "Created %s from example template", CONFIG_PATH
                    )
                else:
                    app.logger.warning(
                        "No config found at %s and no example available",
                        CONFIG_PATH,
                    )

            # All output lives under OUTPUT_DIR — create it and its children
            for subdir in ("logs", "strm"):
                (OUTPUT_DIR / subdir).mkdir(parents=True, exist_ok=True)

            # Verify the log directory is writable before launching
            log_dir = OUTPUT_DIR / "logs"
            if not os.access(str(log_dir), os.W_OK):
                msg = (
                    f"Log directory {log_dir} is not writable "
                    f"(uid={os.getuid()}).  Pipeline aborted."
                )
                app.logger.error(msg)
                raise RuntimeError(msg)

            start = time.monotonic()

            run_pipeline()

            elapsed = time.monotonic() - start

            with _pipeline_lock:
                _pipeline_state["last_run_end"] = datetime.now().isoformat()
                _pipeline_state["last_elapsed"] = round(elapsed, 1)
                _pipeline_state["last_summary"] = _build_summary_from_config()
                _pipeline_state["current_progress"] = f"Finished in {elapsed:.1f}s"
                _pipeline_state["running"] = False

        except Exception as exc:
            tb = traceback.format_exc()
            logging.getLogger("web").error(f"Pipeline failed: {exc}\n{tb}")

            with _pipeline_lock:
                _pipeline_state["last_run_end"] = datetime.now().isoformat()
                _pipeline_state["error"] = str(exc)[:500]
                _pipeline_state["running"] = False
                _pipeline_state["current_progress"] = "Pipeline failed — see logs"

        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr

            # Write captured output to log file
            output = captured.getvalue()
            if output.strip():
                try:
                    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
                    with open(LOG_PATH, "a", encoding="utf-8") as lf:
                        lf.write(f"\n--- Pipeline run output ---\n{output}\n")
                except Exception:
                    pass

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()


def _build_summary_from_config() -> dict:
    """Build a summary dict by scanning current output directory and cache."""
    summary = {
        "movies": {"written": 0, "skipped": 0, "excluded": 0},
        "tv": {"written": 0, "skipped": 0, "excluded": 0},
        "docs": {"written": 0, "skipped": 0, "excluded": 0},
        "unknown": {"written": 0, "skipped": 0, "excluded": 0},
    }

    if not CONFIG_PATH.exists():
        return summary

    try:
        cfg = load_config(CONFIG_PATH)
    except Exception:
        return summary

    strm_dir = cfg.strm_output_dir
    if strm_dir.exists():
        for cat, cat_label in [("movies", "Movies"), ("tv", "TV Shows"), ("docs", "Documentaries")]:
            cat_path = strm_dir / cat_label
            if cat_path.is_dir():
                count = sum(1 for _ in cat_path.rglob("*.strm"))
                summary[cat]["written"] = count

    if CACHE_PATH.exists():
        try:
            cache = SQLiteCache(CACHE_PATH)
            cached = cache.strm_cache_dict()
            excluded_count = sum(1 for v in cached.values() if v.get("allowed") == 0)
            skipped_count = sum(1 for v in cached.values() if v.get("allowed") == 1 and v.get("path") is None)
            summary["movies"]["excluded"] = excluded_count
            summary["movies"]["skipped"] = skipped_count
            cache.close()
        except Exception:
            pass

    return summary


# ---------------------------------------------------------------------------
# Scheduler (APScheduler)
# ---------------------------------------------------------------------------
try:
    from apscheduler.schedulers.background import BackgroundScheduler

    _scheduler = BackgroundScheduler(daemon=True)
    _scheduler_started = False
    _current_schedule_interval = 0  # minutes; 0 = disabled
except ImportError:
    _scheduler = None
    _scheduler_started = False
    _current_schedule_interval = 0


def _get_schedule_interval() -> int:
    """Read schedule_interval_minutes from config.json, default 0 (disabled)."""
    env_val = os.environ.get("SCHEDULE_INTERVAL_MINUTES", "")
    if env_val:
        try:
            return int(env_val)
        except ValueError:
            pass
    if CONFIG_PATH.exists():
        try:
            data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
            return int(data.get("schedule_interval_minutes", 0))
        except Exception:
            pass
    return 0


def _start_scheduler() -> None:
    """Start or restart the scheduler with the current interval."""
    global _scheduler_started, _current_schedule_interval

    if _scheduler is None:
        return

    interval = _get_schedule_interval()
    if interval == _current_schedule_interval and _scheduler_started:
        return

    # Remove existing jobs
    if _scheduler_started:
        _scheduler.remove_all_jobs()

    if interval > 0:
        _scheduler.add_job(
            lambda: _run_pipeline_background(dry_run=False),
            "interval",
            minutes=interval,
            id="pipeline_scheduled",
            replace_existing=True,
            next_run_time=datetime.now() + timedelta(minutes=interval),
        )
        app.logger.info(f"Scheduler started: pipeline every {interval} minutes")
    else:
        app.logger.info("Scheduler disabled (interval = 0)")

    if not _scheduler_started:
        _scheduler.start()
        _scheduler_started = True

    _current_schedule_interval = interval


# Initial scheduler start (if imported)
if _scheduler is not None:
    try:
        _start_scheduler()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def dashboard() -> str:
    """Dashboard — status overview & run controls."""
    cfg = None
    cfg_error = None
    if CONFIG_PATH.exists():
        try:
            cfg = load_config(CONFIG_PATH)
        except Exception as e:
            cfg_error = str(e)

    # Scheduler info
    sched_interval = _get_schedule_interval()
    next_run = None
    if _scheduler is not None and _scheduler_started and sched_interval > 0:
        job = _scheduler.get_job("pipeline_scheduled")
        if job and job.next_run_time:
            next_run = job.next_run_time.isoformat()

    return render_template(
        "dashboard.html",
        config=cfg,
        config_error=cfg_error,
        state=_pipeline_state,
        schedule_interval=sched_interval,
        next_run=next_run,
    )


@app.route("/config", methods=["GET", "POST"])
def config_editor() -> str:
    """View and edit config.json."""
    save_message = None
    save_error = None

    if request.method == "POST":
        try:
            data = _parse_config_form(request.form)
            CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
            CONFIG_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")
            save_message = "Configuration saved successfully."
            # Restart scheduler if interval changed
            if _scheduler is not None:
                try:
                    _start_scheduler()
                except Exception:
                    pass
        except Exception as e:
            save_error = str(e)

    # Load existing config for the form
    if CONFIG_PATH.exists():
        try:
            raw = CONFIG_PATH.read_text(encoding="utf-8")
            config_data = json.loads(raw)
            config_pretty = json.dumps(config_data, indent=2)
        except Exception as e:
            config_data = {}
            config_pretty = ""
            if not save_error:
                save_error = f"Error reading config: {e}"
    else:
        config_data = {}
        config_pretty = ""

    return render_template(
        "config.html",
        config_data=config_data,
        config_pretty=config_pretty,
        save_message=save_message,
        save_error=save_error,
    )


def _parse_config_form(form: dict) -> dict:
    """Convert form data back into a config.json dict."""
    data: dict = {}

    # M3U sources (textarea, one per line)
    m3u_raw = form.get("m3u", "")
    m3u_lines = [line.strip() for line in m3u_raw.splitlines() if line.strip()]
    if len(m3u_lines) == 1:
        data["m3u"] = m3u_lines[0]
    else:
        data["m3u"] = m3u_lines

    movie_dirs_raw = form.get("movie_media_dirs", "")
    data["movie_media_dirs"] = [
        d.strip() for d in movie_dirs_raw.splitlines() if d.strip()
    ]

    tv_dirs_raw = form.get("tv_media_dirs", "")
    data["tv_media_dirs"] = [
        d.strip() for d in tv_dirs_raw.splitlines() if d.strip()
    ]

    data["dry_run"] = form.get("dry_run") == "1"
    data["write_non_us_report"] = form.get("write_non_us_report") == "1"

    mw = form.get("max_workers", "").strip()
    if mw.lower() == "max":
        data["max_workers"] = "max"
    elif mw.isdigit():
        data["max_workers"] = int(mw)

    # Schedule interval
    sched = form.get("schedule_interval_minutes", "0").strip()
    if sched.isdigit():
        data["schedule_interval_minutes"] = int(sched)
    else:
        data["schedule_interval_minutes"] = 0

    # Keyword lists
    for key in [
        "tv_group_keywords",
        "doc_group_keywords",
        "movie_group_keywords",
        "replay_group_keywords",
    ]:
        raw = form.get(key, "")
        data[key] = [k.strip() for k in raw.splitlines() if k.strip()]

    ignore_keywords = {}
    for section in ["tvshows", "movies", "documentaries"]:
        raw = form.get(f"ignore_{section}", "")
        items = [k.strip() for k in raw.splitlines() if k.strip()]
        if items:
            ignore_keywords[section] = items
    data["ignore_keywords"] = ignore_keywords

    return data


@app.route("/logs")
def log_viewer() -> str:
    """Display recent log entries."""
    lines: list[str] = []
    total_lines = 0

    if LOG_PATH.exists():
        try:
            with open(LOG_PATH, "r", encoding="utf-8", errors="ignore") as f:
                all_lines = f.readlines()
                total_lines = len(all_lines)
                # Show last 500 lines
                lines = [line.rstrip() for line in all_lines[-500:]]
        except Exception as e:
            lines = [f"Error reading log file: {e}"]

    auto_refresh = request.args.get("auto_refresh", "0") == "1"

    return render_template(
        "logs.html",
        lines=lines,
        total_lines=total_lines,
        log_path=str(LOG_PATH),
        auto_refresh=auto_refresh,
    )


@app.route("/browse")
def browse() -> str:
    """Browse the STRM output directory tree."""
    cfg = None
    tree_data = None
    error = None

    if CONFIG_PATH.exists():
        try:
            cfg = load_config(CONFIG_PATH)
        except Exception as e:
            error = f"Config error: {e}"

    if cfg and cfg.strm_output_dir.exists():
        tree_data = _build_tree(cfg.strm_output_dir)
    elif cfg:
        error = f"STRM output directory does not exist: {cfg.strm_output_dir}"

    return render_template(
        "browse.html",
        config=cfg,
        tree_data=tree_data,
        error=error,
    )


def _build_tree(base_dir: Path) -> dict:
    """Build a nested dict representing the output directory tree."""
    categories = {}
    for cat_label in ["Movies", "TV Shows", "Documentaries"]:
        cat_path = base_dir / cat_label
        if not cat_path.is_dir():
            continue
        items = []
        for entry in sorted(cat_path.iterdir(), key=lambda x: x.name.lower()):
            if entry.name.startswith("."):
                continue
            if entry.is_dir():
                strm_files = sorted(
                    [f.name for f in entry.rglob("*.strm")],
                    key=lambda x: x.lower(),
                )
                subdirs = sorted(
                    [d for d in entry.iterdir() if d.is_dir() and not d.name.startswith(".")],
                    key=lambda x: x.name.lower(),
                )
                items.append({
                    "name": entry.name,
                    "type": "dir",
                    "strm_count": len(strm_files),
                    "strm_files": strm_files[:20],  # limit per folder
                    "subdirs": [
                        {
                            "name": sd.name,
                            "type": "dir",
                            "strm_count": sum(1 for _ in sd.rglob("*.strm")),
                            "strm_files": sorted(
                                [f.name for f in sd.rglob("*.strm")],
                                key=lambda x: x.lower(),
                            )[:20],
                            "subdirs": [],
                        }
                        for sd in subdirs
                    ],
                })
        categories[cat_label] = {
            "items": items,
            "total_strm": sum(item["strm_count"] for item in items),
        }
    return categories


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

@app.route("/api/run", methods=["POST"])
def api_run() -> Response:
    """Trigger a pipeline run."""
    dry_run = request.json.get("dry_run", False) if request.is_json else False
    with _pipeline_lock:
        if _pipeline_state["running"]:
            return jsonify({"status": "error", "message": "Pipeline already running"}), 409

    _run_pipeline_background(dry_run=dry_run)
    return jsonify({"status": "ok", "message": "Pipeline started"})


@app.route("/api/status")
def api_status() -> Response:
    """Return current pipeline status as JSON."""
    with _pipeline_lock:
        sched_interval = _get_schedule_interval()
        next_run = None
        if _scheduler is not None and _scheduler_started and sched_interval > 0:
            job = _scheduler.get_job("pipeline_scheduled")
            if job and job.next_run_time:
                next_run = job.next_run_time.isoformat()

        # Gather cache stats when not running (avoid contention)
        cache_stats = None
        if not _pipeline_state["running"] and CACHE_PATH.exists():
            try:
                cache = SQLiteCache(CACHE_PATH)
                cache_stats = cache.stats()
                cache.close()
            except Exception:
                pass

        return jsonify({
            "running": _pipeline_state["running"],
            "last_run_start": _pipeline_state["last_run_start"],
            "last_run_end": _pipeline_state["last_run_end"],
            "last_elapsed": _pipeline_state["last_elapsed"],
            "last_summary": _pipeline_state["last_summary"],
            "error": _pipeline_state["error"],
            "current_progress": _pipeline_state["current_progress"],
            "scheduler": {
                "interval_minutes": sched_interval,
                "next_run": next_run,
                "enabled": sched_interval > 0,
            },
            "cache": cache_stats,
        })


# ---------------------------------------------------------------------------
# Cache management API
# ---------------------------------------------------------------------------

@app.route("/api/cache/stats")
def api_cache_stats() -> Response:
    """Return detailed database and cache statistics."""
    if not CACHE_PATH.exists():
        return jsonify({"error": "Cache database does not exist yet"}), 404

    try:
        cache = SQLiteCache(CACHE_PATH)
        stats = cache.stats()
        cache.close()
        return jsonify(stats)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/cache/maintenance", methods=["POST"])
def api_cache_maintenance() -> Response:
    """Run database maintenance (WAL checkpoint, integrity check, optimize)."""
    if not CACHE_PATH.exists():
        return jsonify({"error": "Cache database does not exist yet"}), 404

    with _pipeline_lock:
        if _pipeline_state["running"]:
            return jsonify({"error": "Cannot run maintenance while pipeline is running"}), 409

    try:
        cache = SQLiteCache(CACHE_PATH)
        result = cache.maintenance()
        cache.close()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/cache/backup", methods=["POST"])
def api_cache_backup() -> Response:
    """Trigger an on-demand cache backup."""
    if not CACHE_PATH.exists():
        return jsonify({"error": "Cache database does not exist yet"}), 404

    backup_path = OUTPUT_DIR / "caches.db.bak"

    try:
        cache = SQLiteCache(CACHE_PATH)
        dest = cache.backup(backup_path)
        stats = cache.stats()
        cache.close()
        return jsonify({
            "status": "ok",
            "backup_path": str(dest),
            "backup_size_bytes": dest.stat().st_size if dest.exists() else 0,
            "db_size_bytes": stats["db_size_bytes"],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/cache/restore", methods=["POST"])
def api_cache_restore() -> Response:
    """Restore the cache database from the latest backup.
    Returns an error if the pipeline is currently running.
    """
    with _pipeline_lock:
        if _pipeline_state["running"]:
            return jsonify({"error": "Cannot restore while pipeline is running"}), 409

    backup_path = OUTPUT_DIR / "caches.db.bak"
    if not backup_path.exists():
        return jsonify({"error": f"No backup found at {backup_path}"}), 404

    try:
        cache = SQLiteCache(CACHE_PATH)
        success = cache.restore_from_backup(backup_path)
        if success:
            stats = cache.stats()
            cache.close()
            return jsonify({
                "status": "ok",
                "message": "Database restored from backup",
                "db_size_bytes": stats["db_size_bytes"],
                "row_counts": stats["row_counts"],
            })
        else:
            cache.close()
            return jsonify({"error": "Restore failed — check logs for details"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Error handlers
# ---------------------------------------------------------------------------

@app.errorhandler(404)
def not_found(_e):
    return render_template("error.html", code=404, message="Page not found"), 404


@app.errorhandler(500)
def server_error(_e):
    return render_template("error.html", code=500, message="Internal server error"), 500


# ---------------------------------------------------------------------------
# Run (development only — production uses Gunicorn)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)