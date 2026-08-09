"""Background job runner for web UI actions (single-flight lock)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
PYTHON = sys.executable

_LOCK = threading.Lock()
_ACTIVE: Optional["RunRecord"] = None
_HISTORY: List["RunRecord"] = []
_HISTORY_MAX = 20


@dataclass
class RunRecord:
    id: str
    action: str
    command: List[str]
    started_at: float
    finished_at: Optional[float] = None
    returncode: Optional[int] = None
    log_path: str = ""
    status: str = "running"  # running | succeeded | failed
    error: str = ""

    def to_public(self, *, include_log_tail: bool = False, tail_bytes: int = 8000) -> Dict[str, Any]:
        data = asdict(self)
        if include_log_tail and self.log_path:
            data["log_tail"] = _tail_file(Path(self.log_path), tail_bytes)
        else:
            data["log_tail"] = ""
        return data


def workspace_path() -> Path:
    env = os.environ.get("CSVREADER_WORKSPACE", "").strip()
    if env:
        return Path(env).resolve()
    return (REPO_ROOT / "data").resolve()


def runs_dir() -> Path:
    d = workspace_path() / "logs" / "ui-runs"
    d.mkdir(parents=True, exist_ok=True)
    return d


def checkpoint_path() -> Path:
    return workspace_path() / "checkpoint.json"


def token_cache_for_cli() -> Optional[Path]:
    env = os.environ.get("SPOTIFY_TOKEN_CACHE", "").strip()
    if env:
        return Path(env).expanduser().resolve()
    candidate = workspace_path() / "spotify_tokens.json"
    if candidate.is_file():
        return candidate.resolve()
    return None


def _tail_file(path: Path, nbytes: int) -> str:
    try:
        raw = path.read_bytes()
    except OSError:
        return ""
    if len(raw) > nbytes:
        raw = raw[-nbytes:]
    return raw.decode("utf-8", errors="replace")


def get_active() -> Optional[RunRecord]:
    with _LOCK:
        return _ACTIVE


def get_run(run_id: str) -> Optional[RunRecord]:
    with _LOCK:
        if _ACTIVE and _ACTIVE.id == run_id:
            return _ACTIVE
        for rec in _HISTORY:
            if rec.id == run_id:
                return rec
    return None


def list_recent() -> List[RunRecord]:
    with _LOCK:
        out: List[RunRecord] = []
        if _ACTIVE:
            out.append(_ACTIVE)
        out.extend(_HISTORY)
        return out


def start_run(action: str, command: List[str]) -> RunRecord:
    """Start a subprocess; raises RuntimeError if another run is active."""
    global _ACTIVE
    with _LOCK:
        if _ACTIVE is not None and _ACTIVE.status == "running":
            raise RuntimeError("another run is already in progress")
        run_id = uuid.uuid4().hex[:12]
        log_path = runs_dir() / f"{run_id}-{action}.log"
        rec = RunRecord(
            id=run_id,
            action=action,
            command=command,
            started_at=time.time(),
            log_path=str(log_path),
        )
        _ACTIVE = rec

    thread = threading.Thread(
        target=_execute,
        args=(rec, command, log_path),
        name=f"csvreader-ui-{action}",
        daemon=True,
    )
    thread.start()
    return rec


def _execute(rec: RunRecord, command: List[str], log_path: Path) -> None:
    global _ACTIVE
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    ws = str(workspace_path())
    env["CSVREADER_WORKSPACE"] = ws
    ws_ini = Path(ws) / "config.ini"
    if ws_ini.is_file():
        env["CSVREADER_CONFIG_FILE"] = str(ws_ini)

    try:
        with log_path.open("w", encoding="utf-8") as logf:
            logf.write(f"$ {' '.join(command)}\n\n")
            logf.flush()
            proc = subprocess.Popen(
                command,
                cwd=str(REPO_ROOT),
                stdout=logf,
                stderr=subprocess.STDOUT,
                env=env,
                text=True,
            )
            rec.returncode = proc.wait()
    except Exception as exc:  # noqa: BLE001 — surface to UI
        rec.error = str(exc)
        rec.returncode = -1
        try:
            with log_path.open("a", encoding="utf-8") as logf:
                logf.write(f"\n[webapp runner error] {exc}\n")
        except OSError:
            pass

    rec.finished_at = time.time()
    rec.status = "succeeded" if rec.returncode == 0 else "failed"

    with _LOCK:
        if _ACTIVE is rec:
            _HISTORY.insert(0, rec)
            del _HISTORY[_HISTORY_MAX:]
            _ACTIVE = None
        try:
            summary = runs_dir() / "last_run.json"
            summary.write_text(
                json.dumps(rec.to_public(include_log_tail=False), indent=2),
                encoding="utf-8",
            )
        except OSError:
            pass


def _append_token_cache(cmd: List[str]) -> None:
    token = token_cache_for_cli()
    if token is not None:
        cmd.extend(["--token-cache", str(token)])


def _csv_indices(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    s = str(raw).strip()
    return s or None


@dataclass
class PipelineOptions:
    """Maps to run_pipeline.py flags (always non-interactive via -y)."""

    # Selection: exactly one mode. Default = all enabled saved.
    selection: str = "saved"  # saved | saved_indices | pick | playlist_id | resume | slskd_only
    saved_indices: str = ""  # "1,3" for --saved 1,3
    pick: str = ""
    playlist_id: str = ""

    dry_run: bool = False
    skip_slskd: bool = False
    force_full_import: bool = False
    continue_on_export_error: bool = False
    no_save_picks: bool = False
    date: str = ""
    download_settle_seconds: Optional[float] = None
    skip_pending_csv: bool = False
    no_trim_queue: bool = False
    csv: str = ""
    checkpoint_file: str = ""


def build_pipeline_command(
    *,
    dry_run: bool = False,
    options: Optional[PipelineOptions] = None,
) -> List[str]:
    """Build run_pipeline.py argv. *dry_run* kept for backward-compatible callers."""
    opts = options or PipelineOptions(dry_run=dry_run)
    if options is None and dry_run:
        opts.dry_run = True

    ws = workspace_path()
    cmd: List[str] = [
        PYTHON,
        str(REPO_ROOT / "run_pipeline.py"),
        "-y",
        "--workspace",
        str(ws),
    ]
    _append_token_cache(cmd)

    sel = (opts.selection or "saved").strip()
    if sel == "resume":
        cmd.append("--resume")
    elif sel == "slskd_only":
        cmd.append("--slskd-only")
    elif sel == "pick":
        pick = _csv_indices(opts.pick)
        if not pick:
            raise ValueError("selection=pick requires pick indices (e.g. 1,4)")
        cmd.extend(["--pick", pick])
    elif sel == "playlist_id":
        pid = (opts.playlist_id or "").strip()
        if not pid:
            raise ValueError("selection=playlist_id requires playlist_id")
        cmd.extend(["--playlist-id", pid])
    elif sel == "saved_indices":
        idxs = _csv_indices(opts.saved_indices)
        if not idxs:
            raise ValueError("selection=saved_indices requires saved_indices")
        cmd.extend(["--saved", idxs])
    else:
        # all enabled saved
        cmd.append("--saved")

    if opts.dry_run:
        cmd.append("--dry-run")
    if opts.skip_slskd:
        cmd.append("--skip-slskd")
    if opts.force_full_import:
        cmd.append("--force-full-import")
    if opts.continue_on_export_error:
        cmd.append("--continue-on-export-error")
    if opts.no_save_picks:
        cmd.append("--no-save-picks")
    if (opts.date or "").strip():
        cmd.extend(["--date", opts.date.strip()])
    if opts.download_settle_seconds is not None:
        cmd.extend(["--download-settle-seconds", str(opts.download_settle_seconds)])
    if opts.skip_pending_csv:
        cmd.append("--skip-pending-csv")
    if opts.no_trim_queue:
        cmd.append("--no-trim-queue")
    if (opts.csv or "").strip():
        cmd.extend(["--csv", opts.csv.strip()])
    if (opts.checkpoint_file or "").strip():
        cmd.extend(["--checkpoint-file", opts.checkpoint_file.strip()])

    # Headless: never open a browser from the container
    cmd.append("--no-browser")
    return cmd


@dataclass
class SlskdTuning:
    batch_size: Optional[int] = None
    delay: Optional[float] = None
    formats: str = ""  # space-separated for nargs+
    exclude: str = ""
    queue_limit: Optional[int] = None
    download_settle_seconds: Optional[float] = None
    debug: bool = False
    skip_pending_csv: bool = False
    no_trim_queue: bool = False
    csv: str = ""
    checkpoint_file: str = ""
    pending_csv: str = ""


def build_reconcile_command(
    *,
    reconcile_from_csv: str = "",
    reconcile_log: str = "",
) -> List[str]:
    ws = workspace_path()
    cmd = [
        PYTHON,
        str(REPO_ROOT / "slskd_spotify.py"),
        "--reconcile-downloads",
        "--csv",
        str(ws / "to_queue.csv"),
        "--output-dir",
        str(ws / "logs"),
    ]
    if (reconcile_from_csv or "").strip():
        cmd.extend(["--reconcile-from-csv", reconcile_from_csv.strip()])
    if (reconcile_log or "").strip():
        cmd.extend(["--reconcile-log", reconcile_log.strip()])
    return cmd


def build_trim_command(*, dry_run: bool = False, no_backup: bool = False) -> List[str]:
    cmd = [
        PYTHON,
        str(REPO_ROOT / "trim_queue.py"),
        "--workspace",
        str(workspace_path()),
    ]
    if dry_run:
        cmd.append("--dry-run")
    if no_backup:
        cmd.append("--no-backup")
    return cmd


def build_merge_command(
    *,
    dry_run: bool = False,
    force_full_import: bool = False,
    date: str = "",
    spotify_export: str = "",
) -> List[str]:
    cmd = [
        PYTHON,
        str(REPO_ROOT / "merge_queue.py"),
        "--workspace",
        str(workspace_path()),
    ]
    if dry_run:
        cmd.append("--dry-run")
    if force_full_import:
        cmd.append("--force-full-import")
    if (date or "").strip():
        cmd.extend(["--date", date.strip()])
    if (spotify_export or "").strip():
        cmd.extend(["--spotify-export", spotify_export.strip()])
    return cmd


def build_cleanup_command(*, validate_only: bool = False, ephemeral: bool = False) -> List[str]:
    cmd = [
        PYTHON,
        str(REPO_ROOT / "pipeline_cleanup.py"),
        "--workspace",
        str(workspace_path()),
    ]
    if validate_only:
        cmd.append("--validate-only")
    elif ephemeral:
        cmd.append("--ephemeral")
    return cmd


def build_slskd_command(
    *,
    resume: bool = False,
    retry_failed: bool = False,
    gen_report: bool = False,
    tuning: Optional[SlskdTuning] = None,
) -> List[str]:
    ws = workspace_path()
    tuning = tuning or SlskdTuning()
    csv_path = (tuning.csv or "").strip() or str(ws / "to_queue.csv")
    cmd = [
        PYTHON,
        str(REPO_ROOT / "slskd_spotify.py"),
        "--csv",
        csv_path,
        "--output-dir",
        str(ws / "logs"),
    ]
    if resume:
        cmd.append("--resume")
    if retry_failed:
        cmd.append("--retry-failed")
    if gen_report:
        cmd.append("--gen-report")
    ckpt = (tuning.checkpoint_file or "").strip() or str(ws / "checkpoint.json")
    cmd.extend(["--checkpoint-file", ckpt])
    if tuning.batch_size is not None:
        cmd.extend(["--batch-size", str(tuning.batch_size)])
    if tuning.delay is not None:
        cmd.extend(["--delay", str(tuning.delay)])
    if (tuning.formats or "").strip():
        cmd.append("--formats")
        cmd.extend(tuning.formats.split())
    if (tuning.exclude or "").strip():
        cmd.append("--exclude")
        cmd.extend(tuning.exclude.split())
    if tuning.queue_limit is not None:
        cmd.extend(["--queue-limit", str(tuning.queue_limit)])
    if tuning.download_settle_seconds is not None:
        cmd.extend(["--download-settle-seconds", str(tuning.download_settle_seconds)])
    if tuning.debug:
        cmd.append("--debug")
    if tuning.skip_pending_csv:
        cmd.append("--skip-pending-csv")
    if tuning.no_trim_queue:
        cmd.append("--no-trim-queue")
    if (tuning.pending_csv or "").strip():
        cmd.extend(["--pending-csv", tuning.pending_csv.strip()])
    return cmd


# Back-compat alias used by older tests
def build_pipeline_command_legacy(*, dry_run: bool = False) -> List[str]:
    return build_pipeline_command(dry_run=dry_run)
