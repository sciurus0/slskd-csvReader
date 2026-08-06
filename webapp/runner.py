"""Background job runner for web UI actions (single-flight lock)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
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
    # Point local config discovery at workspace secrets when present.
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
        # Persist last run summary for status endpoint after restart
        try:
            summary = runs_dir() / "last_run.json"
            summary.write_text(
                json.dumps(rec.to_public(include_log_tail=False), indent=2),
                encoding="utf-8",
            )
        except OSError:
            pass


def build_pipeline_command(*, dry_run: bool = False) -> List[str]:
    ws = workspace_path()
    cmd = [
        PYTHON,
        str(REPO_ROOT / "run_pipeline.py"),
        "--saved",
        "-y",
        "--workspace",
        str(ws),
    ]
    token_env = os.environ.get("SPOTIFY_TOKEN_CACHE", "").strip()
    token_path = Path(token_env) if token_env else (ws / "spotify_tokens.json")
    if token_path.is_file() or token_env:
        cmd.extend(["--token-cache", str(token_path)])
    if dry_run:
        cmd.append("--dry-run")
    return cmd


def build_reconcile_command() -> List[str]:
    return [
        PYTHON,
        str(REPO_ROOT / "slskd_spotify.py"),
        "--reconcile-downloads",
        "--csv",
        str(workspace_path() / "to_queue.csv"),
        "--output-dir",
        str(workspace_path() / "logs"),
    ]


def build_trim_command(*, dry_run: bool = False) -> List[str]:
    cmd = [
        PYTHON,
        str(REPO_ROOT / "trim_queue.py"),
        "--workspace",
        str(workspace_path()),
    ]
    if dry_run:
        cmd.append("--dry-run")
    return cmd
