"""FastAPI app: button panel for pipeline / reconcile / trim."""

from __future__ import annotations

import csv
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from slskd_config import read_slskd_base_url, reset_local_config_cache
from webapp import runner

STATIC_DIR = Path(__file__).resolve().parent / "static"


class RunRequest(BaseModel):
    dry_run: bool = Field(default=False, description="Pass --dry-run where supported")


def create_app() -> FastAPI:
    app = FastAPI(title="slskd-csvReader", version="0.1.0")

    @app.get("/api/health")
    def health() -> Dict[str, Any]:
        return {"status": "ok", "service": "csvreader"}

    @app.get("/api/status")
    def status() -> Dict[str, Any]:
        reset_local_config_cache()
        ws = runner.workspace_path()
        queue = ws / "to_queue.csv"
        ledger = ws / "success_ledger.csv"
        saved = ws / "saved_playlists.json"
        active = runner.get_active()
        recent = [r.to_public() for r in runner.list_recent()[:5]]
        return {
            "workspace": str(ws),
            "slskd_base_url": read_slskd_base_url(),
            "queue_rows": _csv_data_rows(queue),
            "ledger_rows": _csv_data_rows(ledger),
            "saved_playlists": _saved_summary(saved),
            "active_run": active.to_public(include_log_tail=True) if active else None,
            "recent_runs": recent,
            "downloads_note": (
                "Audio files land in SLSKD's configured download directory "
                "(NAS: /volume1/Media/downloads/complete/slskd), not in this workspace."
            ),
        }

    @app.post("/api/runs/pipeline")
    def run_pipeline(body: RunRequest = RunRequest()) -> Dict[str, Any]:
        return _start("pipeline", runner.build_pipeline_command(dry_run=body.dry_run))

    @app.post("/api/runs/reconcile")
    def run_reconcile() -> Dict[str, Any]:
        return _start("reconcile", runner.build_reconcile_command())

    @app.post("/api/runs/trim")
    def run_trim(body: RunRequest = RunRequest()) -> Dict[str, Any]:
        return _start("trim", runner.build_trim_command(dry_run=body.dry_run))

    @app.get("/api/runs/{run_id}")
    def get_run(run_id: str) -> Dict[str, Any]:
        rec = runner.get_run(run_id)
        if not rec:
            raise HTTPException(status_code=404, detail="run not found")
        return rec.to_public(include_log_tail=True)

    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    if STATIC_DIR.is_dir():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    return app


def _start(action: str, command: list[str]) -> Dict[str, Any]:
    try:
        rec = runner.start_run(action, command)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return rec.to_public()


def _csv_data_rows(path: Path) -> Optional[int]:
    if not path.is_file():
        return None
    try:
        with path.open(newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            rows = list(reader)
        if not rows:
            return 0
        # header + data
        return max(0, len(rows) - 1)
    except OSError:
        return None


def _saved_summary(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        return {"count": 0, "enabled": 0, "names": []}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"count": 0, "enabled": 0, "names": [], "error": "unreadable"}
    playlists = data.get("playlists") if isinstance(data, dict) else data
    if not isinstance(playlists, list):
        return {"count": 0, "enabled": 0, "names": []}
    names = []
    enabled = 0
    for item in playlists:
        if not isinstance(item, dict):
            continue
        name = item.get("name") or item.get("id") or "?"
        names.append(str(name))
        if item.get("enabled", True):
            enabled += 1
    return {"count": len(names), "enabled": enabled, "names": names[:20]}


app = create_app()
