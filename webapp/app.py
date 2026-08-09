"""FastAPI app: operator control panel for pipeline / Spotify / ops."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from slskd_config import CHECKPOINT_BASENAME, read_slskd_base_url, reset_local_config_cache
from slskd_saved_playlists import (
    apply_enabled_updates,
    list_saved_playlists_public,
    load_saved_playlists,
    save_saved_playlists,
    upsert_library_selection,
)
from webapp import runner
from webapp import spotify_ops

STATIC_DIR = Path(__file__).resolve().parent / "static"


class RunRequest(BaseModel):
    dry_run: bool = Field(default=False, description="Pass --dry-run where supported")
    no_backup: bool = False


class PlaylistEnabledUpdate(BaseModel):
    id: str
    enabled: bool


class LibraryPlaylistRef(BaseModel):
    id: str
    name: str = ""


class PlaylistsUpdateRequest(BaseModel):
    updates: List[PlaylistEnabledUpdate] = Field(default_factory=list)
    selected: List[LibraryPlaylistRef] = Field(default_factory=list)
    replace_enabled_set: bool = False


class PipelineRunRequest(BaseModel):
    selection: str = Field(
        default="saved",
        description="saved | saved_indices | pick | playlist_id | resume | slskd_only",
    )
    saved_indices: str = ""
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


class ReconcileRequest(BaseModel):
    reconcile_from_csv: str = ""
    reconcile_log: str = ""


class MergeRequest(BaseModel):
    dry_run: bool = False
    force_full_import: bool = False
    date: str = ""
    spotify_export: str = ""


class CleanupRequest(BaseModel):
    validate_only: bool = False
    ephemeral: bool = False


class SlskdRunRequest(BaseModel):
    resume: bool = False
    retry_failed: bool = False
    gen_report: bool = False
    batch_size: Optional[int] = None
    delay: Optional[float] = None
    formats: str = ""
    exclude: str = ""
    queue_limit: Optional[int] = None
    download_settle_seconds: Optional[float] = None
    debug: bool = False
    skip_pending_csv: bool = False
    no_trim_queue: bool = False
    csv: str = ""
    checkpoint_file: str = ""
    pending_csv: str = ""


def create_app() -> FastAPI:
    app = FastAPI(title="slskd-csvReader", version="0.2.0")

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
        ckpt = ws / CHECKPOINT_BASENAME
        active = runner.get_active()
        recent = [r.to_public() for r in runner.list_recent()[:5]]
        token = spotify_ops.token_status()
        return {
            "workspace": str(ws),
            "slskd_base_url": read_slskd_base_url(),
            "queue_rows": _csv_data_rows(queue),
            "ledger_rows": _csv_data_rows(ledger),
            "saved_playlists": _saved_summary(saved),
            "checkpoint": {
                "path": str(ckpt),
                "exists": ckpt.is_file(),
                "size": ckpt.stat().st_size if ckpt.is_file() else 0,
            },
            "spotify_token": token,
            "active_run": active.to_public(include_log_tail=True) if active else None,
            "recent_runs": recent,
            "downloads_note": (
                "Audio files land in SLSKD's configured download directory "
                "(NAS: /volume1/Media/downloads/complete/slskd), not in this workspace."
            ),
        }

    @app.get("/api/spotify/token-status")
    def spotify_token_status() -> Dict[str, Any]:
        reset_local_config_cache()
        return spotify_ops.token_status()

    @app.get("/api/spotify/library")
    def spotify_library(
        max_playlists: Optional[int] = Query(default=None, ge=1, le=500),
    ) -> Dict[str, Any]:
        reset_local_config_cache()
        try:
            playlists = spotify_ops.fetch_library(max_playlists=max_playlists)
        except RuntimeError as exc:
            raise HTTPException(status_code=401, detail=str(exc)) from exc
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=502, detail=f"Spotify library fetch failed: {exc}") from exc
        return {"playlists": playlists}

    @app.get("/api/playlists")
    def list_playlists() -> Dict[str, Any]:
        reset_local_config_cache()
        state = load_saved_playlists(runner.workspace_path())
        return {"playlists": list_saved_playlists_public(state)}

    @app.post("/api/playlists")
    def update_playlists(body: PlaylistsUpdateRequest) -> Dict[str, Any]:
        ws = runner.workspace_path()
        state = load_saved_playlists(ws)
        if body.selected:
            state = upsert_library_selection(
                state,
                [{"id": s.id, "name": s.name} for s in body.selected],
                replace_enabled_set=body.replace_enabled_set,
            )
        if body.updates:
            updates = {u.id: u.enabled for u in body.updates}
            state = apply_enabled_updates(state, updates)
        save_saved_playlists(ws, state)
        return {"playlists": list_saved_playlists_public(state)}

    @app.post("/api/runs/pipeline")
    def run_pipeline(body: PipelineRunRequest = PipelineRunRequest()) -> Dict[str, Any]:
        opts = runner.PipelineOptions(
            selection=body.selection,
            saved_indices=body.saved_indices,
            pick=body.pick,
            playlist_id=body.playlist_id,
            dry_run=body.dry_run,
            skip_slskd=body.skip_slskd,
            force_full_import=body.force_full_import,
            continue_on_export_error=body.continue_on_export_error,
            no_save_picks=body.no_save_picks,
            date=body.date,
            download_settle_seconds=body.download_settle_seconds,
            skip_pending_csv=body.skip_pending_csv,
            no_trim_queue=body.no_trim_queue,
            csv=body.csv,
            checkpoint_file=body.checkpoint_file,
        )
        try:
            cmd = runner.build_pipeline_command(options=opts)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        action = "pipeline"
        if body.selection == "resume":
            action = "resume"
        elif body.selection == "slskd_only":
            action = "slskd_only"
        elif body.dry_run:
            action = "pipeline-dry"
        return _start(action, cmd)

    @app.post("/api/runs/reconcile")
    def run_reconcile(body: ReconcileRequest = ReconcileRequest()) -> Dict[str, Any]:
        return _start(
            "reconcile",
            runner.build_reconcile_command(
                reconcile_from_csv=body.reconcile_from_csv,
                reconcile_log=body.reconcile_log,
            ),
        )

    @app.post("/api/runs/trim")
    def run_trim(body: RunRequest = RunRequest()) -> Dict[str, Any]:
        return _start(
            "trim",
            runner.build_trim_command(dry_run=body.dry_run, no_backup=body.no_backup),
        )

    @app.post("/api/runs/merge")
    def run_merge(body: MergeRequest = MergeRequest()) -> Dict[str, Any]:
        return _start(
            "merge",
            runner.build_merge_command(
                dry_run=body.dry_run,
                force_full_import=body.force_full_import,
                date=body.date,
                spotify_export=body.spotify_export,
            ),
        )

    @app.post("/api/runs/cleanup")
    def run_cleanup(body: CleanupRequest = CleanupRequest()) -> Dict[str, Any]:
        return _start(
            "cleanup",
            runner.build_cleanup_command(
                validate_only=body.validate_only,
                ephemeral=body.ephemeral,
            ),
        )

    @app.post("/api/runs/slskd")
    def run_slskd(body: SlskdRunRequest = SlskdRunRequest()) -> Dict[str, Any]:
        modes = sum(bool(x) for x in (body.resume, body.retry_failed, body.gen_report))
        if modes > 1:
            raise HTTPException(
                status_code=400,
                detail="Use only one of resume, retry_failed, or gen_report",
            )
        tuning = runner.SlskdTuning(
            batch_size=body.batch_size,
            delay=body.delay,
            formats=body.formats,
            exclude=body.exclude,
            queue_limit=body.queue_limit,
            download_settle_seconds=body.download_settle_seconds,
            debug=body.debug,
            skip_pending_csv=body.skip_pending_csv,
            no_trim_queue=body.no_trim_queue,
            csv=body.csv,
            checkpoint_file=body.checkpoint_file,
            pending_csv=body.pending_csv,
        )
        if body.resume:
            action = "resume"
        elif body.retry_failed:
            action = "retry_failed"
        elif body.gen_report:
            action = "gen_report"
        else:
            action = "slskd_only"
        return _start(
            action,
            runner.build_slskd_command(
                resume=body.resume,
                retry_failed=body.retry_failed,
                gen_report=body.gen_report,
                tuning=tuning,
            ),
        )

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
