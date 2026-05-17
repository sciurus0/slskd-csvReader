#!/usr/bin/env python3
"""
Interactive end-to-end pipeline: Spotify playlist pick → export → merge_queue → slskd-spotify.

Run from the repo workspace (DEV or PROD) so paths resolve to to_queue.csv and dated exports.

Examples:
    python3 run_pipeline.py
    python3 run_pipeline.py --workspace .
    python3 run_pipeline.py --pick 1,4,7 --yes
    python3 run_pipeline.py --dry-run
    python3 run_pipeline.py --skip-slskd
    python3 run_pipeline.py --resume -y
    python3 run_pipeline.py --slskd-only --csv to_queue_pending.csv -y
    python3 run_pipeline.py --pick 1,4,7 --continue-on-export-error -y
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from slskd_config import CHECKPOINT_FILE, load_api_txt
from slskd_csv import checkpoint_resume_row, load_checkpoint
from slskd_export_paths import default_new_export_path, parse_export_date

from spotify_playlist_fetch import (
    DEFAULT_REDIRECT_URI,
    DEFAULT_THROTTLE_CACHE,
    DEFAULT_TOKEN_CACHE,
    _enforce_throttle_cooldown,
    ensure_user_access_token,
    fetch_playlist_track_rows,
    fetch_user_playlists,
    print_user_playlists,
    write_csv,
)


def _die(msg: str, code: int = 1) -> None:
    print(msg, file=sys.stderr)
    raise SystemExit(code)


def _repo_root() -> Path:
    return Path(__file__).resolve().parent


class StageAction(str, Enum):
    RUN = "run"
    SKIP = "skip"
    PREVIEW = "preview"


@dataclass(frozen=True)
class PipelineStagePlan:
    key: str
    title: str
    action: StageAction
    writes: tuple[str, ...] = ()


def build_pipeline_plan(
    *,
    slskd_only: bool,
    resume: bool,
    dry_run: bool,
    skip_slskd: bool,
    export_path: Path,
    queue_path: Path,
    force_full_import: bool,
) -> List[PipelineStagePlan]:
    """Describe which pipeline stages run and what they may write (RUN-03)."""
    if slskd_only:
        slskd_writes: tuple[str, ...] = (
            "logs/",
            "checkpoint.pkl",
            "success_ledger.csv (on downloads)",
            "to_queue_pending.csv",
        )
        if resume:
            slskd_writes += ("to_queue.csv (trim/reconcile may update)",)
        return [
            PipelineStagePlan("spotify", "Spotify export", StageAction.SKIP),
            PipelineStagePlan("merge", "merge_queue", StageAction.SKIP),
            PipelineStagePlan(
                "slskd",
                "slskd-spotify"
                + (" (resume)" if resume else ""),
                StageAction.RUN,
                slskd_writes,
            ),
        ]

    merge_writes = () if dry_run else (
        f"dated backup of {queue_path.name}",
        str(queue_path),
        "merge_state.json",
    )
    merge_action = StageAction.PREVIEW if dry_run else StageAction.RUN
    slskd_action = (
        StageAction.SKIP
        if dry_run or skip_slskd
        else StageAction.RUN
    )
    slskd_writes_full = (
        ()
        if slskd_action is StageAction.SKIP
        else (
            "logs/",
            "checkpoint.pkl",
            "success_ledger.csv",
            "to_queue_pending.csv",
            "to_queue.csv (if --trim-queue)",
        )
    )
    return [
        PipelineStagePlan(
            "spotify",
            "Spotify export",
            StageAction.RUN,
            (str(export_path),),
        ),
        PipelineStagePlan(
            "merge",
            "merge_queue",
            merge_action,
            merge_writes,
        ),
        PipelineStagePlan(
            "slskd",
            "slskd-spotify",
            slskd_action,
            slskd_writes_full,
        ),
    ]


def format_pipeline_plan(
    *,
    workspace: Path,
    stages: List[PipelineStagePlan],
    dry_run: bool,
    force_full_import: bool,
    continue_on_export_error: bool = False,
) -> List[str]:
    lines = [
        "Pipeline plan",
        f"  workspace: {workspace}",
    ]
    if dry_run:
        lines.append(
            "  mode: dry-run (export is written; merge previews counts only)"
        )
    if continue_on_export_error:
        lines.append(
            "  export: --continue-on-export-error (skip failed playlists)"
        )
    if force_full_import:
        lines.append("  merge: --force-full-import (ignore added_at watermarks)")
    for stage in stages:
        if stage.action is StageAction.SKIP:
            action = "skip"
        elif stage.action is StageAction.PREVIEW:
            action = "preview (no writes)"
        else:
            action = "run"
        line = f"  [{stage.key}] {stage.title}: {action}"
        if stage.writes:
            line += f" → writes {', '.join(stage.writes)}"
        lines.append(line)
    return lines


def _print_pipeline_plan(lines: List[str]) -> None:
    print("\n".join(lines), file=sys.stderr)


def _print_pipeline_summary(
    *,
    stage_seconds: List[tuple[str, float]],
    stopped_early: Optional[str] = None,
) -> None:
    total = sum(s for _, s in stage_seconds)
    print("\nPipeline summary", file=sys.stderr)
    for name, secs in stage_seconds:
        print(f"  {name}: {secs:.1f}s", file=sys.stderr)
    print(f"  total: {total:.1f}s", file=sys.stderr)
    if stopped_early:
        print(f"  stopped: {stopped_early}", file=sys.stderr)


def parse_pick_input(raw: str) -> List[int]:
    """Parse comma-separated 1-based playlist indices (interactive or --pick)."""
    s = (raw or "").strip()
    if not s:
        raise ValueError("Enter at least one playlist number.")
    parts = [p.strip() for p in s.split(",") if p.strip()]
    if not parts:
        raise ValueError("Enter at least one playlist number.")
    indices: List[int] = []
    for part in parts:
        try:
            n = int(part)
        except ValueError:
            raise ValueError(f"Invalid index {part!r} (use integers, e.g. 1 or 1,4,7).") from None
        if n < 1:
            raise ValueError(f"Index must be >= 1, got {n}.")
        indices.append(n)
    return list(dict.fromkeys(indices))


def prompt_playlist_picks(max_index: int) -> List[int]:
    """Prompt until valid comma-separated indices are entered."""
    while True:
        try:
            raw = input(
                "\nEnter playlist numbers to export (comma-separated, e.g. 1,4,7): "
            ).strip()
            indices = parse_pick_input(raw)
        except ValueError as e:
            print(f"  {e}", file=sys.stderr)
            continue
        bad = [i for i in indices if i > max_index]
        if bad:
            print(
                f"  Out of range: {bad} (you have {max_index} playlist(s) listed).",
                file=sys.stderr,
            )
            continue
        return indices


@dataclass(frozen=True)
class ExportResult:
    row_count: int
    failures: Tuple[str, ...] = ()


def _summarize_export_failures(failures: Tuple[str, ...]) -> None:
    if not failures:
        return
    print(f"\nExport skipped {len(failures)} playlist(s):", file=sys.stderr)
    for msg in failures:
        print(f"  - {msg}", file=sys.stderr)


def export_playlists(
    access_token: str,
    playlists: List[Dict[str, Any]],
    pick_indices: List[int],
    output_path: Path,
    *,
    continue_on_error: bool = False,
) -> ExportResult:
    """Export selected playlists into one combined CSV (RUN-04)."""
    max_index = max(pick_indices)
    if max_index > len(playlists):
        _die(f"Pick {max_index} is out of range (you have {len(playlists)} playlists).")

    all_rows: List[Dict[str, str]] = []
    failures: List[str] = []
    for idx in pick_indices:
        pl = playlists[idx - 1]
        playlist_id = (pl.get("id") or "").strip()
        label = pl.get("name") or playlist_id or f"#{idx}"
        if not playlist_id:
            msg = f"#{idx} ({label}): no playlist id"
            if continue_on_error:
                failures.append(msg)
                print(f"  Skipped export: {msg}", file=sys.stderr)
                continue
            _die(f"Playlist #{idx} has no id; cannot export.")
        try:
            rows = fetch_playlist_track_rows(access_token, playlist_id)
        except Exception as e:
            if continue_on_error:
                msg = f"#{idx} ({label}): {e}"
                failures.append(msg)
                print(f"  Skipped export: {msg}", file=sys.stderr)
                continue
            _die(f"Export failed for playlist #{idx} ({label}): {e}")
        print(f"  Playlist #{idx} ({label}): {len(rows)} rows", file=sys.stderr)
        all_rows.extend(rows)

    if failures:
        _summarize_export_failures(tuple(failures))

    if not all_rows:
        if failures and len(failures) == len(pick_indices):
            _die(
                "All selected playlist(s) failed to export; "
                "fix errors or retry without --continue-on-export-error."
            )
        print(
            "Warning: no tracks exported (empty playlist(s) or no readable tracks).",
            file=sys.stderr,
        )
    write_csv(str(output_path), all_rows)
    return ExportResult(len(all_rows), tuple(failures))


def _run_python_step(
    script: str,
    args: List[str],
    *,
    workspace: Path,
    label: str,
) -> float:
    cmd = [sys.executable, str(_repo_root() / script), *args]
    print(f"\n=== {label} (start) ===", file=sys.stderr)
    print("  " + " ".join(cmd), file=sys.stderr)
    t0 = time.monotonic()
    result = subprocess.run(cmd, cwd=str(workspace))
    elapsed = time.monotonic() - t0
    if result.returncode != 0:
        _die(f"{label} failed (exit {result.returncode}, {elapsed:.1f}s).")
    print(
        f"=== {label} (done, {elapsed:.1f}s, exit 0) ===",
        file=sys.stderr,
    )
    return elapsed


def _checkpoint_path(workspace: Path, explicit: Optional[Path]) -> Path:
    return (explicit or workspace / CHECKPOINT_FILE).resolve()


def _print_resume_hint(checkpoint_path: Path) -> None:
    """Log whether a slskd checkpoint exists before resume (RUN-02)."""
    if not checkpoint_path.is_file():
        print(
            f"No checkpoint at {checkpoint_path}; slskd-spotify will start from row 1.",
            file=sys.stderr,
        )
        return
    checkpoint = load_checkpoint(str(checkpoint_path))
    if not checkpoint:
        print(f"Could not read checkpoint {checkpoint_path}.", file=sys.stderr)
        return
    next_row = checkpoint_resume_row(checkpoint)
    total = checkpoint.get("total_rows", "?")
    print(
        f"Checkpoint: resume at row {next_row + 1} of {total} "
        f"(0-based next index {next_row}).",
        file=sys.stderr,
    )


def build_slskd_argv(
    *,
    workspace: Path,
    queue_path: Path,
    resume: bool,
    checkpoint_path: Path,
    download_settle_seconds: Optional[float],
    skip_download_reconcile: bool,
    skip_pending_csv: bool,
    trim_queue: bool,
) -> List[str]:
    args = [
        "--csv",
        str(queue_path),
        "--output-dir",
        str(workspace / "logs"),
        "--checkpoint-file",
        str(checkpoint_path),
    ]
    if resume:
        args.append("--resume")
    if download_settle_seconds is not None:
        args.extend(["--download-settle-seconds", str(download_settle_seconds)])
    if skip_download_reconcile:
        args.append("--skip-download-reconcile")
    if skip_pending_csv:
        args.append("--skip-pending-csv")
    if trim_queue:
        args.append("--trim-queue")
    return args


def confirm_slskd(*, queue_path: Path, assume_yes: bool) -> bool:
    if assume_yes:
        return True
    try:
        answer = input(
            f"\nStart SLSKD queue processing for {queue_path.name}? [y/N]: "
        ).strip().lower()
    except EOFError:
        return False
    return answer in ("y", "yes")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Interactive Spotify → merge_queue → slskd-spotify pipeline."
    )
    parser.add_argument(
        "--workspace",
        type=Path,
        default=Path.cwd(),
        help="Directory for export, queue, and logs (default: current directory)",
    )
    parser.add_argument(
        "--pick",
        metavar="N[,N...]",
        default=None,
        help="Skip interactive prompt; export these 1-based playlist indices",
    )
    parser.add_argument(
        "--date",
        metavar="YYYYMMDD",
        default=None,
        help="Date prefix for new export file (default: today). merge_queue reads that file.",
    )
    parser.add_argument(
        "--force-full-import",
        action="store_true",
        help="Pass through to merge_queue: ignore added_at watermarks (ledger + dedupe still apply)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Write Spotify export, then merge_queue --dry-run (preview counts only; "
            "no queue backup, merge_state, or slskd)"
        ),
    )
    parser.add_argument(
        "--skip-slskd",
        action="store_true",
        help="Stop after merge_queue (do not invoke slskd-spotify.py)",
    )
    parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="Do not prompt before starting slskd-spotify.py",
    )
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="Print Spotify authorize URL instead of opening a browser",
    )
    parser.add_argument(
        "--token-cache",
        type=Path,
        default=None,
        help=f"Spotify token cache (default: {DEFAULT_TOKEN_CACHE})",
    )
    parser.add_argument(
        "--redirect-uri",
        default=None,
        help=f"Spotify OAuth redirect URI (default: env, api.txt, or {DEFAULT_REDIRECT_URI})",
    )
    parser.add_argument(
        "--download-settle-seconds",
        type=float,
        default=None,
        help="Passed to slskd-spotify.py (default: slskd default)",
    )
    parser.add_argument(
        "--skip-download-reconcile",
        action="store_true",
        help="Passed to slskd-spotify.py",
    )
    parser.add_argument(
        "--skip-pending-csv",
        action="store_true",
        help="Passed to slskd-spotify.py",
    )
    parser.add_argument(
        "--resume",
        "-r",
        action="store_true",
        help="Skip Spotify export and merge; run slskd-spotify.py --resume from checkpoint.pkl",
    )
    parser.add_argument(
        "--slskd-only",
        action="store_true",
        help="Skip Spotify export and merge; run slskd-spotify.py only",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=None,
        metavar="PATH",
        help="Queue CSV for slskd (default: <workspace>/to_queue.csv)",
    )
    parser.add_argument(
        "--checkpoint-file",
        type=Path,
        default=None,
        help=f"Checkpoint pickle for slskd resume (default: <workspace>/{CHECKPOINT_FILE})",
    )
    parser.add_argument(
        "--trim-queue",
        action="store_true",
        help="Passed to slskd-spotify.py after reconciliation",
    )
    parser.add_argument(
        "--continue-on-export-error",
        action="store_true",
        help=(
            "On multi-playlist export, skip playlists that fail and continue "
            "(default: abort the pipeline on the first failure)"
        ),
    )
    args = parser.parse_args()

    workspace = args.workspace.resolve()
    if not workspace.is_dir():
        _die(f"Workspace is not a directory: {workspace}")

    try:
        export_path, date_str = default_new_export_path(
            workspace, parse_export_date(args.date) if args.date else None
        )
    except ValueError as e:
        _die(f"--date {e}")
    queue_path = (args.csv or workspace / "to_queue.csv").resolve()
    checkpoint_path = _checkpoint_path(workspace, args.checkpoint_file)
    slskd_only = args.resume or args.slskd_only

    if args.resume and args.slskd_only:
        print("Note: --slskd-only is implied by --resume.", file=sys.stderr)

    stages = build_pipeline_plan(
        slskd_only=slskd_only,
        resume=args.resume,
        dry_run=args.dry_run,
        skip_slskd=args.skip_slskd,
        export_path=export_path,
        queue_path=queue_path,
        force_full_import=args.force_full_import,
    )
    _print_pipeline_plan(
        format_pipeline_plan(
            workspace=workspace,
            stages=stages,
            dry_run=args.dry_run,
            force_full_import=args.force_full_import,
            continue_on_export_error=args.continue_on_export_error,
        )
    )
    stage_seconds: List[tuple[str, float]] = []

    if slskd_only:
        if args.dry_run:
            _die("--dry-run cannot be used with --resume or --slskd-only.")
        if args.skip_slskd:
            _die("--skip-slskd conflicts with --resume / --slskd-only.")
        if not queue_path.is_file():
            _die(f"Queue CSV not found: {queue_path}")
        if args.resume:
            _print_resume_hint(checkpoint_path)
        if not confirm_slskd(queue_path=queue_path, assume_yes=args.yes):
            print("Skipped slskd-spotify.py.", file=sys.stderr)
            _print_pipeline_summary(
                stage_seconds=stage_seconds, stopped_early="user declined slskd"
            )
            return
        slskd_argv = build_slskd_argv(
            workspace=workspace,
            queue_path=queue_path,
            resume=args.resume,
            checkpoint_path=checkpoint_path,
            download_settle_seconds=args.download_settle_seconds,
            skip_download_reconcile=args.skip_download_reconcile,
            skip_pending_csv=args.skip_pending_csv,
            trim_queue=args.trim_queue,
        )
        stage_seconds.append(
            (
                "slskd-spotify",
                _run_python_step(
                    "slskd-spotify.py",
                    slskd_argv,
                    workspace=workspace,
                    label="slskd-spotify",
                ),
            )
        )
        _print_pipeline_summary(stage_seconds=stage_seconds)
        return

    file_secrets = load_api_txt()
    default_redirect = (
        (args.redirect_uri or "").strip()
        or os.environ.get("SPOTIFY_REDIRECT_URI", "").strip()
        or file_secrets.get("spotify_redirect_uri", "").strip()
        or DEFAULT_REDIRECT_URI
    )
    client_id = (
        os.environ.get("SPOTIFY_CLIENT_ID", "").strip()
        or file_secrets.get("spotify_client_id", "").strip()
    )
    client_secret = (
        os.environ.get("SPOTIFY_CLIENT_SECRET", "").strip()
        or file_secrets.get("spotify_client_secret", "").strip()
    ) or None
    if not client_id:
        _die(
            "Set SPOTIFY_CLIENT_ID or add client_id under [spotify] in api.txt.\n"
            f"Register redirect URI: {default_redirect}"
        )

    token_path = args.token_cache or DEFAULT_TOKEN_CACHE

    print("\n=== spotify_export (start) ===", file=sys.stderr)
    t_spotify = time.monotonic()
    access_token = ensure_user_access_token(
        client_id,
        client_secret,
        default_redirect,
        token_path,
        args.no_browser,
    )
    _enforce_throttle_cooldown(DEFAULT_THROTTLE_CACHE)

    print("\nYour playlists:", file=sys.stderr)
    playlists = fetch_user_playlists(access_token, resolve_track_counts=False)
    print_user_playlists(playlists)
    if not playlists:
        _die("No playlists returned; nothing to export.")

    if args.pick is not None:
        try:
            pick_indices = parse_pick_input(args.pick)
        except ValueError as e:
            _die(str(e))
        bad = [i for i in pick_indices if i > len(playlists)]
        if bad:
            _die(f"--pick out of range: {bad} (listed {len(playlists)} playlists).")
    else:
        pick_indices = prompt_playlist_picks(len(playlists))

    print(
        f"\nExporting playlist(s) {pick_indices} → {export_path.name}",
        file=sys.stderr,
    )
    export_result = export_playlists(
        access_token,
        playlists,
        pick_indices,
        export_path,
        continue_on_error=args.continue_on_export_error,
    )
    spotify_elapsed = time.monotonic() - t_spotify
    stage_seconds.append(("spotify_export", spotify_elapsed))
    fail_note = (
        f", {len(export_result.failures)} playlist(s) skipped"
        if export_result.failures
        else ""
    )
    print(
        f"=== spotify_export (done, {spotify_elapsed:.1f}s) ===\n"
        f"  wrote {export_result.row_count} rows to {export_path.name}{fail_note}",
        file=sys.stderr,
    )
    if args.dry_run:
        print(
            "  dry-run: export file kept for merge_queue preview; "
            "queue and merge_state will not be updated.",
            file=sys.stderr,
        )

    merge_args = [
        "--workspace",
        str(workspace),
        "--spotify-export",
        str(export_path),
    ]
    if args.force_full_import:
        merge_args.append("--force-full-import")
    if args.dry_run:
        merge_args.append("--dry-run")
    stage_seconds.append(
        (
            "merge_queue",
            _run_python_step(
                "merge_queue.py",
                merge_args,
                workspace=workspace,
                label="merge_queue",
            ),
        )
    )

    stopped_early: Optional[str] = None
    if export_result.failures:
        stopped_early = (
            f"{len(export_result.failures)} playlist export failure(s); "
            "continued with partial export"
        )

    if args.dry_run or args.skip_slskd:
        reason = "dry-run" if args.dry_run else "--skip-slskd"
        print(f"\nPipeline stopped before slskd-spotify ({reason}).", file=sys.stderr)
        _print_pipeline_summary(
            stage_seconds=stage_seconds,
            stopped_early=stopped_early or reason,
        )
        return

    if not confirm_slskd(queue_path=queue_path, assume_yes=args.yes):
        print("Skipped slskd-spotify.py.", file=sys.stderr)
        _print_pipeline_summary(
            stage_seconds=stage_seconds, stopped_early="user declined slskd"
        )
        return

    slskd_argv = build_slskd_argv(
        workspace=workspace,
        queue_path=queue_path,
        resume=False,
        checkpoint_path=checkpoint_path,
        download_settle_seconds=args.download_settle_seconds,
        skip_download_reconcile=args.skip_download_reconcile,
        skip_pending_csv=args.skip_pending_csv,
        trim_queue=args.trim_queue,
    )
    stage_seconds.append(
        (
            "slskd-spotify",
            _run_python_step(
                "slskd-spotify.py",
                slskd_argv,
                workspace=workspace,
                label="slskd-spotify",
            ),
        )
    )
    _print_pipeline_summary(
        stage_seconds=stage_seconds,
        stopped_early=stopped_early,
    )


if __name__ == "__main__":
    main()
