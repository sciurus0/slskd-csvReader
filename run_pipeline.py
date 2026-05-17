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
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from slskd_config import load_api_txt

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


def export_playlists(
    access_token: str,
    playlists: List[Dict[str, Any]],
    pick_indices: List[int],
    output_path: Path,
) -> int:
    """Export selected playlists into one combined CSV."""
    max_index = max(pick_indices)
    if max_index > len(playlists):
        _die(f"Pick {max_index} is out of range (you have {len(playlists)} playlists).")

    all_rows: List[Dict[str, str]] = []
    for idx in pick_indices:
        pl = playlists[idx - 1]
        playlist_id = (pl.get("id") or "").strip()
        if not playlist_id:
            _die(f"Playlist #{idx} has no id; cannot export.")
        label = pl.get("name") or playlist_id
        try:
            rows = fetch_playlist_track_rows(access_token, playlist_id)
        except Exception as e:
            _die(f"Export failed for playlist #{idx} ({label}): {e}")
        print(f"  Playlist #{idx} ({label}): {len(rows)} rows", file=sys.stderr)
        all_rows.extend(rows)

    if not all_rows:
        print(
            "Warning: no tracks exported (empty playlist(s) or no readable tracks).",
            file=sys.stderr,
        )
    write_csv(str(output_path), all_rows)
    return len(all_rows)


def _run_python_step(
    script: str,
    args: List[str],
    *,
    workspace: Path,
    label: str,
) -> None:
    cmd = [sys.executable, str(_repo_root() / script), *args]
    print(f"\n=== {label} ===", file=sys.stderr)
    print("  " + " ".join(cmd), file=sys.stderr)
    result = subprocess.run(cmd, cwd=str(workspace))
    if result.returncode != 0:
        _die(f"{label} failed (exit {result.returncode}).")


def confirm_slskd(*, assume_yes: bool) -> bool:
    if assume_yes:
        return True
    try:
        answer = input("\nStart SLSKD queue processing for to_queue.csv? [y/N]: ").strip().lower()
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
        help="Calendar date for filenames (default: today, local)",
    )
    parser.add_argument(
        "--force-full-import",
        action="store_true",
        help="Pass through to merge_queue: ignore added_at watermarks (ledger + dedupe still apply)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Export and merge_queue --dry-run only; do not write queue or run slskd",
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
    args = parser.parse_args()

    workspace = args.workspace.resolve()
    if not workspace.is_dir():
        _die(f"Workspace is not a directory: {workspace}")

    date_str = args.date or datetime.now().strftime("%Y%m%d")
    if len(date_str) != 8 or not date_str.isdigit():
        _die("--date must be YYYYMMDD")

    export_path = workspace / f"{date_str}-spotify-export.csv"
    queue_path = workspace / "to_queue.csv"

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

    print("=== Spotify ===", file=sys.stderr)
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
    row_count = export_playlists(access_token, playlists, pick_indices, export_path)
    print(f"Wrote {row_count} rows to {export_path}", file=sys.stderr)

    merge_args = ["--workspace", str(workspace), "--date", date_str]
    if args.force_full_import:
        merge_args.append("--force-full-import")
    if args.dry_run:
        merge_args.append("--dry-run")
    _run_python_step("merge_queue.py", merge_args, workspace=workspace, label="merge_queue")

    if args.dry_run or args.skip_slskd:
        print("\nPipeline stopped before slskd-spotify.", file=sys.stderr)
        return

    if not confirm_slskd(assume_yes=args.yes):
        print("Skipped slskd-spotify.py.", file=sys.stderr)
        return

    slskd_args = ["--csv", str(queue_path), "--output-dir", str(workspace / "logs")]
    if args.download_settle_seconds is not None:
        slskd_args.extend(
            ["--download-settle-seconds", str(args.download_settle_seconds)]
        )
    if args.skip_download_reconcile:
        slskd_args.append("--skip-download-reconcile")
    if args.skip_pending_csv:
        slskd_args.append("--skip-pending-csv")

    _run_python_step("slskd-spotify.py", slskd_args, workspace=workspace, label="slskd-spotify")


if __name__ == "__main__":
    main()
