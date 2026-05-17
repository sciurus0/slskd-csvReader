"""
Pipeline workspace layout (POLISH-02, POLISH-03).

Default workspace is ``<cwd>/data/`` with:

- ``to_queue.csv``, ``success_ledger.csv``, ``merge_state.json``, ``checkpoint.pkl``
- ``exports/`` — ``YYYYMMDD-spotify-export.csv``
- ``logs/`` — slskd import logs and reports
- ``archive/csv-YYYYMMDD/`` — dated CSV backups (merge, trim)

Legacy flat layout (queue + exports beside scripts in ``cwd``) is detected when
``to_queue.csv`` exists at ``cwd`` but not under ``data/``.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import List, Optional

DATA_DIR_NAME = "data"
EXPORTS_DIR_NAME = "exports"
LOGS_DIR_NAME = "logs"
ARCHIVE_DIR_NAME = "archive"
ARCHIVE_CSV_PREFIX = "csv-"
QUEUE_FILENAME = "to_queue.csv"
PENDING_RETRY_FILENAME = "to_queue_pending.csv"
PENDING_VALIDATE_FILENAME = "to_queue_pending_validate.csv"


def default_workspace(cwd: Optional[Path] = None) -> Path:
    """
    Resolve the default pipeline workspace directory.

    Prefer ``cwd/data/`` unless legacy flat files live directly under ``cwd``.
    """
    base = (cwd or Path.cwd()).resolve()
    data_dir = base / DATA_DIR_NAME
    legacy_queue = base / QUEUE_FILENAME
    data_queue = data_dir / QUEUE_FILENAME

    if data_queue.is_file():
        return data_dir
    if legacy_queue.is_file():
        return base
    if data_dir.is_dir():
        return data_dir
    return data_dir


def resolve_workspace(
    workspace: Optional[Path],
    *,
    cwd: Optional[Path] = None,
) -> Path:
    if workspace is not None:
        return workspace.resolve()
    return default_workspace(cwd)


def exports_dir(workspace: Path) -> Path:
    return workspace / EXPORTS_DIR_NAME


def logs_dir(workspace: Path) -> Path:
    return workspace / LOGS_DIR_NAME


def queue_csv_path(workspace: Path) -> Path:
    return workspace / QUEUE_FILENAME


def archive_dir(workspace: Path) -> Path:
    return workspace / ARCHIVE_DIR_NAME


def archive_csv_dir(workspace: Path, date_str: str) -> Path:
    """Dated folder for CSV backups, e.g. ``archive/csv-20260518/``."""
    return archive_dir(workspace) / f"{ARCHIVE_CSV_PREFIX}{date_str}"


def archive_csv_path(workspace: Path, date_str: str, filename: str) -> Path:
    """Path for a backup file inside ``archive/csv-YYYYMMDD/`` (parent dirs created)."""
    dest_dir = archive_csv_dir(workspace, date_str)
    dest_dir.mkdir(parents=True, exist_ok=True)
    return dest_dir / filename


def copy_to_archive(
    workspace: Path,
    source: Path,
    *,
    date_str: str,
    dest_name: Optional[str] = None,
) -> Path:
    """Copy ``source`` into ``archive/csv-YYYYMMDD/`` and return the destination path."""
    dest = archive_csv_path(workspace, date_str, dest_name or source.name)
    shutil.copy2(source, dest)
    return dest


def is_validate_pending_csv(path: str | Path) -> bool:
    return Path(path).name == PENDING_VALIDATE_FILENAME


def ephemeral_pending_csv_paths(
    workspace: Path,
    *,
    include_retry_pending: bool = True,
) -> List[Path]:
    names = [PENDING_VALIDATE_FILENAME]
    if include_retry_pending:
        names.append(PENDING_RETRY_FILENAME)
    return [workspace / name for name in names if (workspace / name).is_file()]


def remove_ephemeral_pending_csvs(
    workspace: Path,
    *,
    include_retry_pending: bool = True,
) -> List[Path]:
    """Remove ephemeral pending CSVs from the workspace (POLISH-03)."""
    removed: List[Path] = []
    for path in ephemeral_pending_csv_paths(
        workspace, include_retry_pending=include_retry_pending
    ):
        path.unlink()
        removed.append(path)
    return removed


def ensure_workspace_layout(workspace: Path) -> None:
    """Create workspace and standard subdirectories if missing."""
    workspace.mkdir(parents=True, exist_ok=True)
    exports_dir(workspace).mkdir(parents=True, exist_ok=True)
    logs_dir(workspace).mkdir(parents=True, exist_ok=True)
    archive_dir(workspace).mkdir(parents=True, exist_ok=True)


def export_search_roots(workspace: Path) -> List[Path]:
    """Directories to scan for ``*-spotify-export.csv`` (exports/ first, then legacy root)."""
    ws = workspace.resolve()
    return [exports_dir(ws).resolve(), ws]
