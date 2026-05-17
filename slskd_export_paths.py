"""
Spotify export filename conventions and discovery (``YYYYMMDD-spotify-export.csv``).
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

SPOTIFY_EXPORT_SUFFIX = "-spotify-export.csv"


def parse_export_date(s: str) -> str:
    if len(s) != 8 or not s.isdigit():
        raise ValueError(f"date must be YYYYMMDD, got {s!r}")
    try:
        datetime.strptime(s, "%Y%m%d")
    except ValueError:
        raise ValueError(f"invalid calendar date: {s!r}") from None
    return s


def spotify_export_basename(date_str: str) -> str:
    return f"{date_str}{SPOTIFY_EXPORT_SUFFIX}"


def export_date_from_name(name: str) -> Optional[str]:
    if not name.endswith(SPOTIFY_EXPORT_SUFFIX):
        return None
    prefix = name[: -len(SPOTIFY_EXPORT_SUFFIX)]
    if len(prefix) != 8 or not prefix.isdigit():
        return None
    try:
        datetime.strptime(prefix, "%Y%m%d")
    except ValueError:
        return None
    return prefix


def find_latest_spotify_export(workspace: Path) -> Optional[Path]:
    dated: List[Tuple[str, Path]] = []
    for path in workspace.glob(f"*{SPOTIFY_EXPORT_SUFFIX}"):
        if not path.is_file():
            continue
        date_str = export_date_from_name(path.name)
        if date_str:
            dated.append((date_str, path))
    if not dated:
        return None
    dated.sort(key=lambda item: item[0])
    return dated[-1][1]


def default_new_export_path(
    workspace: Path,
    date_str: Optional[str] = None,
) -> Tuple[Path, str]:
    """Path for writing a new export (default date: today's local calendar)."""
    d = date_str or datetime.now().strftime("%Y%m%d")
    return workspace / spotify_export_basename(d), d


def resolve_spotify_export(
    workspace: Path,
    *,
    date_str: Optional[str] = None,
    export_path: Optional[Path] = None,
) -> Tuple[Path, str]:
    """
    Resolve which export CSV to read.

    Priority: explicit ``export_path`` → ``date_str`` file → newest ``*-spotify-export.csv``.
    """
    if export_path is not None:
        path = export_path.resolve()
        date = (
            date_str
            or export_date_from_name(path.name)
            or datetime.now().strftime("%Y%m%d")
        )
        return path, date
    if date_str is not None:
        return workspace / spotify_export_basename(date_str), date_str
    latest = find_latest_spotify_export(workspace)
    if latest is None:
        raise FileNotFoundError(
            f"No Spotify export in {workspace} (expected *{SPOTIFY_EXPORT_SUFFIX})"
        )
    date = export_date_from_name(latest.name)
    if not date:
        raise ValueError(f"Could not parse date from export filename: {latest.name}")
    return latest, date
