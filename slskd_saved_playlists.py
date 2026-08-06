"""
Saved Spotify playlists for stable export (GOAL-04).

Persists playlist IDs in ``<workspace>/saved_playlists.json`` so routine runs
do not depend on ``GET /me/playlists`` list order.
"""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

SAVED_PLAYLISTS_FILENAME = "saved_playlists.json"
SAVED_PLAYLISTS_VERSION = 1
_SPOTIFY_PLAYLIST_ID = re.compile(r"^[0-9A-Za-z]{22}$")


def saved_playlists_path(workspace: Path) -> Path:
    return workspace / SAVED_PLAYLISTS_FILENAME


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _empty_state() -> Dict[str, Any]:
    return {"version": SAVED_PLAYLISTS_VERSION, "updated_at": "", "playlists": []}


def load_saved_playlists(workspace: Path) -> Dict[str, Any]:
    path = saved_playlists_path(workspace)
    if not path.is_file():
        return _empty_state()
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return _empty_state()
    if not isinstance(data, dict):
        return _empty_state()
    playlists = data.get("playlists")
    if not isinstance(playlists, list):
        playlists = []
    normalized: List[Dict[str, Any]] = []
    for entry in playlists:
        if not isinstance(entry, dict):
            continue
        pid = (entry.get("id") or "").strip()
        if not pid:
            continue
        normalized.append(
            {
                "id": pid,
                "name": (entry.get("name") or "").strip(),
                "enabled": bool(entry.get("enabled", True)),
            }
        )
    return {
        "version": int(data.get("version") or SAVED_PLAYLISTS_VERSION),
        "updated_at": str(data.get("updated_at") or ""),
        "playlists": normalized,
    }


def save_saved_playlists(workspace: Path, state: Dict[str, Any]) -> None:
    path = saved_playlists_path(workspace)
    path.parent.mkdir(parents=True, exist_ok=True)
    out = {
        "version": SAVED_PLAYLISTS_VERSION,
        "updated_at": _utc_now_iso(),
        "playlists": state.get("playlists") or [],
    }
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, sort_keys=True)
        f.write("\n")
        f.flush()
    tmp.replace(path)


def parse_spotify_playlist_id(raw: str) -> str:
    """Accept bare 22-char ID or open.spotify.com / spotify:playlist: URL."""
    s = (raw or "").strip()
    if _SPOTIFY_PLAYLIST_ID.fullmatch(s):
        return s
    m = re.search(r"playlist[/:]([0-9A-Za-z]{22})", s)
    if m:
        return m.group(1)
    raise ValueError(f"Could not parse playlist ID from: {raw!r}")


def parse_playlist_id_csv(raw: str) -> List[str]:
    parts = [p.strip() for p in (raw or "").split(",") if p.strip()]
    if not parts:
        raise ValueError("Enter at least one playlist ID.")
    return [parse_spotify_playlist_id(p) for p in parts]


def entries_for_export(
    state: Dict[str, Any],
    *,
    pick_indices: Optional[List[int]] = None,
) -> List[Dict[str, Any]]:
    """
    Build export targets from saved state.

    *pick_indices* None → all enabled playlists in file order.
    Otherwise 1-based indices into the full saved list (enabled or not).
    """
    playlists: List[Dict[str, Any]] = list(state.get("playlists") or [])
    if not playlists:
        return []

    if pick_indices is None:
        return [dict(p) for p in playlists if p.get("enabled", True)]

    out: List[Dict[str, Any]] = []
    for idx in pick_indices:
        if idx < 1 or idx > len(playlists):
            raise IndexError(
                f"Saved playlist index {idx} is out of range (1–{len(playlists)})."
            )
        out.append(dict(playlists[idx - 1]))
    return out


def merge_library_picks_into_saved(
    state: Dict[str, Any],
    library_playlists: Sequence[Dict[str, Any]],
    pick_indices: List[int],
) -> Dict[str, Any]:
    """Upsert picks from a library listing into saved state (preserve file order)."""
    by_id: Dict[str, Dict[str, Any]] = {
        (p.get("id") or "").strip(): dict(p)
        for p in state.get("playlists") or []
        if (p.get("id") or "").strip()
    }
    order: List[str] = [
        (p.get("id") or "").strip()
        for p in state.get("playlists") or []
        if (p.get("id") or "").strip()
    ]

    for idx in pick_indices:
        if idx < 1 or idx > len(library_playlists):
            continue
        pl = library_playlists[idx - 1]
        pid = (pl.get("id") or "").strip()
        if not pid:
            continue
        entry = {
            "id": pid,
            "name": (pl.get("name") or "").strip(),
            "enabled": True,
        }
        if pid not in by_id:
            order.append(pid)
        by_id[pid] = entry

    state["playlists"] = [by_id[pid] for pid in order]
    return state


def apply_enabled_updates(state: Dict[str, Any], updates: Dict[str, bool]) -> Dict[str, Any]:
    """Set ``enabled`` for saved playlists whose id is a key in *updates*.

    Unknown ids are ignored. Mutates and returns *state* for convenience.
    """
    for pl in state.get("playlists") or []:
        pid = (pl.get("id") or "").strip()
        if pid in updates:
            pl["enabled"] = bool(updates[pid])
    return state


def list_saved_playlists_public(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """1-based, JSON-friendly view of saved playlists for the web UI."""
    playlists: List[Dict[str, Any]] = list(state.get("playlists") or [])
    return [
        {
            "index": i,
            "id": pl.get("id") or "",
            "name": pl.get("name") or pl.get("id") or "?",
            "enabled": bool(pl.get("enabled", True)),
        }
        for i, pl in enumerate(playlists, start=1)
    ]


def entries_from_playlist_ids(ids: Sequence[str], *, names: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
    names = names or {}
    out: List[Dict[str, Any]] = []
    for pid in ids:
        pid = pid.strip()
        if not pid:
            continue
        out.append({"id": pid, "name": names.get(pid, ""), "enabled": True})
    return out


def print_saved_playlists(state: Dict[str, Any]) -> None:
    playlists: List[Dict[str, Any]] = list(state.get("playlists") or [])
    if not playlists:
        print("No saved playlists.", file=sys.stderr)
        return
    print(f"{'#':>3}  {'on':^3}  {'id':<24}  name", file=sys.stderr)
    print("-" * 92, file=sys.stderr)
    for i, pl in enumerate(playlists, start=1):
        on = "yes" if pl.get("enabled", True) else "no"
        name = (pl.get("name") or "").replace("\n", " ")
        if len(name) > 52:
            name = name[:49] + "..."
        print(
            f"{i:3}  {on:^3}  {(pl.get('id') or '?'):<24}  {name}",
            file=sys.stderr,
        )
