"""GOAL-04: saved_playlists.json load/save and export target resolution."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from slskd_saved_playlists import (
    entries_for_export,
    entries_from_playlist_ids,
    load_saved_playlists,
    merge_library_picks_into_saved,
    parse_playlist_id_csv,
    parse_spotify_playlist_id,
    save_saved_playlists,
)


class TestSavedPlaylists(unittest.TestCase):
    def test_parse_playlist_id(self) -> None:
        self.assertEqual(
            parse_spotify_playlist_id("37i9dQZF1DXcBWIGoYBM5M"),
            "37i9dQZF1DXcBWIGoYBM5M",
        )
        self.assertEqual(
            parse_spotify_playlist_id(
                "https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M"
            ),
            "37i9dQZF1DXcBWIGoYBM5M",
        )

    def test_merge_library_picks_preserves_order(self) -> None:
        state = {
            "playlists": [
                {"id": "aaaaaaaaaaaaaaaaaaaaaa", "name": "Old", "enabled": True},
            ]
        }
        library = [
            {"id": "bbbbbbbbbbbbbbbbbbbbbb", "name": "New", "tracks_total": 1},
            {"id": "aaaaaaaaaaaaaaaaaaaaaa", "name": "Old Renamed", "tracks_total": 2},
        ]
        merge_library_picks_into_saved(state, library, [2, 1])
        ids = [p["id"] for p in state["playlists"]]
        self.assertEqual(
            ids,
            ["aaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbb"],
        )
        self.assertEqual(state["playlists"][0]["name"], "Old Renamed")
        self.assertEqual(state["playlists"][1]["name"], "New")

    def test_entries_for_export_enabled_only(self) -> None:
        state = {
            "playlists": [
                {"id": "a" * 22, "name": "On", "enabled": True},
                {"id": "b" * 22, "name": "Off", "enabled": False},
            ]
        }
        all_enabled = entries_for_export(state)
        self.assertEqual(len(all_enabled), 1)
        picked = entries_for_export(state, pick_indices=[2])
        self.assertEqual(picked[0]["name"], "Off")

    def test_save_and_load_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ws = Path(tmp)
            state = {
                "playlists": [
                    {"id": "c" * 22, "name": "Test", "enabled": True},
                ]
            }
            save_saved_playlists(ws, state)
            loaded = load_saved_playlists(ws)
            self.assertEqual(len(loaded["playlists"]), 1)
            self.assertEqual(loaded["playlists"][0]["id"], "c" * 22)

    def test_parse_playlist_id_csv(self) -> None:
        ids = parse_playlist_id_csv(
            "37i9dQZF1DXcBWIGoYBM5M, 37i9dQZF1DX0XUsuyj4F3n"
        )
        self.assertEqual(len(ids), 2)

    def test_entries_from_playlist_ids(self) -> None:
        pid = "d" * 22
        rows = entries_from_playlist_ids([pid], names={pid: "Named"})
        self.assertEqual(rows[0]["name"], "Named")


if __name__ == "__main__":
    unittest.main()
