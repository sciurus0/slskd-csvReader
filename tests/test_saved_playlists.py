"""GOAL-04: saved_playlists.json load/save and export target resolution."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from slskd_saved_playlists import (
    apply_enabled_updates,
    entries_for_export,
    entries_from_playlist_ids,
    list_saved_playlists_public,
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

    def test_apply_enabled_updates(self) -> None:
        state = {
            "playlists": [
                {"id": "a" * 22, "name": "On", "enabled": True},
                {"id": "b" * 22, "name": "Off", "enabled": False},
            ]
        }
        apply_enabled_updates(state, {"a" * 22: False, "z" * 22: True})
        self.assertFalse(state["playlists"][0]["enabled"])
        self.assertFalse(state["playlists"][1]["enabled"])  # untouched, unknown id ignored

    def test_list_saved_playlists_public(self) -> None:
        state = {
            "playlists": [
                {"id": "a" * 22, "name": "On", "enabled": True},
                {"id": "b" * 22, "name": "", "enabled": False},
            ]
        }
        rows = list_saved_playlists_public(state)
        self.assertEqual(rows[0], {"index": 1, "id": "a" * 22, "name": "On", "enabled": True})
        self.assertEqual(rows[1]["name"], "b" * 22)  # falls back to id when name blank
        self.assertEqual(rows[1]["index"], 2)


if __name__ == "__main__":
    unittest.main()
