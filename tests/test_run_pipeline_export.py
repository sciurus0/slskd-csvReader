"""RUN-04: multi-playlist export failure policy."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from run_pipeline import ExportResult, export_playlists


class TestExportPlaylists(unittest.TestCase):
    def _playlists(self) -> list[dict]:
        return [
            {"id": "pl1", "name": "One"},
            {"id": "pl2", "name": "Two"},
            {"id": "pl3", "name": "Three"},
        ]

    def test_abort_on_fetch_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "export.csv"

            def fetch(_token: str, pid: str) -> list[dict[str, str]]:
                if pid == "pl2":
                    raise RuntimeError("429 cooldown")
                return [{"artist": "A", "album": "", "track": "t"}]

            with patch(
                "run_pipeline.fetch_playlist_track_rows",
                side_effect=fetch,
            ):
                with self.assertRaises(SystemExit):
                    export_playlists(
                        "token",
                        self._playlists(),
                        [1, 2, 3],
                        out,
                        continue_on_error=False,
                    )

    def test_continue_skips_failed_playlists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "export.csv"

            def fetch(_token: str, pid: str) -> list[dict[str, str]]:
                if pid == "pl2":
                    raise RuntimeError("429 cooldown")
                return [{"artist": pid, "album": "", "track": "t"}]

            with patch(
                "run_pipeline.fetch_playlist_track_rows",
                side_effect=fetch,
            ):
                result = export_playlists(
                    "token",
                    self._playlists(),
                    [1, 2, 3],
                    out,
                    continue_on_error=True,
                )

            self.assertEqual(result.row_count, 2)
            self.assertEqual(len(result.failures), 1)
            self.assertIn("#2", result.failures[0])
            self.assertTrue(out.is_file())

    def test_continue_all_fail_exits(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "export.csv"

            with patch(
                "run_pipeline.fetch_playlist_track_rows",
                side_effect=RuntimeError("network"),
            ):
                with self.assertRaises(SystemExit):
                    export_playlists(
                        "token",
                        self._playlists(),
                        [1, 2],
                        out,
                        continue_on_error=True,
                    )

    def test_continue_skips_missing_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "export.csv"
            playlists = [
                {"id": "pl1", "name": "One"},
                {"id": "", "name": "Bad"},
            ]

            with patch(
                "run_pipeline.fetch_playlist_track_rows",
                return_value=[{"artist": "A", "album": "", "track": "t"}],
            ):
                result = export_playlists(
                    "token",
                    playlists,
                    [1, 2],
                    out,
                    continue_on_error=True,
                )

            self.assertEqual(result, ExportResult(1, ("#2 (Bad): no playlist id",)))


if __name__ == "__main__":
    unittest.main()
