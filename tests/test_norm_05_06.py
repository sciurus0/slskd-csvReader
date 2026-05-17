"""NORM-05 (import scope) and NORM-06 (ledger identity)."""

from __future__ import annotations

import unittest

from slskd_pipeline_state import (
    append_success_ledger,
    backfill_success_ledger,
    export_row_eligible_for_merge,
    filter_export_for_merge,
    ledger_identity_artist,
    load_ledger_keys,
    normalize_ledger_row,
    pipeline_row_dedupe_key,
    success_rows_from_results_log,
)


class TestNorm05ImportScope(unittest.TestCase):
    def test_unavailable_row_is_merge_eligible(self) -> None:
        row = {
            "artist": "Michael Wilbur",
            "album": "Victory",
            "track": "Victory",
            "is_unavailable": "true",
            "playlist_id": "p1",
            "added_at": "2026-05-17T12:00:00Z",
        }
        self.assertTrue(export_row_eligible_for_merge(row))

    def test_filter_keeps_unavailable_not_in_ledger(self) -> None:
        export_rows = [
            {
                "artist": "Artist",
                "artist_primary": "Artist",
                "album": "Al",
                "track": "T",
                "is_unavailable": "true",
                "playlist_id": "p1",
                "added_at": "2026-05-17T12:00:00Z",
            }
        ]
        kept, stats = filter_export_for_merge(export_rows, set(), {"playlists": {}}, force_full_import=True)
        self.assertEqual(len(kept), 1)
        self.assertEqual(stats.export_rows_kept, 1)


class TestNorm06LedgerIdentity(unittest.TestCase):
    def test_dedupe_key_uses_primary_not_full_credits(self) -> None:
        row = {
            "artist": "Michael Wilbur, Tonio Sagan",
            "artist_primary": "Michael Wilbur",
            "album": "Victory",
            "track": "Victory",
        }
        key = pipeline_row_dedupe_key(row)
        self.assertEqual(key, ("triple", "michael wilbur", "victory", "victory"))

    def test_ledger_identity_from_legacy_artist_only(self) -> None:
        row = {"artist": "Michael Wilbur, Tonio Sagan", "artist_primary": ""}
        self.assertEqual(ledger_identity_artist(row), "Michael Wilbur")

    def test_normalize_ledger_row_backfills_primary(self) -> None:
        out = normalize_ledger_row(
            {
                "artist": "Michael Wilbur, Tonio Sagan",
                "album": "A",
                "track": "T",
            }
        )
        self.assertEqual(out["artist_primary"], "Michael Wilbur")
        self.assertIn(",", out["artist"])

    def test_success_log_row_gets_artist_primary(self) -> None:
        rows = success_rows_from_results_log(
            [
                {
                    "status": "success",
                    "artist": "Michael Wilbur, Tonio Sagan",
                    "artist_primary": "Michael Wilbur",
                    "album": "A",
                    "track": "T",
                }
            ]
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["artist_primary"], "Michael Wilbur")

    def test_load_ledger_keys_matches_primary_identity(self) -> None:
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            append_success_ledger(
                workspace,
                [
                    {
                        "artist": "Michael Wilbur, Tonio Sagan",
                        "artist_primary": "Michael Wilbur",
                        "album": "Victory",
                        "track": "Victory",
                        "completed_at": "2026-05-17T12:00:00Z",
                    }
                ],
            )
            keys = load_ledger_keys(workspace)
            queue_key = pipeline_row_dedupe_key(
                {
                    "artist": "Michael Wilbur, Tonio Sagan",
                    "artist_primary": "Michael Wilbur",
                    "album": "Victory",
                    "track": "Victory",
                }
            )
            self.assertIn(queue_key, keys)

    def test_backfill_adds_artist_primary_column(self) -> None:
        import csv
        import tempfile
        from io import StringIO
        from pathlib import Path

        from slskd_csv import decode_pipeline_text
        from slskd_pipeline_state import SUCCESS_LEDGER_COLUMNS

        with tempfile.TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            path = workspace / "success_ledger.csv"
            path.write_text(
                "spotify_track_id,artist,album,track,completed_at,playlist_id\n"
                ',\"Michael Wilbur, Tonio Sagan\",Victory,Victory,2026-05-17T12:00:00Z,\n',
                encoding="utf-8",
            )
            rows_in, rows_out = backfill_success_ledger(workspace, dry_run=False)
            self.assertEqual(rows_in, 1)
            self.assertEqual(rows_out, 1)
            text = decode_pipeline_text(path.read_bytes())
            rows = list(csv.DictReader(StringIO(text)))
            self.assertEqual(list(rows[0].keys()), list(SUCCESS_LEDGER_COLUMNS))
            self.assertEqual(rows[0]["artist_primary"], "Michael Wilbur")


if __name__ == "__main__":
    unittest.main()
