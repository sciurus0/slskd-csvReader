"""NORM-04 — album/track sanitize-only (Phase C)."""

from __future__ import annotations

import unittest

from merge_queue import _sanitize_pipeline_row
from slskd_normalize import normalize_album_track_n4, normalize_queue_row
from slskd_sanitize import sanitize_queue_field


class TestNormalizeAlbumTrackN4(unittest.TestCase):
    def test_remix_preserved_on_track(self) -> None:
        raw = "You Want It Darker - Paul Kalkbrenner Remix"
        row = normalize_album_track_n4({"album": "", "track": raw})
        self.assertEqual(row["track"], raw)
        self.assertIn("Remix", row["track"])

    def test_feat_preserved_on_track(self) -> None:
        raw = "Rosella (feat. Sessa)"
        row = normalize_album_track_n4({"album": raw, "track": raw})
        self.assertIn("feat", row["track"].lower())

    def test_forbidden_slash_becomes_space(self) -> None:
        row = normalize_album_track_n4({"album": "", "track": "UPS / DOWNS"})
        self.assertEqual(row["track"], "UPS DOWNS")

    def test_forbidden_colon_becomes_space(self) -> None:
        row = normalize_album_track_n4(
            {
                "album": "CHPT. 9: 4 MiLLiON PPL LiVE iN LOS ANGELES",
                "track": "UPS DOWNS",
            }
        )
        self.assertEqual(row["album"], "CHPT. 9 4 MiLLiON PPL LiVE iN LOS ANGELES")
        self.assertIn("LiVE", row["album"])

    def test_ascii_quotes_removed_from_track(self) -> None:
        raw = 'Tehran 1979 (From "American Gods" Soundtrack) (feat. Guest)'
        row = normalize_album_track_n4({"album": "", "track": raw})
        self.assertNotIn('"', row["track"])
        self.assertIn("Soundtrack", row["track"])
        self.assertIn("feat", row["track"].lower())

    def test_matches_sanitize_queue_field_only(self) -> None:
        album = "Album | B-side"
        track = "Track/Name"
        row = normalize_album_track_n4({"album": album, "track": track})
        self.assertEqual(row["album"], sanitize_queue_field(album))
        self.assertEqual(row["track"], sanitize_queue_field(track))


class TestNormalizeQueueRowArtistOnly(unittest.TestCase):
    def test_album_track_unchanged_by_artist_norm(self) -> None:
        album = "Some Album (Deluxe)"
        track = "Song (Live at Wembley)"
        row = normalize_queue_row(
            {
                "artist": "Michael Wilbur, Tonio Sagan",
                "album": album,
                "track": track,
            }
        )
        self.assertEqual(row["album"], album)
        self.assertEqual(row["track"], track)
        self.assertEqual(row["artist_primary"], "Michael Wilbur")
        self.assertEqual(row["artist_alternates"], "Tonio Sagan")


class TestSanitizePipelineRow(unittest.TestCase):
    def _base_row(self, **kwargs: str) -> dict[str, str]:
        base = {
            "artist": "Artist",
            "album": "",
            "track": "",
            "duration_ms": "",
            "spotify_track_id": "",
            "is_unavailable": "",
        }
        base.update(kwargs)
        return base

    def test_pipeline_does_not_strip_feat_from_track(self) -> None:
        track = "Rosella (feat. Sessa)"
        out = _sanitize_pipeline_row(self._base_row(track=track, album=track))
        self.assertIn("feat", out["track"].lower())

    def test_pipeline_album_track_are_sanitize_only(self) -> None:
        album = "CHPT. 9: 4 MiLLiON PPL LiVE iN LOS ANGELES"
        track = "UPS / DOWNS"
        raw = self._base_row(album=album, track=track)
        out = _sanitize_pipeline_row(raw)
        self.assertEqual(out["album"], sanitize_queue_field(album))
        self.assertEqual(out["track"], sanitize_queue_field(track))


if __name__ == "__main__":
    unittest.main()
