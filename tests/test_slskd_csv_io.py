"""HOUSE-01: centralized pipeline CSV read helpers."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from slskd_csv import (
    atomic_write_pipeline_csv,
    decode_pipeline_text,
    read_pipeline_csv_rows,
    read_pipeline_csv_with_fieldnames,
)


class TestSlskdCsvIo(unittest.TestCase):
    def test_read_with_bom(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "queue.csv"
            atomic_write_pipeline_csv(
                path,
                [{"artist": "A", "album": "B", "track": "C"}],
                fieldnames=("artist", "album", "track"),
            )
            fieldnames, rows = read_pipeline_csv_with_fieldnames(path)
            self.assertEqual(fieldnames, ["artist", "album", "track"])
            self.assertEqual(rows[0]["artist"], "A")

    def test_non_utf8_fails_fast(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bad.csv"
            path.write_bytes(b"artist,album,track\n\xff\xfe")
            with self.assertRaises(UnicodeDecodeError):
                read_pipeline_csv_rows(path)

    def test_decode_pipeline_text_rejects_latin1(self) -> None:
        with self.assertRaises(UnicodeDecodeError):
            decode_pipeline_text("café".encode("latin-1"))


if __name__ == "__main__":
    unittest.main()
