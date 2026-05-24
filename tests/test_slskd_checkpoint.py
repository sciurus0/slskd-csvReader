"""SEC-01: JSON checkpoint save/load and legacy pickle migration."""

from __future__ import annotations

import json
import pickle
import tempfile
import unittest
from pathlib import Path

from slskd_csv import (
    CHECKPOINT_FORMAT_VERSION,
    load_checkpoint,
    resolve_checkpoint_paths,
    save_checkpoint,
)
from slskd_worker import Stats


class TestCheckpointJson(unittest.TestCase):
    def test_resolve_paths_from_json(self) -> None:
        json_path, pkl_path = resolve_checkpoint_paths("/tmp/ws/checkpoint.json")
        self.assertEqual(json_path, Path("/tmp/ws/checkpoint.json"))
        self.assertEqual(pkl_path, Path("/tmp/ws/checkpoint.pkl"))

    def test_resolve_paths_from_pkl_arg(self) -> None:
        json_path, pkl_path = resolve_checkpoint_paths("/tmp/ws/checkpoint.pkl")
        self.assertEqual(json_path, Path("/tmp/ws/checkpoint.json"))
        self.assertEqual(pkl_path, Path("/tmp/ws/checkpoint.pkl"))

    def test_save_and_load_json_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ckpt = Path(tmp) / "checkpoint.json"
            stats = Stats()
            stats.total_processed = 3
            results = [{"row_index": 0, "artist": "A", "status": "failed"}]
            save_checkpoint(
                str(ckpt),
                next_row_index=5,
                total_rows=10,
                stats=stats,
                results_log=results,
                queued_files_tracker=[],
            )
            loaded = load_checkpoint(str(ckpt))
            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertEqual(loaded["format_version"], CHECKPOINT_FORMAT_VERSION)
            self.assertEqual(loaded["next_row_index"], 5)
            self.assertEqual(loaded["total_rows"], 10)
            self.assertEqual(len(loaded["results_log"]), 1)

    def test_migrates_legacy_pickle_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            pkl = Path(tmp) / "checkpoint.pkl"
            legacy = {
                "next_row_index": 2,
                "row_index": 1,
                "total_rows": 8,
                "stats": {"total_processed": 2, "successful": 0, "failed": 2},
                "results_log": [],
            }
            with pkl.open("wb") as f:
                pickle.dump(legacy, f)

            loaded = load_checkpoint(str(pkl))
            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertEqual(loaded["next_row_index"], 2)

            json_path = Path(tmp) / "checkpoint.json"
            self.assertTrue(json_path.is_file())
            on_disk = json.loads(json_path.read_text(encoding="utf-8"))
            self.assertEqual(on_disk["next_row_index"], 2)


if __name__ == "__main__":
    unittest.main()
