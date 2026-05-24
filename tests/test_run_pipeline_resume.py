"""RUN-02: run_pipeline resume / slskd-only argv wiring."""

from __future__ import annotations

import unittest
from pathlib import Path

from run_pipeline import _checkpoint_path, build_slskd_argv


class TestRunPipelineResume(unittest.TestCase):
    def test_checkpoint_path_defaults_to_workspace(self) -> None:
        ws = Path("/tmp/dev-workspace")
        self.assertEqual(
            _checkpoint_path(ws, None),
            (ws / "checkpoint.json").resolve(),
        )

    def test_build_slskd_argv_default_trims_queue(self) -> None:
        ws = Path("/tmp/dev-workspace")
        queue = ws / "to_queue.csv"
        ckpt = ws / "checkpoint.json"
        argv = build_slskd_argv(
            workspace=ws,
            queue_path=queue,
            resume=True,
            checkpoint_path=ckpt,
            download_settle_seconds=None,
            skip_pending_csv=False,
            no_trim_queue=False,
        )
        self.assertIn("--resume", argv)
        self.assertNotIn("--trim-queue", argv)
        self.assertNotIn("--no-trim-queue", argv)
        self.assertEqual(
            argv,
            [
                "--csv",
                str(queue),
                "--output-dir",
                str(ws / "logs"),
                "--checkpoint-file",
                str(ckpt),
                "--resume",
            ],
        )

    def test_build_slskd_argv_no_trim_queue_flag(self) -> None:
        ws = Path("/tmp/dev-workspace")
        argv = build_slskd_argv(
            workspace=ws,
            queue_path=ws / "to_queue.csv",
            resume=False,
            checkpoint_path=ws / "checkpoint.json",
            download_settle_seconds=None,
            skip_pending_csv=False,
            no_trim_queue=True,
        )
        self.assertIn("--no-trim-queue", argv)

    def test_build_slskd_argv_full_run_no_resume(self) -> None:
        ws = Path("/tmp/dev-workspace")
        argv = build_slskd_argv(
            workspace=ws,
            queue_path=ws / "to_queue_pending.csv",
            resume=False,
            checkpoint_path=ws / "checkpoint.json",
            download_settle_seconds=30.0,
            skip_pending_csv=True,
            no_trim_queue=False,
        )
        self.assertNotIn("--resume", argv)
        self.assertNotIn("--no-trim-queue", argv)
        self.assertIn("--download-settle-seconds", argv)
        self.assertIn("30.0", argv)
        self.assertIn("--skip-pending-csv", argv)


if __name__ == "__main__":
    unittest.main()
