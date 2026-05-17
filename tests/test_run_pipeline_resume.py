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
            (ws / "data" / "checkpoint.pkl").resolve(),
        )

    def test_build_slskd_argv_resume_and_checkpoint(self) -> None:
        ws = Path("/tmp/dev-workspace")
        queue = ws / "to_queue.csv"
        ckpt = ws / "checkpoint.pkl"
        argv = build_slskd_argv(
            workspace=ws,
            queue_path=queue,
            resume=True,
            checkpoint_path=ckpt,
            download_settle_seconds=None,
            skip_download_reconcile=False,
            skip_pending_csv=False,
            trim_queue=True,
        )
        self.assertIn("--resume", argv)
        self.assertIn("--trim-queue", argv)
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
                "--trim-queue",
            ],
        )

    def test_build_slskd_argv_full_run_no_resume(self) -> None:
        ws = Path("/tmp/dev-workspace")
        argv = build_slskd_argv(
            workspace=ws,
            queue_path=ws / "to_queue_pending.csv",
            resume=False,
            checkpoint_path=ws / "checkpoint.pkl",
            download_settle_seconds=30.0,
            skip_download_reconcile=True,
            skip_pending_csv=True,
            trim_queue=False,
        )
        self.assertNotIn("--resume", argv)
        self.assertIn("--download-settle-seconds", argv)
        self.assertIn("30.0", argv)
        self.assertIn("--skip-download-reconcile", argv)
        self.assertIn("--skip-pending-csv", argv)


if __name__ == "__main__":
    unittest.main()
