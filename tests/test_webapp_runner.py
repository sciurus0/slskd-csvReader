"""Web UI runner helpers (no live SLSKD)."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from webapp import runner


class TestRunnerCommands(unittest.TestCase):
    def test_pipeline_command_includes_saved_and_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ws = Path(tmp)
            with mock.patch.dict(os.environ, {"CSVREADER_WORKSPACE": str(ws)}, clear=False):
                cmd = runner.build_pipeline_command(dry_run=True)
            self.assertIn("--saved", cmd)
            self.assertIn("-y", cmd)
            self.assertIn("--dry-run", cmd)
            self.assertIn("--no-browser", cmd)
            self.assertIn(str(ws.resolve()), cmd)

    def test_pipeline_options_pick_and_flags(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ws = Path(tmp)
            opts = runner.PipelineOptions(
                selection="pick",
                pick="1,4",
                force_full_import=True,
                continue_on_export_error=True,
                skip_slskd=True,
                date="20260101",
            )
            with mock.patch.dict(os.environ, {"CSVREADER_WORKSPACE": str(ws)}, clear=False):
                cmd = runner.build_pipeline_command(options=opts)
            self.assertIn("--pick", cmd)
            self.assertIn("1,4", cmd)
            self.assertIn("--force-full-import", cmd)
            self.assertIn("--continue-on-export-error", cmd)
            self.assertIn("--skip-slskd", cmd)
            self.assertIn("--date", cmd)
            self.assertIn("20260101", cmd)
            self.assertNotIn("--saved", cmd)

    def test_pipeline_saved_indices(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ws = Path(tmp)
            opts = runner.PipelineOptions(selection="saved_indices", saved_indices="2,3")
            with mock.patch.dict(os.environ, {"CSVREADER_WORKSPACE": str(ws)}, clear=False):
                cmd = runner.build_pipeline_command(options=opts)
            i = cmd.index("--saved")
            self.assertEqual(cmd[i + 1], "2,3")

    def test_pipeline_pick_requires_indices(self) -> None:
        with self.assertRaises(ValueError):
            runner.build_pipeline_command(
                options=runner.PipelineOptions(selection="pick", pick="")
            )

    def test_resume_and_slskd_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ws = Path(tmp)
            with mock.patch.dict(os.environ, {"CSVREADER_WORKSPACE": str(ws)}, clear=False):
                resume = runner.build_pipeline_command(
                    options=runner.PipelineOptions(selection="resume")
                )
                only = runner.build_pipeline_command(
                    options=runner.PipelineOptions(selection="slskd_only")
                )
            self.assertIn("--resume", resume)
            self.assertIn("--slskd-only", only)

    def test_reconcile_and_trim(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ws = Path(tmp)
            with mock.patch.dict(os.environ, {"CSVREADER_WORKSPACE": str(ws)}, clear=False):
                rec = runner.build_reconcile_command(reconcile_from_csv="/tmp/r.csv")
                trim = runner.build_trim_command(dry_run=True, no_backup=True)
            self.assertIn("--reconcile-downloads", rec)
            self.assertIn("--reconcile-from-csv", rec)
            self.assertIn("--dry-run", trim)
            self.assertIn("--no-backup", trim)

    def test_merge_cleanup_slskd(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ws = Path(tmp)
            with mock.patch.dict(os.environ, {"CSVREADER_WORKSPACE": str(ws)}, clear=False):
                merge = runner.build_merge_command(dry_run=True, force_full_import=True)
                cleanup = runner.build_cleanup_command(ephemeral=True)
                slskd = runner.build_slskd_command(
                    retry_failed=True,
                    tuning=runner.SlskdTuning(batch_size=5, debug=True),
                )
            self.assertTrue(any(p.endswith("merge_queue.py") for p in merge))
            self.assertIn("--force-full-import", merge)
            self.assertIn("--ephemeral", cleanup)
            self.assertIn("--retry-failed", slskd)
            self.assertIn("--batch-size", slskd)
            self.assertIn("5", slskd)
            self.assertIn("--debug", slskd)


if __name__ == "__main__":
    unittest.main()
