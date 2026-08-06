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
            self.assertIn(str(ws.resolve()), cmd)

    def test_reconcile_and_trim(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ws = Path(tmp)
            with mock.patch.dict(os.environ, {"CSVREADER_WORKSPACE": str(ws)}, clear=False):
                rec = runner.build_reconcile_command()
                trim = runner.build_trim_command()
            self.assertIn("--reconcile-downloads", rec)
            self.assertTrue(any(p.endswith("trim_queue.py") for p in trim))


if __name__ == "__main__":
    unittest.main()
