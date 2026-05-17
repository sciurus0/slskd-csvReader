"""RUN-03: pipeline plan and dry-run stage descriptions."""

from __future__ import annotations

import unittest
from pathlib import Path

from run_pipeline import (
    StageAction,
    build_pipeline_plan,
    format_pipeline_plan,
)


class TestRunPipelinePlan(unittest.TestCase):
    def test_full_run_plan(self) -> None:
        ws = Path("/tmp/ws")
        stages = build_pipeline_plan(
            slskd_only=False,
            resume=False,
            dry_run=False,
            skip_slskd=False,
            export_path=ws / "20260517-spotify-export.csv",
            queue_path=ws / "to_queue.csv",
            force_full_import=False,
        )
        self.assertEqual(len(stages), 3)
        self.assertEqual(stages[0].action, StageAction.RUN)
        self.assertEqual(stages[1].action, StageAction.RUN)
        self.assertEqual(stages[2].action, StageAction.RUN)
        self.assertIn("to_queue.csv", stages[1].writes[1])

    def test_dry_run_merge_preview_only(self) -> None:
        ws = Path("/tmp/ws")
        stages = build_pipeline_plan(
            slskd_only=False,
            resume=False,
            dry_run=True,
            skip_slskd=False,
            export_path=ws / "export.csv",
            queue_path=ws / "to_queue.csv",
            force_full_import=False,
        )
        self.assertEqual(stages[0].action, StageAction.RUN)
        self.assertEqual(stages[1].action, StageAction.PREVIEW)
        self.assertEqual(stages[1].writes, ())
        self.assertEqual(stages[2].action, StageAction.SKIP)

    def test_resume_slskd_only(self) -> None:
        stages = build_pipeline_plan(
            slskd_only=True,
            resume=True,
            dry_run=False,
            skip_slskd=False,
            export_path=Path("/tmp/export.csv"),
            queue_path=Path("/tmp/to_queue.csv"),
            force_full_import=False,
        )
        self.assertEqual(stages[0].action, StageAction.SKIP)
        self.assertEqual(stages[2].action, StageAction.RUN)
        self.assertIn("resume", stages[2].title)

    def test_format_includes_dry_run_mode_line(self) -> None:
        ws = Path("/tmp/ws")
        stages = build_pipeline_plan(
            slskd_only=False,
            resume=False,
            dry_run=True,
            skip_slskd=False,
            export_path=ws / "export.csv",
            queue_path=ws / "to_queue.csv",
            force_full_import=True,
        )
        lines = format_pipeline_plan(
            workspace=ws,
            stages=stages,
            dry_run=True,
            force_full_import=True,
        )
        joined = "\n".join(lines)
        self.assertIn("dry-run", joined)
        self.assertIn("force-full-import", joined)
        self.assertIn("preview (no writes)", joined)


if __name__ == "__main__":
    unittest.main()
