"""POLISH-02: workspace data/ layout and legacy flat detection."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from slskd_export_paths import (
    default_new_export_path,
    find_latest_spotify_export,
    spotify_export_basename,
)
from slskd_workspace import (
    DATA_DIR_NAME,
    PENDING_RETRY_FILENAME,
    PENDING_VALIDATE_FILENAME,
    archive_csv_dir,
    archive_csv_path,
    copy_to_archive,
    default_workspace,
    export_search_roots,
    exports_dir,
    queue_csv_path,
    remove_ephemeral_pending_csvs,
    resolve_workspace,
)


class TestSlskdWorkspace(unittest.TestCase):
    def test_greenfield_defaults_to_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cwd = Path(tmp)
            self.assertEqual(default_workspace(cwd).resolve(), (cwd / DATA_DIR_NAME).resolve())

    def test_legacy_flat_when_queue_at_cwd(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cwd = Path(tmp)
            (cwd / "to_queue.csv").write_text("artist\n", encoding="utf-8")
            self.assertEqual(default_workspace(cwd).resolve(), cwd.resolve())

    def test_prefers_data_when_both_exist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cwd = Path(tmp)
            data = cwd / DATA_DIR_NAME
            data.mkdir()
            (cwd / "to_queue.csv").write_text("legacy\n", encoding="utf-8")
            (data / "to_queue.csv").write_text("data\n", encoding="utf-8")
            self.assertEqual(default_workspace(cwd).resolve(), data.resolve())

    def test_resolve_workspace_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            explicit = Path(tmp) / "custom"
            explicit.mkdir()
            self.assertEqual(resolve_workspace(explicit).resolve(), explicit.resolve())

    def test_export_search_roots(self) -> None:
        ws = Path("/tmp/ws")
        roots = export_search_roots(ws)
        self.assertEqual(roots[0], exports_dir(ws).resolve())
        self.assertEqual(roots[-1], ws.resolve())


class TestArchiveLayout(unittest.TestCase):
    def test_archive_csv_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ws = Path(tmp)
            path = archive_csv_path(ws, "20260518", "20260518-to_queue.csv")
            self.assertEqual(path.parent.resolve(), archive_csv_dir(ws, "20260518").resolve())
            self.assertTrue(path.parent.is_dir())

    def test_copy_to_archive(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ws = Path(tmp)
            src = ws / "to_queue.csv"
            src.write_text("a\n", encoding="utf-8")
            dest = copy_to_archive(ws, src, date_str="20260518")
            self.assertTrue(dest.is_file())
            self.assertEqual(dest.read_text(encoding="utf-8"), "a\n")


class TestEphemeralPendingCleanup(unittest.TestCase):
    def test_remove_validate_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ws = Path(tmp)
            validate = ws / PENDING_VALIDATE_FILENAME
            retry = ws / PENDING_RETRY_FILENAME
            validate.write_text("v\n", encoding="utf-8")
            retry.write_text("r\n", encoding="utf-8")
            removed = remove_ephemeral_pending_csvs(ws, include_retry_pending=False)
            self.assertEqual(len(removed), 1)
            self.assertFalse(validate.is_file())
            self.assertTrue(retry.is_file())

    def test_remove_both_when_ephemeral(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ws = Path(tmp)
            (ws / PENDING_VALIDATE_FILENAME).write_text("v\n", encoding="utf-8")
            (ws / PENDING_RETRY_FILENAME).write_text("r\n", encoding="utf-8")
            removed = remove_ephemeral_pending_csvs(ws, include_retry_pending=True)
            self.assertEqual(len(removed), 2)


class TestExportPathsWithExportsDir(unittest.TestCase):
    def test_new_export_under_exports(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ws = Path(tmp)
            path, date_str = default_new_export_path(ws, "20260518")
            self.assertEqual(path.parent, exports_dir(ws))
            self.assertEqual(path.name, spotify_export_basename(date_str))

    def test_find_latest_prefers_exports_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ws = Path(tmp)
            ex = exports_dir(ws)
            ex.mkdir(parents=True)
            legacy = ws / "20260510-spotify-export.csv"
            newer = ex / "20260518-spotify-export.csv"
            legacy.write_text("old\n", encoding="utf-8")
            newer.write_text("new\n", encoding="utf-8")
            found = find_latest_spotify_export(ws)
            self.assertEqual(found.resolve(), newer.resolve())

    def test_legacy_export_in_workspace_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ws = Path(tmp)
            path = ws / "20260512-spotify-export.csv"
            path.write_text("x\n", encoding="utf-8")
            self.assertEqual(find_latest_spotify_export(ws).resolve(), path.resolve())


if __name__ == "__main__":
    unittest.main()
