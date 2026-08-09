"""CFG / SEC: local config loader — base_url, config.ini vs api.txt, permissions."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import slskd_config


class TestEnsureSlskdApiKey(unittest.TestCase):
    def test_missing_key_exits(self) -> None:
        with mock.patch.object(slskd_config, "read_slskd_api_key", return_value=""):
            with self.assertRaises(SystemExit) as ctx:
                slskd_config.ensure_slskd_api_key()
        self.assertEqual(ctx.exception.code, 1)

    def test_placeholder_exits(self) -> None:
        with mock.patch.object(
            slskd_config,
            "read_slskd_api_key",
            return_value=slskd_config.SLSKD_API_KEY_PLACEHOLDER,
        ):
            with self.assertRaises(SystemExit):
                slskd_config.ensure_slskd_api_key()

    def test_env_key_accepted(self) -> None:
        with mock.patch.dict(os.environ, {"SLSKD_API_KEY": "real-key"}, clear=True):
            slskd_config.reset_local_config_cache()
            self.assertEqual(slskd_config.ensure_slskd_api_key(), "real-key")


class TestApiTxtPermissions(unittest.TestCase):
    def test_warns_on_world_readable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "api.txt"
            path.write_text("[slskd]\napi_key = x\n", encoding="utf-8")
            path.chmod(0o644)
            with mock.patch("sys.stderr") as err:
                slskd_config._warn_api_txt_permissions(path)
            err.write.assert_called()


class TestSlskdBaseUrl(unittest.TestCase):
    def tearDown(self) -> None:
        slskd_config.reset_local_config_cache()

    def test_default_localhost(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            with mock.patch.object(slskd_config, "load_local_config", return_value={}):
                slskd_config.reset_local_config_cache()
                self.assertEqual(
                    slskd_config.read_slskd_base_url(),
                    "http://localhost:5030",
                )

    def test_env_overrides_file(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"SLSKD_BASE_URL": "http://192.168.0.245:5030/"},
            clear=True,
        ):
            with mock.patch.object(
                slskd_config,
                "load_local_config",
                return_value={"slskd_base_url": "http://localhost:5030"},
            ):
                slskd_config.reset_local_config_cache()
                self.assertEqual(
                    slskd_config.read_slskd_base_url(),
                    "http://192.168.0.245:5030",
                )

    def test_config_ini_base_url_and_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.ini"
            path.write_text(
                "[slskd]\n"
                "api_key = k\n"
                "base_url = http://nas:5030/\n"
                "api_path = api/v0\n",
                encoding="utf-8",
            )
            parsed = slskd_config.load_local_config(path)
            self.assertEqual(parsed["slskd_base_url"], "http://nas:5030")
            self.assertEqual(parsed["slskd_api_path"], "/api/v0")
            self.assertEqual(parsed["slskd_api_key"], "k")

    def test_config_ini_preferred_over_api_txt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "config.ini").write_text(
                "[slskd]\napi_key = from-ini\nbase_url = http://from-ini:5030\n",
                encoding="utf-8",
            )
            (root / "api.txt").write_text(
                "[slskd]\napi_key = from-txt\nbase_url = http://from-txt:5030\n",
                encoding="utf-8",
            )
            with mock.patch.object(slskd_config, "_repo_root", return_value=root):
                slskd_config.reset_local_config_cache()
                cfg = slskd_config.load_local_config()
            self.assertEqual(cfg["slskd_api_key"], "from-ini")
            self.assertEqual(cfg["slskd_base_url"], "http://from-ini:5030")


if __name__ == "__main__":
    unittest.main()
