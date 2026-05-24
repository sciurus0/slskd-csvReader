"""SEC-03/04: SLSKD API key and api.txt permission helpers."""

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
            slskd_config._API_TXT_CACHE.clear()
            slskd_config._API_TXT_LOADED = False
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


if __name__ == "__main__":
    unittest.main()
