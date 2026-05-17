"""Tests for Spotify OAuth helpers (Bugbot PR #4 findings)."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import spotify_playlist_fetch as spf


class TestOAuthCallbackPath(unittest.TestCase):
    def test_callback_path_from_redirect_uri(self) -> None:
        self.assertEqual(
            spf._oauth_callback_path("http://127.0.0.1:8765/callback"),
            "/callback",
        )

    def test_root_redirect_uses_slash(self) -> None:
        self.assertEqual(spf._oauth_callback_path("http://127.0.0.1:8765/"), "/")


class TestOAuthCallbackResult(unittest.TestCase):
    def test_stray_path_is_ignored(self) -> None:
        self.assertIsNone(
            spf._oauth_callback_result(
                "/favicon.ico",
                "/callback",
                "state",
            )
        )

    def test_valid_callback_returns_code(self) -> None:
        parsed = spf._oauth_callback_result(
            "/callback?code=abc&state=state",
            "/callback",
            "state",
        )
        self.assertIsNotNone(parsed)
        updates, _body = parsed
        self.assertEqual(updates, {"code": "abc"})


class TestEnsureUserAccessTokenExpiry(unittest.TestCase):
    def test_expires_at_set_after_interactive_login(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            token_path = Path(tmp) / "tokens.json"
            times = iter([1000.0, 1180.0])

            with (
                mock.patch.object(spf, "time") as time_mod,
                mock.patch.object(
                    spf, "interactive_authorize", return_value="auth-code"
                ),
                mock.patch.object(
                    spf,
                    "_exchange_code_for_tokens",
                    return_value={
                        "access_token": "at",
                        "refresh_token": "rt",
                        "expires_in": 3600,
                    },
                ),
            ):
                time_mod.time.side_effect = lambda: next(times)
                token = spf.ensure_user_access_token(
                    "client",
                    None,
                    "http://127.0.0.1:8765/callback",
                    token_path,
                    no_browser=True,
                )

            self.assertEqual(token, "at")
            saved = json.loads(token_path.read_text(encoding="utf-8"))
            self.assertEqual(saved["expires_at"], 1180.0 + 3600)


class TestSaveJsonCache(unittest.TestCase):
    def test_token_and_throttle_use_shared_writer(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "cache.json"
            spf._save_token_cache(path, {"a": 1})
            spf._save_throttle_cache(path, {"b": 2})
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), {"b": 2})


if __name__ == "__main__":
    unittest.main()
