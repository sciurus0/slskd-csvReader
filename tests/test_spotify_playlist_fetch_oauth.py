"""Tests for Spotify OAuth helpers (Bugbot PR #4 findings)."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import spotify_playlist_fetch as spf


class TestOAuthRedirectHost(unittest.TestCase):
    def test_loopback_host_accepted(self) -> None:
        host, port = spf._redirect_host_port("http://127.0.0.1:8765/callback")
        self.assertEqual(host, "127.0.0.1")
        self.assertEqual(port, 8765)

    def test_localhost_accepted(self) -> None:
        host, port = spf._redirect_host_port("http://localhost:8765/callback")
        self.assertEqual(host, "localhost")
        self.assertEqual(port, 8765)

    def test_non_loopback_rejected(self) -> None:
        with self.assertRaises(SystemExit):
            spf._redirect_host_port("http://0.0.0.0:8765/callback")

    def test_https_rejected(self) -> None:
        with self.assertRaises(SystemExit):
            spf._redirect_host_port("https://127.0.0.1:8765/callback")


class TestRedactSensitiveText(unittest.TestCase):
    def test_redacts_token_fields_in_json(self) -> None:
        body = json.dumps(
            {"error": "bad", "access_token": "secret", "refresh_token": "rt"}
        )
        out = spf._redact_sensitive_text(body, max_len=500)
        self.assertIn("***", out)
        self.assertNotIn("secret", out)


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
                time_mod.time.return_value = 1180.0
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
