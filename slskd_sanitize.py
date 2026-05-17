"""
Single definition of queue CSV field sanitization.

Used when building ``to_queue.csv`` (see ``merge_queue.py``). The download worker
expects rows to be sanitized already and does not repeat this step.

NORM-04 (album / track) — approved transforms only
-------------------------------------------------
Album and track use :func:`sanitize_queue_field` at merge time. There is **no**
semantic normalization (no stripping Remix, Live, feat., soundtrack text, etc.).

Approved changes (Phase A sign-off on DEV export/queue data):

* **Filesystem-safe punctuation** — ``\\ / : * ? " < > |`` → space (e.g. ``UPS / DOWNS`` → ``UPS DOWNS``).
* **Typographic quotes** — Unicode curly quotes/apostrophes → ASCII equivalents; ASCII ``"`` in titles may be removed when matched by the forbidden set (e.g. ``From "American Gods" Soundtrack`` → ``From American Gods Soundtrack``).
* **NFKC unicode** — compatibility normalization (e.g. composite characters); preserves letters such as Arabic script, only normalizes form.
* **Control characters** — removed; whitespace collapsed; trailing ``.``/space trimmed.
* **Empty after cleanup** — becomes ``unnamed`` (rare for album/track).

Explicitly **not** applied to album/track at merge: NORM-02 feat. strip, comma/`;` credit split,
remix/live/version removal, or bracket/parenthetical trimming.

Search-time filename filtering (``should_reject_version``) is separate from merge sanitization.
"""

from __future__ import annotations

import re
import unicodedata


def sanitize_queue_field(name: str, replacement: str = " ") -> str:
    """
    Normalize metadata for stable Soulseek search tokens and safe paths.

    Preserves Unicode letters and semantic tokens (Remix, feat., Live, etc.).
    See module docstring for NORM-04 approved transform categories.
    """
    if not name:
        return ""

    # Typographic quotes / apostrophes → ASCII (Soulseek + spreadsheet-friendly).
    name = (
        name.replace("\u2019", "'")
        .replace("\u2018", "'")
        .replace("\u201c", '"')
        .replace("\u201d", '"')
    )

    name = unicodedata.normalize("NFKC", name)
    name = re.sub(r"[\x00-\x1f\x7f]", "", name)
    forbidden = r'[\/:*?"<>|]'
    name = re.sub(forbidden, replacement, name)
    name = name.rstrip(" .")
    name = re.sub(r"\s+", " ", name)

    if not name.strip():
        return "unnamed"

    return name.strip()
