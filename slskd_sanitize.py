"""
Single definition of queue CSV field sanitization.

Used when building ``to_queue.csv`` (see ``merge_queue.py``). The download worker
expects rows to be sanitized already and does not repeat this step.
"""

from __future__ import annotations

import re
import unicodedata


def sanitize_queue_field(name: str, replacement: str = " ") -> str:
    """
    Normalize metadata for stable Soulseek search tokens and safe paths.

    Preserves Unicode letters/symbols where possible; strips control chars and
    filesystem-forbidden characters (Windows-oriented set).
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
