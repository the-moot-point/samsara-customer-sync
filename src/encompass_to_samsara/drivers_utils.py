from __future__ import annotations

import re
import unicodedata

_MAX_USERNAME_LENGTH = 189
_ALLOWED_CHARACTERS_RE = re.compile(r"[^a-z0-9]")


def _slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    return _ALLOWED_CHARACTERS_RE.sub("", ascii_value.lower())


def generate_username(first: str, last: str, taken: set[str]) -> str:
    """Generate a unique username for a driver.

    The username is formed from the first initial and last name, stripped to
    lowercase ASCII letters and digits. When collisions occur, a numeric suffix
    is added while ensuring the resulting username does not exceed the maximum
    allowed length.
    """

    first_slug = _slugify(first)
    last_slug = _slugify(last)

    base = f"{first_slug[:1]}{last_slug}"

    suffix = 1
    while True:
        suffix_str = f"-{suffix}"
        max_base_length = _MAX_USERNAME_LENGTH - len(suffix_str)
        truncated_base = base[:max_base_length] if max_base_length > 0 else ""
        candidate = f"{truncated_base}{suffix_str}"
        if candidate not in taken:
            return candidate
        suffix += 1
