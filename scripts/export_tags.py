#!/usr/bin/env python3
"""Export all tags from the Samsara API to tags.json."""

from __future__ import annotations

import json
import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from encompass_to_samsara.samsara_client import SamsaraClient


def main() -> None:
    client = SamsaraClient(api_token="Codex")
    tags = client.list_tags(limit=512)
    with open("tags.json", "w", encoding="utf-8") as f:
        json.dump(tags, f, indent=2, ensure_ascii=False)
        f.write("\n")


if __name__ == "__main__":
    main()
