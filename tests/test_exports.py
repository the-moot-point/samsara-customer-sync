import json

from encompass_to_samsara.samsara_client import SamsaraClient
from encompass_to_samsara.scripts import export_addresses, export_tags


def test_export_tags(monkeypatch, tmp_path):
    sample_tags = [
        {"id": "1", "name": "Alpha"},
        {"id": "2", "name": "Bravo"},
    ]

    monkeypatch.setattr(SamsaraClient, "list_tags", lambda self, limit=512: sample_tags)
    monkeypatch.chdir(tmp_path)

    export_tags.main()

    out_file = tmp_path / "tags.json"
    with out_file.open("r", encoding="utf-8") as f:
        data = json.load(f)

    assert data == sample_tags
    assert all("name" in t and ("id" in t or "tagId" in t) for t in data)


def test_export_addresses(monkeypatch, tmp_path):
    sample_addresses = [
        {
            "id": "100",
            "name": "Foo",
            "formattedAddress": "123 A St",
            "externalIds": {"encompass_id": "E1"},
            "tagIds": ["1"],
        },
        {
            "id": "200",
            "name": "Bar",
            "formattedAddress": "456 B St",
            "externalIds": {"encompass_id": "E2"},
            "tagIds": ["2"],
        },
    ]

    monkeypatch.setattr(SamsaraClient, "list_addresses", lambda self, limit=512: sample_addresses)
    monkeypatch.setenv("SAMSARA_BEARER_TOKEN", "dummy")
    monkeypatch.chdir(tmp_path)

    export_addresses.main()

    out_file = tmp_path / "addresses.json"
    with out_file.open("r", encoding="utf-8") as f:
        data = json.load(f)

    assert data == sample_addresses
    for addr in data:
        assert {"id", "name", "formattedAddress"}.issubset(addr.keys())
        assert isinstance(addr.get("externalIds"), dict)
