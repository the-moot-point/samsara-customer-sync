from encompass_to_samsara.tags import build_tag_index, resolve_tag_id


def test_build_tag_index_and_resolve(monkeypatch, client):
    sample_tags = [
        {"id": "1", "name": "Alpha"},
        {"tagId": "2", "name": "Bravo"},
        {"id": "3", "name": "CHARLIE"},
        {"id": "4", "name": ""},           # invalid name
        {"id": "", "name": "Delta"},       # invalid id
        {"tagId": None, "name": "Echo"},    # invalid id
        {"id": "5", "name": None},         # invalid name
    ]

    monkeypatch.setattr(client, "list_tags", lambda: sample_tags)

    index = build_tag_index(client)

    assert index == {"alpha": "1", "bravo": "2", "charlie": "3"}

    assert resolve_tag_id(index, "Alpha") == "1"
    assert resolve_tag_id(index, "BRAVO") == "2"
    assert resolve_tag_id(index, "delta") is None
    assert resolve_tag_id(index, "") is None

