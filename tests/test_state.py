from encompass_to_samsara.state import DEFAULT_STATE, load_state, save_state


def test_load_state_missing_file_returns_default(tmp_path):
    path = tmp_path / "state.json"
    assert load_state(str(path)) == DEFAULT_STATE

def test_load_state_invalid_json_returns_default(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text("{ not valid json", encoding="utf-8")
    assert load_state(str(path)) == DEFAULT_STATE

def test_save_and_load_state_roundtrip(tmp_path):
    path = tmp_path / "nested" / "state.json"
    state = {"fingerprints": {"1": "fp"}, "candidate_deletes": {"2": "del"}}
    save_state(str(path), state)
    assert load_state(str(path)) == state
