import json

import pytest
import responses

from encompass_to_samsara.samsara_client import SamsaraClient

API = "https://api.samsara.com"


@pytest.fixture
def token_env(monkeypatch):
    monkeypatch.setenv("SAMSARA_API_TOKEN", "test-token")


@pytest.fixture
def client(token_env):
    return SamsaraClient(api_token="test-token")


@pytest.fixture
def tags_payload():
    return {
        "tags": [
            {"id": "1", "name": "ManagedBy:EncompassSync"},
            {"id": "2", "name": "CandidateDelete"},
            {"id": "10", "name": "Austin"},
            {"id": "20", "name": "JECO"},
        ]
    }


@pytest.fixture
def base_responses(tags_payload):
    with responses.RequestsMock() as rsps:
        rsps.assert_all_requests_are_fired = False
        rsps.add(responses.GET, f"{API}/tags", json=tags_payload, status=200)
        yield rsps


@pytest.fixture
def warehouses_csv(tmp_path):
    """Create a minimal warehouses denylist mapping file for sync tests."""

    path = tmp_path / "warehouses.csv"
    if not path.exists():
        path.write_text("samsara_id,name\n", encoding="utf-8")
    return path


@pytest.fixture
def sample_state_file(tmp_path):
    """Write an empty state file to exercise normalization logic."""

    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps({}), encoding="utf-8")
    return state_path


@pytest.fixture
def sample_samsara_address():
    """Baseline Samsara address payload used for diff/patch assertions."""

    return {
        "id": "500",
        "name": "Legacy Store",
        "formattedAddress": "Legacy Address",
        "externalIds": {"EncompassId": "8214", "fingerprint": "legacy-fp"},
        "tagIds": ["1"],
    }
