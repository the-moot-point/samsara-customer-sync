import os
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
    return {"tags": [
        {"id": "1", "name": "ManagedBy:EncompassSync"},
        {"id": "2", "name": "CandidateDelete"},
        {"id": "10", "name": "Austin"},
        {"id": "20", "name": "JECO"},
    ]}

@pytest.fixture
def base_responses(tags_payload):
    with responses.RequestsMock() as rsps:
        rsps.assert_all_requests_are_fired = False
        rsps.add(responses.GET, f"{API}/tags", json=tags_payload, status=200)
        yield rsps
