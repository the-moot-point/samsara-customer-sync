import json
from unittest.mock import Mock

import pytest
import requests

from encompass_to_samsara.samsara_client import SamsaraClient


def make_response(status_code: int, json_body=None, headers=None):
    resp = requests.Response()
    resp.status_code = status_code
    resp.headers = headers or {}
    if json_body is not None:
        resp._content = json.dumps(json_body).encode("utf-8")
    else:
        resp._content = b""
    return resp


def test_request_retries_on_429(monkeypatch, client):
    r1 = make_response(429)
    r2 = make_response(200, {"ok": True})

    mock_req = Mock(side_effect=[r1, r2])
    monkeypatch.setattr(requests.Session, "request", mock_req)

    sleep_mock = Mock()
    monkeypatch.setattr("time.sleep", sleep_mock)
    monkeypatch.setattr("random.random", lambda: 0.0)

    resp = client.request("GET", "/foo")

    assert resp is r2
    assert mock_req.call_count == 2
    sleep_mock.assert_called_once()
    assert sleep_mock.call_args[0][0] == pytest.approx(client.retry.base_delay)
