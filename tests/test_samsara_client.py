import json
import logging
import re
from unittest.mock import Mock

import pytest
import requests


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


def test_list_addresses_cursor_pagination(monkeypatch, client):
    r1 = make_response(
        200,
        {
            "data": [{"id": "1"}, {"id": "2"}],
            "pagination": {"hasNextPage": True, "endCursor": "abc"},
        },
    )
    r2 = make_response(
        200,
        {
            "data": [{"id": "3"}],
            "pagination": {"hasNextPage": False, "endCursor": None},
        },
    )

    mock_req = Mock(side_effect=[r1, r2])
    monkeypatch.setattr(requests.Session, "request", mock_req)

    items = client.list_addresses(limit=2)

    assert items == [{"id": "1"}, {"id": "2"}, {"id": "3"}]
    assert mock_req.call_count == 2
    assert mock_req.call_args_list[1][1]["params"]["after"] == "abc"


def test_create_address_error_logging(monkeypatch, client, caplog):
    payload = {"name": "foo"}
    resp = make_response(400, {"message": "Invalid address", "requestId": "req-123"})
    mock_req = Mock(return_value=resp)
    monkeypatch.setattr(client, "request", mock_req)

    with caplog.at_level(logging.ERROR):
        with pytest.raises(requests.HTTPError) as exc:
            client.create_address(payload)

    msg = str(exc.value)
    assert "Invalid address" in msg
    assert "req-123" in msg
    # ensure payload logged
    assert any("payload={'name': 'foo'}" in record.getMessage() for record in caplog.records)


def test_create_address_with_sanitized_external_ids(monkeypatch, client):
    from encompass_to_samsara.transform import clean_external_ids

    raw_payload = {"name": "foo", "externalIds": {"bad$key": "v!"}}
    payload = raw_payload.copy()
    payload["externalIds"] = clean_external_ids(raw_payload["externalIds"])
    resp = make_response(200, {"id": "123"})
    mock_req = Mock(return_value=resp)
    monkeypatch.setattr(client, "request", mock_req)

    result = client.create_address(payload)

    assert result == {"id": "123"}
    sent = mock_req.call_args[1]["json_body"]
    allowed = re.compile(r"^[A-Za-z0-9_.:-]+$")
    assert all(allowed.match(k) for k in sent["externalIds"])
    assert all(allowed.match(v) for v in sent["externalIds"].values())
