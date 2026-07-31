from __future__ import annotations

import threading
from typing import Any, Optional

import pytest
import requests

from wb.mqtt_waterius import waterius_api


class _FakeResponse:
    def __init__(self, status_code: int, text: str = "") -> None:
        self.status_code = status_code
        self.text = text


class _FakeSession:
    def __init__(self, response: Optional[_FakeResponse] = None, exc: Optional[Exception] = None) -> None:
        self._response = response
        self._exc = exc
        self.calls = []

    def post(self, url: str, **kwargs: Any) -> Optional[_FakeResponse]:
        self.calls.append((url, kwargs))
        if self._exc:
            raise self._exc
        return self._response


class _SeqSession:
    """
    Returns a queued sequence of responses, one per post() call.
    """

    def __init__(self, responses: list) -> None:
        self._responses = list(responses)
        self.calls = []

    def post(self, url: str, **kwargs: Any) -> _FakeResponse:
        self.calls.append((url, kwargs))
        return self._responses.pop(0)


def _client(session: Any, **knobs: Any) -> waterius_api.WateriusClient:
    """
    Client on a fake session. Kwargs override the transport defaults.
    """
    return waterius_api.WateriusClient(session=session, **knobs)


def test_build_payload_rejects_channel_without_value() -> None:
    # Waterius maps readings by channel number, so a device with a gap would write readings
    # into the wrong slots. The daemon does not send such a device, and the body for it is
    # not built either. The message names the device by its masked key and the channel by
    # its topic, so the journal shows which device and which reading is missing.
    channels = [
        waterius_api.ChannelReading("d/a", 0, None),
        waterius_api.ChannelReading("d/b", 1, 0.2),
    ]
    with pytest.raises(ValueError, match="device 01234: channel d/a"):
        waterius_api.build_payload("0123456789abcdef0123456789abcdef", "Boiler", channels)


def test_send_success() -> None:
    session = _FakeSession(response=_FakeResponse(200, "ok"))
    result = _client(session).send({"key": "K"})
    assert result.ok
    assert result.status_code == 200
    assert session.calls[0][1]["json"] == {"key": "K"}


def test_send_http_error() -> None:
    session = _FakeSession(response=_FakeResponse(403, "forbidden"))
    result = _client(session).send({"key": "K"})
    assert not result.ok
    assert result.status_code == 403


def test_send_network_error() -> None:
    session = _FakeSession(exc=requests.ConnectionError("no route"))
    result = _client(session, backoff=0).send({"key": "K"})
    assert not result.ok
    assert "no route" in result.error
    assert len(session.calls) == waterius_api.DEFAULT_RETRIES  # retried


def test_send_retries_503_then_succeeds() -> None:
    session = _SeqSession([_FakeResponse(503, "busy"), _FakeResponse(200, "")])
    result = _client(session, backoff=0).send({"key": "K"})
    assert result.ok
    assert result.status_code == 200
    assert len(session.calls) == 2  # first 503, second OK


def test_send_503_exhausted_reports_failure() -> None:
    session = _SeqSession([_FakeResponse(503, "busy")] * 3)
    result = _client(session, retries=3, backoff=0).send({"key": "K"})
    assert not result.ok
    assert result.status_code == 503
    assert len(session.calls) == 3


def test_send_stop_aborts_retries() -> None:
    # An already-set stop event (the daemon shutting down) aborts the remaining retries
    # instead of waiting through the backoff, so shutdown stays responsive.
    session = _SeqSession([_FakeResponse(503, "busy")] * 3)
    stop = threading.Event()
    stop.set()
    result = _client(session, retries=3).send({"key": "K"}, stop=stop)
    assert not result.ok
    assert result.status_code == 503
    assert len(session.calls) == 1  # stopped after the first attempt's backoff


def test_send_400_not_retried() -> None:
    session = _SeqSession([_FakeResponse(400, '["Incorrect fields: key"]'), _FakeResponse(200, "")])
    result = _client(session).send({"key": ""})
    assert not result.ok
    assert result.status_code == 400
    assert len(session.calls) == 1  # non-retryable, stops immediately


def test_send_404_reports_key_not_found() -> None:
    # Waterius returns 404 when the key resolves to no device (invalid/revoked). It is a
    # hard error surfaced with a clear message, and not retried.
    session = _SeqSession([_FakeResponse(404, ""), _FakeResponse(200, "")])
    result = _client(session).send({"key": "bogus"})
    assert not result.ok
    assert result.status_code == 404
    assert result.error == waterius_api.KEY_NOT_FOUND_ERROR
    assert len(session.calls) == 1  # non-retryable, stops immediately
