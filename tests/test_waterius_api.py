import json
import threading
from dataclasses import dataclass
from typing import Any, Optional

import pytest
import requests

from wb.mqtt_waterius import waterius_api


@dataclass
class _FakeResponse:
    status_code: int
    text: str = ""

    def json(self) -> Any:
        # Like requests, a body that is not JSON raises a ValueError subclass.
        return json.loads(self.text)


class _FakeSession:
    def __init__(self, response: Optional[_FakeResponse] = None, exc: Optional[Exception] = None) -> None:
        self._response = response
        self._exc = exc
        self.calls = []
        self.closed = False

    def post(self, url: str, **kwargs: Any) -> Optional[_FakeResponse]:
        self.calls.append((url, kwargs))
        if self._exc:
            raise self._exc
        return self._response

    def close(self) -> None:
        self.closed = True


class _SeqSession:  # pylint: disable=too-few-public-methods
    """
    Returns a queued sequence of responses, one per post() call.

    The last response repeats once the queue runs dry, so a test queues the responses its
    scenario needs without sizing the queue to the number of attempts the client makes.
    """

    def __init__(self, responses: list) -> None:
        self._responses = list(responses)
        self.calls = []

    def post(self, url: str, **kwargs: Any) -> _FakeResponse:
        self.calls.append((url, kwargs))
        if len(self._responses) > 1:
            return self._responses.pop(0)
        return self._responses[0]


def _client(session: Any, **knobs: Any) -> waterius_api.WateriusClient:
    """
    Client on a fake session. Kwargs override the transport defaults.

    The client owns its session and takes no injection, so the fake replaces the private
    field on purpose.
    """
    client = waterius_api.WateriusClient(**knobs)
    client._session = session  # pylint: disable=protected-access
    return client


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
    assert len(session.calls) == waterius_api.DEFAULT_MAX_ATTEMPTS  # retried


def test_send_retries_503_then_succeeds() -> None:
    session = _SeqSession([_FakeResponse(503, "busy"), _FakeResponse(200, "")])
    result = _client(session, backoff=0).send({"key": "K"})
    assert result.ok
    assert result.status_code == 200
    assert len(session.calls) == 2  # first 503, second OK


def test_send_503_exhausted_reports_failure() -> None:
    session = _SeqSession([_FakeResponse(503, "busy")])
    result = _client(session, max_attempts=3, backoff=0).send({"key": "K"})
    assert not result.ok
    assert result.status_code == 503
    assert len(session.calls) == 3


def test_send_stop_aborts_retries() -> None:
    # An already-set stop event (the daemon shutting down) aborts the remaining retries
    # instead of waiting through the backoff, so shutdown stays responsive.
    session = _SeqSession([_FakeResponse(503, "busy")])
    stop_event = threading.Event()
    stop_event.set()
    result = _client(session, max_attempts=3).send({"key": "K"}, stop_event=stop_event)
    assert not result.ok
    assert result.status_code == 503
    assert len(session.calls) == 1  # stopped after the first attempt's backoff


def test_send_400_not_retried() -> None:
    session = _SeqSession([_FakeResponse(400, '["Incorrect fields: key"]'), _FakeResponse(200, "")])
    result = _client(session).send({"key": ""})
    assert not result.ok
    assert result.status_code == 400
    assert len(session.calls) == 1  # non-retryable, stops immediately


def test_send_400_reports_server_explanation() -> None:
    # The body names the offending fields, which is more useful than the status code alone.
    session = _SeqSession([_FakeResponse(400, '"Incorrect fields: serial0"')])
    result = _client(session).send({"key": ""})
    assert not result.ok
    assert result.status_code == 400
    assert result.error == "Incorrect fields: serial0"


def test_send_ignores_a_body_that_is_not_json() -> None:
    session = _SeqSession([_FakeResponse(500, "<html>500 Internal Server Error</html>")])
    result = _client(session).send({"key": ""})
    assert result.status_code == 500
    assert result.error is None  # the UI falls back to the status code


def test_send_404_reports_key_not_found() -> None:
    # Waterius returns 404 when the key resolves to no device (invalid/revoked). It is a
    # hard error surfaced with a clear message, and not retried.
    session = _SeqSession([_FakeResponse(404, ""), _FakeResponse(200, "")])
    result = _client(session).send({"key": "bogus"})
    assert not result.ok
    assert result.status_code == 404
    assert result.error == waterius_api.KEY_NOT_FOUND_ERROR
    assert len(session.calls) == 1  # non-retryable, stops immediately


def test_client_releases_its_session_on_exit() -> None:
    # The context manager is the intended entry point: the session goes out with the batch.
    session = _FakeSession(response=_FakeResponse(200))
    with _client(session) as client:
        assert client.send({"key": "K"}).ok
    assert session.closed


def test_client_releases_its_session_on_exception() -> None:
    # A batch that blows up must not leak the pool — that is what the context manager is for.
    session = _FakeSession(response=_FakeResponse(200))
    with pytest.raises(RuntimeError):
        with _client(session):
            raise RuntimeError("batch blew up")
    assert session.closed
