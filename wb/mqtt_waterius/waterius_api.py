"""
Waterius universal API — build payloads and send readings.

Protocol: POST JSON {"ch0": <value>, "data_type0": <type>,
..., "key": <token>}. One key = one device = up to 4 channels (ch0..ch3).
"""

import logging
import threading
from dataclasses import dataclass
from types import TracebackType
from typing import Optional

import requests

DEFAULT_ENDPOINT = "https://uc.waterius.ru"

# Transport defaults of the client below.
DEFAULT_TIMEOUT = 15  # seconds, per request
DEFAULT_MAX_ATTEMPTS = 3  # requests per send, retries included
DEFAULT_BACKOFF = 2.0  # seconds before the next attempt, multiplied by the attempt number

# Transient status codes worth retrying. The endpoint sits behind nginx that returns 503
# under a burst (empirically confirmed rate limiting, not a real error). 429/502/504
# are the usual transient companions.
RETRYABLE_STATUS_CODES = {429, 502, 503, 504}

# Waterius returns 404 when the key resolves to no registered device (an invalid or
# revoked key). A hard error, not transient — surface it clearly and do not retry.
KEY_NOT_FOUND_CODE = 404
KEY_NOT_FOUND_ERROR = "Key not accepted by Waterius"

# The body of a rejected request names the offending fields. Trimmed to stay readable in
# a UI control.
MAX_ERROR_LENGTH = 100

logger = logging.getLogger(__name__)


def mask_key(key: str) -> str:
    """
    Mask a Waterius key for logs and device titles.

    A key is a cloud write credential, so it is never shown in full. Keys are 32-character
    tokens and mask to their first 5 characters.

    Args:
        key: Waterius device key, non-empty — the config rejects a device without one

    Returns:
        First characters of the key

    Examples:
        >>> mask_key("0123456789abcdef0123456789abcdef")
        '01234'
    """
    return key[:5]


@dataclass(frozen=True)
class ChannelData:
    """
    One channel of the payload — its reading, meter type and serial number.

    Attributes:
        source: source control as `<device>/<control>`, names the channel in error messages
        data_type: Waterius data-type code, becomes data_type<N> in the body
        value: the reading itself, None when the value has not arrived yet
        serial: meter serial number, becomes serial<N> when set
    """

    source: str
    data_type: int
    value: Optional[float]
    serial: Optional[str] = None


@dataclass(repr=False)
class SendResult:
    """
    Outcome of a single POST to Waterius.

    Attributes:
        ok: whether Waterius accepted the request
        status_code: HTTP status of the last attempt, None when no response arrived
        error: failure reason, None when the request succeeded or the status speaks for itself
    """

    ok: bool
    status_code: Optional[int] = None
    error: Optional[str] = None

    def __repr__(self) -> str:
        if self.ok:
            return f"SendResult(ok, http={self.status_code})"
        return f"SendResult(failed, http={self.status_code}, error={self.error!r})"


def build_payload(key: str, name: str, channels: list[ChannelData]) -> dict[str, object]:
    """
    Build the JSON body for one Waterius device.

    Waterius maps readings by channel number, so a device with a gap would write its
    readings into the wrong slots of the cabinet. Every channel must therefore carry a
    value, and one without a value is an error rather than something to skip. Keeping the
    device out of the send is the caller's job.

    Args:
        key: Waterius device key
        name: device name for the Waterius cabinet, may be empty
        channels: readings to send, in ch0..ch3 order

    Returns:
        Request body as {"key": ..., "name": ..., "ch<N>": value, "data_type<N>": type,
        "serial<N>": ...}

    Raises:
        ValueError: a channel has no value

    Examples:
        >>> build_payload("KEY", "Boiler", [ChannelData("d/c", 0, 0.1)])
        {'key': 'KEY', 'name': 'Boiler', 'ch0': 0.1, 'data_type0': 0}

        An empty name keeps the name in the cabinet, and a channel serial is optional.

        >>> build_payload("K", "", [ChannelData("d/a", 0, 0.1, "1001"),
        ...                         ChannelData("d/b", 1, 0.2)])
        {'key': 'K', 'name': '', 'ch0': 0.1, 'data_type0': 0, 'serial0': '1001', 'ch1': 0.2, 'data_type1': 1}
    """
    payload: dict[str, object] = {"key": key, "name": name}
    for index, channel in enumerate(channels):
        if channel.value is None:
            raise ValueError(f"device {mask_key(key)}: channel {channel.source} has no value")
        payload[f"ch{index}"] = channel.value
        payload[f"data_type{index}"] = channel.data_type
        if channel.serial:
            payload[f"serial{index}"] = str(channel.serial)
    return payload


def _response_error(response: requests.Response) -> Optional[str]:
    """
    Failure reason from the body of a rejected response, None when the body says nothing.

    Examples:
        >>> class _Rejected:  # stands in for requests.Response
        ...     text = '["Incorrect fields: serial0"]'
        ...     def json(self):
        ...         return ["Incorrect fields: serial0"]
        >>> _response_error(_Rejected())
        'Incorrect fields: serial0'
    """
    try:
        body = response.json()
    except ValueError:
        return None  # an error page of the proxy in front of Waterius, not an explanation
    if isinstance(body, list):
        body = ", ".join(str(item) for item in body)
    elif not isinstance(body, str):
        body = response.text
    return body.strip()[:MAX_ERROR_LENGTH] or None


class WateriusClient:
    """
    Client of the Waterius universal API.

    Owns one requests session, so devices sent one after another reuse the connection
    instead of a TLS handshake per key. Use as a context manager, one client per send
    batch, the session it owns lives and dies with it.

    Args:
        endpoint: Waterius universal API URL
        timeout: per-request timeout in seconds
        max_attempts: how many requests to make before giving up
        backoff: seconds before the next attempt, multiplied by the attempt number
    """

    # Transport knobs with sane defaults, overridden only by tests.
    def __init__(
        self,
        endpoint: str = DEFAULT_ENDPOINT,
        timeout: int = DEFAULT_TIMEOUT,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        backoff: float = DEFAULT_BACKOFF,
    ) -> None:
        self.endpoint = endpoint
        self.timeout = timeout
        self.max_attempts = max_attempts
        self.backoff = backoff
        # A session opens no socket: the connection is made on the first request.
        self._session = requests.Session()

    def __enter__(self) -> "WateriusClient":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.close()

    def close(self) -> None:
        """
        Release the pooled connections. The client is unusable afterwards.
        """
        self._session.close()

    def send(self, payload: dict[str, object], stop_event: Optional[threading.Event] = None) -> SendResult:
        """
        POST one device's readings to Waterius.

        Never raises on a transport failure. Transient ones (nginx 503, 429/502/504, network
        errors) are retried with a linear backoff. A non-retryable HTTP error returns at once
        with the server's own explanation, and a 404 means the key is not registered (invalid
        or revoked).

        Args:
            payload: request body from build_payload
            stop_event: caller's shutdown event. The backoff waits on it instead of sleeping,
                so a shutdown aborts the remaining attempts at once

        Returns:
            Outcome of the last attempt, with the status code and the failure reason when known
        """
        # Callers without a shutdown flag get a never-set event: waiting on it is a plain sleep.
        stop_event = stop_event or threading.Event()
        max_attempts = self.max_attempts
        # Holds the most recent failure, so the caller gets the real reason once the attempts run
        # out. Only the initial value survives if max_attempts is 0.
        last_failure = SendResult(ok=False, error="no attempt made")
        for attempt in range(1, max_attempts + 1):
            try:
                response = self._session.post(
                    self.endpoint,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                logger.warning("Send failed (attempt %d/%d): %s", attempt, max_attempts, exc)
                last_failure = SendResult(ok=False, error=str(exc))
            else:
                if 200 <= response.status_code < 300:
                    return SendResult(ok=True, status_code=response.status_code)
                if response.status_code not in RETRYABLE_STATUS_CODES:
                    if response.status_code == KEY_NOT_FOUND_CODE:
                        error = KEY_NOT_FOUND_ERROR
                    else:
                        error = _response_error(response)
                    logger.error("Waterius returned HTTP %s: %s", response.status_code, response.text[:200])
                    return SendResult(ok=False, status_code=response.status_code, error=error)
                last_failure = SendResult(ok=False, status_code=response.status_code)
                logger.warning(
                    "Waterius HTTP %s (attempt %d/%d), retrying", response.status_code, attempt, max_attempts
                )
            if attempt == max_attempts:
                break  # that was the last attempt, nothing left to wait for
            if stop_event.wait(self.backoff * attempt):
                break  # the event fired during the backoff: a shutdown was requested
        return last_failure
