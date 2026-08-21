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


def short_key(key: str) -> str:
    """
    Cut form of a Waterius key, enough to tell devices apart in logs, titles and errors.

    A key is a cloud write credential, so it is never shown in full. The star marks the cut.

    Args:
        key: Waterius device key, non-empty — the config rejects a device without one

    Returns:
        First 5 characters of the key and a star

    Examples:
        >>> short_key("0123456789abcdef0123456789abcdef")
        '01234*'
    """
    return key[:5] + "*"


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
class DeliveryReport:
    """
    Outcome of one delivery to Waterius, retries included.

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
            return f"DeliveryReport(ok, http={self.status_code})"
        return f"DeliveryReport(failed, http={self.status_code}, error={self.error!r})"


def build_payload(device_key: str, name: str, channels: list[ChannelData]) -> dict[str, object]:
    """
    Build the JSON body for one Waterius device.

    The cabinet takes readings by channel number, so a gap would write them into the wrong
    slots. A channel without a value is therefore an error, and keeping such a device out of
    the send is the caller's job.

    Args:
        device_key: Waterius device key, the write credential of one cabinet device
        name: device name for the cabinet, empty keeps the name already set there
        channels: values for ch0..ch3, in that order

    Returns:
        Request body as {"key": ..., "name": ..., "ch<N>": value, "data_type<N>": type,
        "serial<N>": ...}

    Raises:
        ValueError: a channel has no value

    Examples:
        >>> build_payload("KEY", "Boiler", [ChannelData("d/c", 0, 0.1)])
        {'key': 'KEY', 'name': 'Boiler', 'ch0': 0.1, 'data_type0': 0}

        >>> build_payload("K", "", [ChannelData("d/a", 0, 0.1, "1001"),
        ...                         ChannelData("d/b", 1, 0.2)])
        {'key': 'K', 'name': '', 'ch0': 0.1, 'data_type0': 0, 'serial0': '1001', 'ch1': 0.2, 'data_type1': 1}
    """
    payload: dict[str, object] = {"key": device_key, "name": name}
    for index, channel in enumerate(channels):
        if channel.value is None:
            raise ValueError(f"device {short_key(device_key)}: channel {channel.source} has no value")
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

    def send(
        self, payload: dict[str, object], stop_event: Optional[threading.Event] = None
    ) -> DeliveryReport:
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
            Outcome of the delivery, with the status code and the failure reason of the last
            attempt when known
        """
        # Callers without a shutdown flag get a never-set event: waiting on it is a plain sleep.
        stop_event = stop_event or threading.Event()
        max_attempts = self.max_attempts
        # Holds the most recent failure, so the caller gets the real reason once the attempts run
        # out. Only the initial value survives if max_attempts is 0.
        last_failure = DeliveryReport(ok=False, error="no attempt made")
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
                last_failure = DeliveryReport(ok=False, error=str(exc))
            else:
                if 200 <= response.status_code < 300:
                    return DeliveryReport(ok=True, status_code=response.status_code)
                if response.status_code not in RETRYABLE_STATUS_CODES:
                    if response.status_code == KEY_NOT_FOUND_CODE:
                        error = KEY_NOT_FOUND_ERROR
                    else:
                        error = _response_error(response)
                    logger.error("Waterius returned HTTP %s: %s", response.status_code, response.text[:200])
                    return DeliveryReport(ok=False, status_code=response.status_code, error=error)
                last_failure = DeliveryReport(ok=False, status_code=response.status_code)
                logger.warning(
                    "Waterius HTTP %s (attempt %d/%d), retrying", response.status_code, attempt, max_attempts
                )
            if attempt == max_attempts:
                break  # that was the last attempt, nothing left to wait for
            if stop_event.wait(self.backoff * attempt):
                break  # the event fired during the backoff: a shutdown was requested
        return last_failure
