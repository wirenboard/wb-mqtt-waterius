"""
Persistent runtime state for wb-mqtt-waterius.

A single JSON file under /var/lib that survives a restart of the service and of the broker.
It holds the automatic-sending switch and, per device, the moment of its last successful send.
Reading and writing is all this module does, the rules around the values live in the service.
"""

import datetime
import hashlib
import json
import logging
import os
from typing import Any, TypedDict

STATE_DIR = "/var/lib/wb-mqtt-waterius"
STATE_FILE = os.path.join(STATE_DIR, "state.json")

logger = logging.getLogger(__name__)


class State(TypedDict):
    """
    Contents of state.json.

    Attributes:
        enabled: automatic sending on or off
        last_sent: ISO moment of the last successful send per device, keyed by key_hash
    """

    enabled: bool
    last_sent: dict[str, str]


def key_hash(key: str) -> str:
    """
    Stable, non-reversible id for a device key.

    Used as the map key of the per-device moments, so the key itself is not copied into
    the state file.

    Args:
        key: Waterius device key

    Returns:
        First 12 hex characters of the key's SHA-1
    """
    return hashlib.sha1(key.encode()).hexdigest()[:12]


def load_state() -> State:
    """
    Load the persistent runtime state, falling back to safe defaults.

    A device without a moment counts as never sent, so the worst a missing or broken file can
    do is one extra send. The service drops the moments of devices gone from the config.
    """
    try:
        with open(STATE_FILE, encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        logger.info("No state file yet, starting with defaults")
        data = {}
    except ValueError as exc:
        logger.warning("State file is not valid JSON, falling back to defaults: %s", exc)
        data = {}
    except OSError as exc:
        logger.warning("Cannot read state file, falling back to defaults: %s", exc)
        data = {}
    if not isinstance(data, dict):
        logger.warning("State file is not a JSON object, falling back to defaults")
        data = {}
    state: State = {
        "enabled": bool(data.get("enabled", True)),
        "last_sent": _get_sent_moments(data.get("last_sent")),
    }
    return state


def _get_sent_moments(raw: Any) -> dict[str, str]:
    """
    Keep the entries that read as an ISO moment and drop the rest.

    Examples:
        >>> _get_sent_moments({"a1b2": "2026-08-18T03:00:00", "c3d4": "yesterday"})
        {'a1b2': '2026-08-18T03:00:00'}
    """
    if not isinstance(raw, dict):
        return {}
    moments: dict[str, str] = {}
    for hashed_key, moment in raw.items():
        try:
            datetime.datetime.fromisoformat(moment)
        except (TypeError, ValueError):
            continue
        moments[hashed_key] = moment
    return moments


def save_state(state: State) -> None:
    """
    Write the state atomically.

    The write goes to a temp file and os.replace moves it into place, so a crash never
    leaves a half-written state.json behind. The temp name carries the pid, so the daemon and
    a manual send cannot truncate each other's write.
    """
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
        tmp = f"{STATE_FILE}.{os.getpid()}.tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(state, handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, STATE_FILE)
    except OSError as exc:
        logger.error("Cannot write state file: %s", exc)
