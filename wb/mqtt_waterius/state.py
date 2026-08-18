"""
Persistent runtime state for wb-mqtt-waterius.

A single JSON file under /var/lib that survives a restart of the service and of the broker.
It holds the automatic-sending switch, the time the marks below were made for, and a mark per
device — the day it last sent and the stamp its card shows. Reading and writing is all this
module does, the rules around the values live in the service.
"""

import hashlib
import json
import logging
import os
from typing import Any

STATE_DIR = "/var/lib/wb-mqtt-waterius"
STATE_FILE = os.path.join(STATE_DIR, "state.json")

logger = logging.getLogger(__name__)


def key_hash(key: str) -> str:
    """
    Stable, non-reversible id for a device key.

    Used as the map key of the per-device timestamps, so the key itself is not copied into
    the state file.

    Args:
        key: Waterius device key

    Returns:
        First 12 hex characters of the key's SHA-1
    """
    return hashlib.sha1(key.encode()).hexdigest()[:12]


def load_state() -> dict:
    """
    Load the persistent runtime state, falling back to safe defaults.

    - ``enabled`` — automatic sending on or off.
    - ``schedule_time`` — the time the marks below were made for. A changed time counts as a
      new slot, so the marks are dropped and the day sends once more.
    - ``last_sent`` — per-device ``{"date", "stamp"}``, keyed by a hash of the device key.
      The date says who already sent today, so a restart resumes instead of losing the day.
      The stamp is what the device card shows. Marks of devices dropped from the config are
      removed by the service on startup.
    """
    try:
        with open(STATE_FILE, encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, ValueError):
        data = {}
    if not isinstance(data, dict):
        data = {}
    return {
        "enabled": bool(data.get("enabled", True)),
        "schedule_time": data.get("schedule_time"),
        "last_sent": _get_device_marks(data.get("last_sent")),
    }


def _get_device_marks(raw: Any) -> dict:
    """
    Keep the well-formed marks and drop the rest, a hand-edited file must not crash the daemon.

    Examples:
        >>> _get_device_marks({"a1b2": {"date": "2026-08-18", "stamp": "Tuesday 2026-08-18 03:00"}})
        {'a1b2': {'date': '2026-08-18', 'stamp': 'Tuesday 2026-08-18 03:00'}}
        >>> _get_device_marks({"a1b2": "Tuesday 2026-08-18 03:00"})
        {}
    """
    if not isinstance(raw, dict):
        return {}
    return {
        hashed_key: {"date": mark.get("date"), "stamp": mark.get("stamp", "")}
        for hashed_key, mark in raw.items()
        if isinstance(mark, dict)
    }


def save_state(state: dict) -> None:
    """
    Write the state atomically.

    The write goes to a temp file and os.replace moves it into place, so a crash never
    leaves a half-written state.json behind.
    """
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(state, handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, STATE_FILE)
    except OSError as exc:
        logger.error("Cannot write state file: %s", exc)
