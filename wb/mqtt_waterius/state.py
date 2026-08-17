"""
Persistent runtime state for wb-mqtt-waterius.

A single JSON file under /var/lib that survives a restart of the service and of the broker.
It holds the automatic-sending switch, the marker of the day already sent and the per-device
display timestamps. Reading and writing is all this module does, the rules around the values
live in the service.
"""

import hashlib
import json
import logging
import os

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
    - ``last_sent_date`` with ``schedule_time`` — the day already sent and the time it was
      sent for. A restart reads them instead of sending again, and a changed time counts as
      a new slot, so it sends once more that day.
    - ``last_sent`` — per-device display timestamps, keyed by a hash of the device key.
    """
    try:
        with open(STATE_FILE, encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, ValueError):
        data = {}
    if not isinstance(data, dict):
        data = {}
    per_device = data.get("last_sent")
    return {
        "enabled": bool(data.get("enabled", True)),
        "last_sent_date": data.get("last_sent_date"),
        "schedule_time": data.get("schedule_time"),
        "last_sent": per_device if isinstance(per_device, dict) else {},
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
