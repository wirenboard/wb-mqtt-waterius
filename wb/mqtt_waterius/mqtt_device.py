"""
Virtual WB devices published by the service.

Topology:

* an integration device ``/devices/wb_mqtt_waterius`` — the automatic-sending switch plus
  read-only status (state, version, current time, next send).
* one device per key ``/devices/wb_mqtt_waterius_<N>`` (1-based) — typed read-only
  channel mirrors plus per-device "Last Sent" and "Errors". Titled by the configured
  device name, or by a masked key prefix when there is none.

A failed send fills the "Errors" control of the affected key device and raises the WB error
flag on it, which the UI shows in red. The integration device aggregates to the "Has Errors"
state.
"""

import json
import re
import threading
import uuid
from collections import Counter
from collections.abc import Callable
from typing import Any, NamedTuple, Optional

from wb_common.mqtt_client import MQTTClient

from wb.mqtt_waterius.config import Config, Device
from wb.mqtt_waterius.waterius_api import mask_key

DEVICE_ID_PREFIX = "wb_mqtt_waterius"
INTEGRATION_DEVICE_BASE = f"/devices/{DEVICE_ID_PREFIX}"
DRIVER = "wb-mqtt-waterius"

_KEY_DEVICE_ID_RE = re.compile(rf"^{re.escape(DEVICE_ID_PREFIX)}_\d+$")

# Error flag for a control, see "Errors" in the WB conventions. Sending a reading out to the
# cloud is a write, so "w".
_ERROR_FLAG = "w"

# Finding our devices needs meta topics only. The "#" catches the JSON meta and per-field
# meta subtopics alike.
_DISCOVERY_WILDCARDS = ["/devices/+/meta/#", "/devices/+/controls/+/meta/#"]

# Our own topic, outside the device tree so a scan of our devices cannot pick up its own
# marker. Published without retain, so the broker keeps nothing.
_MARKER_TOPIC = "/wb_mqtt_waterius/marker"

# Guard for a broker that answers nothing. It bounds the whole retained burst, not just its
# first message, so it has room for a large installation.
DEFAULT_SCAN_TIMEOUT = 5.0


# data_type code -> (WB control type, units, bilingual title). All meters use the generic
# "value" type with an explicit unit. Titles follow Waterius's own cabinet naming.
DATA_TYPE_CONTROLS = {
    0: ("value", "m^3", {"en": "Cold Water", "ru": "Холодная вода"}),
    1: ("value", "m^3", {"en": "Hot Water", "ru": "Горячая вода"}),
    2: ("value", "kWh", {"en": "Electricity", "ru": "Электричество"}),
    3: ("value", "m^3", {"en": "Gas", "ru": "Газ"}),
    4: ("value", "Gcal", {"en": "Heat", "ru": "Отопление"}),
    5: ("value", "kWh", {"en": "Electricity (Day)", "ru": "Электричество (День)"}),
    6: ("value", "kWh", {"en": "Electricity (Night)", "ru": "Электричество (Ночь)"}),
    7: ("value", "kWh", {"en": "Electricity (Peak)", "ru": "Электричество (Пик)"}),
    8: ("value", "kWh", {"en": "Electricity (Semi-Peak)", "ru": "Электричество (Полупик)"}),
    9: ("value", "m^3", {"en": "Potable Water", "ru": "Питьевая вода"}),
}

# Numeric state codes for the integration device "state" control. The labels live in meta.enum.
STATE_INITIALIZING = 0
STATE_CONFIG_INVALID = 1
STATE_DISABLED = 2
STATE_ACTIVE = 3
STATE_HAS_ERRORS = 4

STATE_ENUM = {
    STATE_INITIALIZING: {"en": "Initialization Started", "ru": "Инициализация запущена"},
    STATE_CONFIG_INVALID: {"en": "Config Not Valid", "ru": "Настройки не корректны"},
    STATE_DISABLED: {"en": "Disabled", "ru": "Отключен"},
    STATE_ACTIVE: {"en": "Active", "ru": "Активен"},
    STATE_HAS_ERRORS: {"en": "Has Errors", "ru": "Есть ошибки"},
}


class Control(NamedTuple):
    """
    One WB control — the id it takes in the topic tree and the meta published for it.
    """

    id: str
    meta: dict


# Integration-device controls, in display order.
STATUS_ENABLED = Control(
    "enabled",
    {
        "type": "switch",
        "order": 1,
        "title": {"en": "Integration Enabled", "ru": "Интеграция включена"},
    },
)
STATUS_STATE = Control(
    "state",
    {
        "type": "value",
        "readonly": True,
        "order": 2,
        "title": {"en": "State", "ru": "Состояние"},
        "enum": STATE_ENUM,
    },
)
STATUS_VERSION = Control(
    "version",
    {"type": "text", "readonly": True, "order": 3, "title": {"en": "Version", "ru": "Версия"}},
)
STATUS_CURRENT_TIME = Control(
    "current_time",
    {
        "type": "text",
        "readonly": True,
        "order": 4,
        "title": {"en": "Current Time", "ru": "Текущее время"},
    },
)
STATUS_NEXT_EXECUTION = Control(
    "next_execution",
    {
        "type": "text",
        "readonly": True,
        "order": 5,
        "title": {"en": "Next Send", "ru": "Следующая отправка"},
    },
)

STATUS_CONTROLS = [STATUS_ENABLED, STATUS_STATE, STATUS_VERSION, STATUS_CURRENT_TIME, STATUS_NEXT_EXECUTION]

# Per-device controls: "Last Sent" on top (order 1), channels in between (order 2..),
# "Errors" at the bottom (order 100).
KEY_LAST_SENT = Control(
    "last_sent",
    {"type": "text", "readonly": True, "order": 1, "title": {"en": "Last Sent", "ru": "Отправлено"}},
)
KEY_LAST_ERROR = Control(
    "last_error",
    {"type": "text", "readonly": True, "order": 100, "title": {"en": "Errors", "ru": "Ошибки"}},
)


def build_key_device_id(index: int) -> str:
    """
    Device id for the 1-based key index.

    Examples:
        >>> build_key_device_id(1)
        'wb_mqtt_waterius_1'
        >>> build_key_device_id(12)
        'wb_mqtt_waterius_12'
    """
    return f"{DEVICE_ID_PREFIX}_{index}"


def _is_our_device(device_id: str) -> bool:
    """
    Tell whether a device id found in the broker is published by this service.

    Examples:
        >>> _is_our_device("wb_mqtt_waterius")
        True
        >>> _is_our_device("wb_mqtt_waterius_5")
        True
        >>> _is_our_device("wb-mqtt-serial")
        False
    """
    return device_id == DEVICE_ID_PREFIX or bool(_KEY_DEVICE_ID_RE.match(device_id))


def _publish(client: MQTTClient, topic: str, value: str) -> None:
    client.publish(topic, value, retain=True, qos=1)


def wait_for_broker(client: MQTTClient, timeout: float = DEFAULT_SCAN_TIMEOUT) -> bool:
    """
    Block until the broker has handled everything published before this call.

    Publishes a token to our own topic and waits for it back. A broker handles one client's
    packets in order, so the token returning means our earlier ones are through. The token is
    unique per call, so a stale one or another process's cannot end this wait.

    Returns:
        False if the token did not come back within the timeout
    """
    arrived = threading.Event()
    marker = uuid.uuid4().hex.encode()

    def _on_marker(_client: MQTTClient, _userdata: Any, message: Any) -> None:
        if message.payload == marker:
            arrived.set()

    client.message_callback_add(_MARKER_TOPIC, _on_marker)
    client.subscribe(_MARKER_TOPIC)
    client.publish(_MARKER_TOPIC, marker, qos=1)
    confirmed = arrived.wait(timeout)
    client.unsubscribe(_MARKER_TOPIC)
    client.message_callback_remove(_MARKER_TOPIC)
    return confirmed


def _scan_retained(
    client: MQTTClient, wildcards: list[str], timeout: float = DEFAULT_SCAN_TIMEOUT
) -> list[str]:
    """
    Collect every non-empty retained topic matching the wildcards.

    The broker queues the retained set while it processes our subscriptions, so waiting for it
    to catch up ends the burst. Callbacks are registered before the subscription, otherwise a
    retained message answering it could arrive before there is anything to collect it.
    """
    topics: list[str] = []

    def _collect_topic(_client: MQTTClient, _userdata: Any, message: Any) -> None:
        if message.payload:
            topics.append(message.topic)

    for wildcard in wildcards:
        client.message_callback_add(wildcard, _collect_topic)
        client.subscribe(wildcard)
    wait_for_broker(client, timeout)
    for wildcard in wildcards:
        client.unsubscribe(wildcard)
        client.message_callback_remove(wildcard)
    return topics


def _discover_our_device_ids(client: MQTTClient, timeout: float) -> list[str]:
    """
    Our device ids present in the broker, sorted, with the integration device always included.

    Scans meta topics only, both a device's own and its controls', so a device whose own
    ``/meta`` is gone is still found by its controls.
    """
    ids = {DEVICE_ID_PREFIX}
    for topic in _scan_retained(client, _DISCOVERY_WILDCARDS, timeout):
        parts = topic.split("/")  # ['', 'devices', <id>, ...]
        if len(parts) >= 3 and _is_our_device(parts[2]):
            ids.add(parts[2])
    return sorted(ids)


def _scan_our_device_topics(client: MQTTClient, our_device_ids: list[str], timeout: float) -> list[str]:
    """
    Every non-empty retained topic published under the given device ids, without duplicates.
    """
    wildcards = [f"/devices/{device_id}/#" for device_id in our_device_ids]
    return list(dict.fromkeys(_scan_retained(client, wildcards, timeout)))


def clear_all(client: MQTTClient, timeout: float = DEFAULT_SCAN_TIMEOUT) -> list[str]:
    """
    Remove the integration device and every key device from the broker.

    Ids come from the broker, not from the config, so a key dropped from the config goes too and
    foreign devices are never touched. Stuck retained commands go as well, safe only because the
    caller drops the switch subscription first, which ``WateriusDevices.clear`` does. A device's
    own ``/meta`` is emptied last, so an interrupted run leaves the device discoverable by it.

    Returns:
        the topics emptied, a device's own ``/meta`` included even where the broker held none
    """
    our_device_ids = _discover_our_device_ids(client, timeout)
    our_device_meta_topics = {f"/devices/{device_id}/meta" for device_id in our_device_ids}
    our_device_topics = _scan_our_device_topics(client, our_device_ids, timeout)
    other_topics = [topic for topic in our_device_topics if topic not in our_device_meta_topics]
    wiped_topics = other_topics + sorted(our_device_meta_topics)
    for topic in wiped_topics:
        _publish(client, topic, "")
    return wiped_topics


class PerKeyDevice:
    """
    One WB device per Waterius key: read-only mirrors of its channels plus its send status.
    """

    def __init__(self, client: MQTTClient, index: int, device: Device, last_sent: str = "") -> None:
        self._client = client
        self._config_device = device
        self._base = f"/devices/{build_key_device_id(index)}"
        self._channels: list[Control] = []
        self._control_ids_by_source_topic: dict[str, list[str]] = {}
        # Restored from persistent state, kept current so a reconnect republishes it.
        self._last_sent = last_sent
        self._build_channels()

    def _title(self) -> dict:
        # Without a device name, fall back to a masked key prefix — the full key is a write
        # credential and /devices/+/meta is readable by the whole LAN.
        title_suffix = self._config_device.name or mask_key(self._config_device.key)
        return {"en": f"Waterius - {title_suffix}", "ru": f"Ватериус - {title_suffix}"}

    def _build_channels(self) -> None:
        type_counts = Counter(channel.data_type for channel in self._config_device.channels)
        for channel_index, channel in enumerate(self._config_device.channels):
            wb_type, units, base = DATA_TYPE_CONTROLS.get(
                channel.data_type,
                ("value", "", {"en": f"Type {channel.data_type}", "ru": f"Тип {channel.data_type}"}),
            )
            # Disambiguate a repeated meter type by the short control name, not the full
            # topic: the UI cell truncates a long title on the tail that distinguishes it.
            suffix = f" ({channel.control})" if type_counts[channel.data_type] > 1 else ""
            title = {lang: text + suffix for lang, text in base.items()}
            meta = {"type": wb_type, "readonly": True, "order": channel_index + 2, "title": title}
            if units:
                meta["units"] = units
            control_id = f"ch{channel_index}"
            self._channels.append(Control(control_id, meta))
            self._control_ids_by_source_topic.setdefault(channel.mqtt_topic, []).append(control_id)

    def publish_meta(self) -> None:
        device_meta = {"driver": DRIVER, "title": self._title()}
        _publish(self._client, f"{self._base}/meta", json.dumps(device_meta))
        for control in self._channels:
            _publish(self._client, f"{self._base}/controls/{control.id}/meta", json.dumps(control.meta))
            _publish(self._client, f"{self._base}/controls/{control.id}", "")
        for control in (KEY_LAST_SENT, KEY_LAST_ERROR):
            _publish(self._client, f"{self._base}/controls/{control.id}/meta", json.dumps(control.meta))
        # Restore the persisted "Last Sent". "Errors" starts empty.
        _publish(self._client, f"{self._base}/controls/{KEY_LAST_SENT.id}", self._last_sent)
        _publish(self._client, f"{self._base}/controls/{KEY_LAST_ERROR.id}", "")
        self._set_error(False)

    def update_channel(self, mqtt_topic: str, raw_value: Optional[str]) -> None:
        """
        Mirror a source reading onto its channel control(s), if this device owns it.

        The value is passed through untouched, so the mirror always shows what the source
        control shows.
        """
        control_ids = self._control_ids_by_source_topic.get(mqtt_topic)
        if not control_ids:
            return
        value = "" if raw_value is None else raw_value
        for control_id in control_ids:
            _publish(self._client, f"{self._base}/controls/{control_id}", value)

    def _set_error(self, on: bool) -> None:
        """
        Toggle the WB error flag on the "Errors" control, shown red in the UI.

        The flag is per-control in WB, and a failed send does not make the channel data
        wrong, so only this control carries it.
        """
        _publish(
            self._client, f"{self._base}/controls/{KEY_LAST_ERROR.id}/meta/error", _ERROR_FLAG if on else ""
        )

    def mark_sent(self, timestamp: str) -> None:
        """
        A successful send for this device: stamp Last Sent, clear the error.

        The stamp is kept in the instance too, so a reconnect republishes it.
        """
        self._last_sent = timestamp
        _publish(self._client, f"{self._base}/controls/{KEY_LAST_SENT.id}", timestamp)
        _publish(self._client, f"{self._base}/controls/{KEY_LAST_ERROR.id}", "")
        self._set_error(False)

    def mark_failed(self, detail: str) -> None:
        """
        A failed send (API error or unavailable channels): show detail on "Errors".
        """
        _publish(self._client, f"{self._base}/controls/{KEY_LAST_ERROR.id}", detail)
        self._set_error(True)


class IntegrationDevice:
    """
    The control/status device: automatic-sending switch and read-only status.
    """

    def __init__(
        self, client: MQTTClient, on_toggle: Optional[Callable[[bool], None]] = None, version: str = ""
    ) -> None:
        self._client = client
        self._on_toggle = on_toggle
        self._version = version

    def publish_meta(self) -> None:
        title = {"en": "Waterius Integration", "ru": "Интеграция с Ватериус"}
        device_meta = {"driver": DRIVER, "title": title}
        _publish(self._client, f"{INTEGRATION_DEVICE_BASE}/meta", json.dumps(device_meta))
        for control in STATUS_CONTROLS:
            _publish(
                self._client,
                f"{INTEGRATION_DEVICE_BASE}/controls/{control.id}/meta",
                json.dumps(control.meta),
            )
        # Startup state. The service applies the resting state once it is ready.
        self.set_state(STATE_INITIALIZING)
        self.set_error(False)
        _publish(self._client, f"{INTEGRATION_DEVICE_BASE}/controls/{STATUS_VERSION.id}", self._version)

    def _enabled_topic(self) -> str:
        return f"{INTEGRATION_DEVICE_BASE}/controls/{STATUS_ENABLED.id}/on"

    def subscribe(self) -> None:
        self._client.subscribe(self._enabled_topic())
        self._client.message_callback_add(self._enabled_topic(), self._on_enabled)

    def unsubscribe(self) -> None:
        """
        Drop the switch subscription and its callback, to be called before a wipe.

        A scan of our own devices matches the command topic too, so the broker hands a stuck
        retained command to a live callback and the switch flips behind the user's back. The
        callback outlives a single setup, so wiping before subscribing is not enough on its own.
        """
        self._client.unsubscribe(self._enabled_topic())
        self._client.message_callback_remove(self._enabled_topic())

    def _on_enabled(self, _client: MQTTClient, _userdata: Any, message: Any) -> None:
        command = message.payload.decode(errors="replace").strip()
        if not command:
            # Emptying the topic is how a command is cleared, never how one is given.
            return
        if self._on_toggle:
            self._on_toggle(command == "1")

    def set_state(self, state: int) -> None:
        """
        Publish a numeric state code (see STATE_ENUM). The UI shows its label.
        """
        _publish(self._client, f"{INTEGRATION_DEVICE_BASE}/controls/{STATUS_STATE.id}", str(state))

    def set_enabled(self, on: bool) -> None:
        _publish(self._client, f"{INTEGRATION_DEVICE_BASE}/controls/{STATUS_ENABLED.id}", "1" if on else "0")

    def set_current_time(self, text: str) -> None:
        _publish(self._client, f"{INTEGRATION_DEVICE_BASE}/controls/{STATUS_CURRENT_TIME.id}", text)

    def set_next_run(self, text: str) -> None:
        _publish(self._client, f"{INTEGRATION_DEVICE_BASE}/controls/{STATUS_NEXT_EXECUTION.id}", text)

    def set_error(self, on: bool) -> None:
        """
        Toggle the WB error flag on the state control (non-empty = red in the UI).
        """
        _publish(
            self._client,
            f"{INTEGRATION_DEVICE_BASE}/controls/{STATUS_STATE.id}/meta/error",
            _ERROR_FLAG if on else "",
        )


class WateriusDevices:
    """
    Owns the integration device and one key device per entry in the config. The service's facade.

    Restored "Last Sent" stamps line up with ``config.devices`` positionally. A shorter list is
    allowed, the keys it does not reach start with an empty stamp.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        client: MQTTClient,
        *,
        on_toggle: Optional[Callable[[bool], None]] = None,
        config: Optional[Config] = None,
        version: str = "",
        last_sent: Optional[list[str]] = None,
    ) -> None:
        self._client = client
        self._integration_device = IntegrationDevice(client, on_toggle, version)
        self._key_devices: list[PerKeyDevice] = []
        if config is not None:
            restore = last_sent or []
            self._key_devices = [
                PerKeyDevice(client, index + 1, device, restore[index] if index < len(restore) else "")
                for index, device in enumerate(config.devices)
            ]

    def publish_meta(self) -> None:
        self._integration_device.publish_meta()
        for key_device in self._key_devices:
            key_device.publish_meta()

    def subscribe(self) -> None:
        self._integration_device.subscribe()

    def update_channel(self, mqtt_topic: str, raw_value: Optional[str]) -> None:
        for key_device in self._key_devices:
            key_device.update_channel(mqtt_topic, raw_value)

    def set_state(self, state: int) -> None:
        self._integration_device.set_state(state)

    def set_enabled(self, on: bool) -> None:
        self._integration_device.set_enabled(on)

    def set_current_time(self, text: str) -> None:
        self._integration_device.set_current_time(text)

    def set_next_run(self, text: str) -> None:
        self._integration_device.set_next_run(text)

    def set_integration_error(self, on: bool) -> None:
        """
        Aggregate error flag on the integration device state control (red at the top level).
        """
        self._integration_device.set_error(on)

    def mark_device_sent(self, index: int, timestamp: str) -> None:
        if 0 <= index < len(self._key_devices):
            self._key_devices[index].mark_sent(timestamp)

    def mark_device_failed(self, index: int, detail: str) -> None:
        if 0 <= index < len(self._key_devices):
            self._key_devices[index].mark_failed(detail)

    def clear(self, timeout: float = DEFAULT_SCAN_TIMEOUT) -> list[str]:
        """
        Wipe every retained waterius device from the broker (clean-slate init).

        Drops the switch subscription first, so the wipe cannot feed a stuck retained command
        back into the toggle, and must run before ``subscribe`` restores it.

        Returns:
            the topics emptied, device meta included even where the broker held none
        """
        self._integration_device.unsubscribe()
        return clear_all(self._client, timeout)
