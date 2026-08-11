"""
Virtual WB devices published by the service.

Topology:

* an integration device ``/devices/wb-mqtt-waterius`` — the automatic-sending switch plus
  read-only status (state, version, current time, next send).
* one device per key ``/devices/wb-mqtt-waterius_<N>`` (1-based) — typed read-only
  channel mirrors plus per-device "Last Sent" and "Last Error". Titled by the configured
  device name, or by a masked key prefix when there is none.

A failed send fills the "Last Error" control of the affected key device and raises the WB error
flag on it, which the UI shows in red. The integration device aggregates to the "Has Errors"
state.
"""

import json
import re
import threading
from collections import Counter
from collections.abc import Callable
from typing import Any, Optional

from wb_common.mqtt_client import MQTTClient

from wb.mqtt_waterius.config import Config, Device
from wb.mqtt_waterius.waterius_api import mask_key

DEVICE_ID = "wb-mqtt-waterius"
DEVICE_BASE = f"/devices/{DEVICE_ID}"
DRIVER = "wb-mqtt-waterius"

_KEY_DEVICE_ID_RE = re.compile(r"^wb-mqtt-waterius_\d+$")

# Retained messages arrive in a burst right after the subscription, so a scan ends on a pause in
# the burst instead of on a fixed window.
_SCAN_IDLE_TIMEOUT = 0.1


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

# Integration-device controls, in display order.
STATUS_CONTROLS = [
    (
        "enabled",
        {
            "type": "switch",
            "order": 1,
            "title": {"en": "Integration Enabled", "ru": "Интеграция включена"},
        },
    ),
    (
        "state",
        {
            "type": "value",
            "readonly": True,
            "order": 2,
            "title": {"en": "State", "ru": "Состояние"},
            "enum": STATE_ENUM,
        },
    ),
    (
        "version",
        {"type": "text", "readonly": True, "order": 3, "title": {"en": "Version", "ru": "Версия"}},
    ),
    (
        "current_time",
        {
            "type": "text",
            "readonly": True,
            "order": 4,
            "title": {"en": "Current Time", "ru": "Текущее время"},
        },
    ),
    (
        "next_execution",
        {
            "type": "text",
            "readonly": True,
            "order": 5,
            "title": {"en": "Next Send", "ru": "Следующая отправка"},
        },
    ),
]

# Per-device controls: "Last Sent" on top (order 1), channels in between (order 2..),
# "Last Error" at the bottom (order 100).
KEY_LAST_SENT = (
    "last_sent",
    {"type": "text", "readonly": True, "order": 1, "title": {"en": "Last Sent", "ru": "Отправлено"}},
)
KEY_LAST_ERROR = (
    "last_error",
    {"type": "text", "readonly": True, "order": 100, "title": {"en": "Errors", "ru": "Ошибки"}},
)


def key_device_id(index: int) -> str:
    """
    Device id for the 1-based key index.

    Examples:
        >>> key_device_id(1)
        'wb-mqtt-waterius_1'
        >>> key_device_id(12)
        'wb-mqtt-waterius_12'
    """
    return f"{DEVICE_ID}_{index}"


def _is_our_device(device_id: str) -> bool:
    """
    Tell whether a device id found in the broker is published by this service.

    Examples:
        >>> _is_our_device("wb-mqtt-waterius")
        True
        >>> _is_our_device("wb-mqtt-waterius_5")
        True
        >>> _is_our_device("wb-mqtt-serial")
        False
    """
    return device_id == DEVICE_ID or bool(_KEY_DEVICE_ID_RE.match(device_id))


def _publish(client: MQTTClient, topic: str, value: str) -> Any:
    return client.publish(topic, value, retain=True, qos=1)


def _remove_control(client: MQTTClient, base: str, control_id: str) -> list[Any]:
    return [
        _publish(client, f"{base}/controls/{control_id}/meta", ""),
        _publish(client, f"{base}/controls/{control_id}", ""),
        _publish(client, f"{base}/controls/{control_id}/meta/error", ""),
    ]


def _scan_retained(client: MQTTClient, wildcards: list[str], settle: float) -> list[str]:
    """
    Collect every non-empty retained topic matching the wildcards.

    Subscribes and collects until the broker stops sending, waiting at most ``settle`` for the
    first message. Callbacks are registered before the subscription, otherwise a retained
    message answering it could arrive before there is anything to collect it.
    """
    topics: list[str] = []
    arrived = threading.Event()

    def _on_meta(_client: MQTTClient, _userdata: Any, message: Any) -> None:
        if message.payload:
            topics.append(message.topic)
        arrived.set()

    for wildcard in wildcards:
        client.message_callback_add(wildcard, _on_meta)
        client.subscribe(wildcard)
    receiving = arrived.wait(settle)
    while receiving:
        arrived.clear()
        receiving = arrived.wait(_SCAN_IDLE_TIMEOUT)
    for wildcard in wildcards:
        client.unsubscribe(wildcard)
        client.message_callback_remove(wildcard)
    return topics


def clear_all(client: MQTTClient, settle: float = 0.5) -> list[Any]:
    """
    Remove the integration device and every key device from the broker.

    Discovers our device/control ids from the broker (not from config), so it also
    cleans up devices left by a previous, larger configuration. The id filter is strict
    to our namespace, so foreign devices are never touched. Used on startup and uninstall.
    """
    topics = _scan_retained(client, ["/devices/+/meta", "/devices/+/controls/+/meta"], settle)
    controls: dict[str, set[str]] = {}  # device id -> control ids
    ids = {DEVICE_ID}
    for topic in topics:
        parts = topic.split("/")  # ['', 'devices', <id>, ...]
        if len(parts) < 3:
            continue
        device_id = parts[2]
        if not _is_our_device(device_id):
            continue
        ids.add(device_id)
        if len(parts) >= 6 and parts[3] == "controls" and parts[5] == "meta":
            controls.setdefault(device_id, set()).add(parts[4])

    publish_results: list[Any] = []
    for device_id in ids:
        base = f"/devices/{device_id}"
        for control_id in controls.get(device_id, ()):
            publish_results.extend(_remove_control(client, base, control_id))
        publish_results.append(_publish(client, f"{base}/meta", ""))
    return publish_results


class KeyDevice:
    """
    One WB device per Waterius key: read-only mirrors of its channels plus its send status.
    """

    def __init__(self, client: MQTTClient, index: int, device: Device, last_sent: str = "") -> None:
        self._client = client
        self._config_device = device
        self._base = f"/devices/{key_device_id(index)}"
        self._channels: list[tuple[str, dict]] = []
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
                int(channel.data_type),
                ("value", "", {"en": f"Type {channel.data_type}", "ru": f"Тип {channel.data_type}"}),
            )
            title = dict(base)
            # Disambiguate a repeated meter type by the short control name, not the full
            # topic: the UI cell truncates a long title on the tail that distinguishes it.
            if type_counts[channel.data_type] > 1:
                title["en"] += f" ({channel.control})"
                title["ru"] += f" ({channel.control})"
            meta = {"type": wb_type, "readonly": True, "order": channel_index + 2, "title": title}
            if units:
                meta["units"] = units
            control_id = f"ch{channel_index}"
            self._channels.append((control_id, meta))
            self._control_ids_by_source_topic.setdefault(channel.mqtt_topic, []).append(control_id)

    def publish_meta(self) -> None:
        device_meta = {"driver": DRIVER, "title": self._title()}
        _publish(self._client, f"{self._base}/meta", json.dumps(device_meta))
        for control_id, meta in self._channels:
            _publish(self._client, f"{self._base}/controls/{control_id}/meta", json.dumps(meta))
            _publish(self._client, f"{self._base}/controls/{control_id}", "")
        for name, meta in (KEY_LAST_SENT, KEY_LAST_ERROR):
            _publish(self._client, f"{self._base}/controls/{name}/meta", json.dumps(meta))
        # Restore the persisted "Last Sent". "Last Error" starts empty.
        _publish(self._client, f"{self._base}/controls/{KEY_LAST_SENT[0]}", self._last_sent)
        _publish(self._client, f"{self._base}/controls/{KEY_LAST_ERROR[0]}", "")
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

    def set_last_sent(self, text: str) -> None:
        self._last_sent = text
        _publish(self._client, f"{self._base}/controls/{KEY_LAST_SENT[0]}", text)

    def _set_error(self, on: bool) -> None:
        """
        Toggle the WB error flag on the Last Error control, shown red in the UI.

        The flag is per-control in WB, and a failed send does not make the channel data
        wrong, so only this control carries it.
        """
        _publish(self._client, f"{self._base}/controls/{KEY_LAST_ERROR[0]}/meta/error", "r" if on else "")

    def mark_sent(self, timestamp: str) -> None:
        """
        A successful send for this device: stamp Last Sent, clear the error.
        """
        self.set_last_sent(timestamp)
        _publish(self._client, f"{self._base}/controls/{KEY_LAST_ERROR[0]}", "")
        self._set_error(False)

    def mark_failed(self, detail: str) -> None:
        """
        A failed send (API error or unavailable channels): show detail on Last Error.
        """
        _publish(self._client, f"{self._base}/controls/{KEY_LAST_ERROR[0]}", detail)
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
        _publish(self._client, f"{DEVICE_BASE}/meta", json.dumps(device_meta))
        for name, meta in STATUS_CONTROLS:
            _publish(self._client, f"{DEVICE_BASE}/controls/{name}/meta", json.dumps(meta))
        # Startup state. The service applies the resting state once it is ready.
        self.set_state(STATE_INITIALIZING)
        self.set_error(False)
        _publish(self._client, f"{DEVICE_BASE}/controls/version", self._version)

    def subscribe(self) -> None:
        enabled_topic = f"{DEVICE_BASE}/controls/enabled/on"
        self._client.subscribe(enabled_topic)
        self._client.message_callback_add(enabled_topic, self._on_enabled)

    def _on_enabled(self, _client: MQTTClient, _userdata: Any, message: Any) -> None:
        enabled = message.payload.decode(errors="replace").strip() == "1"
        if self._on_toggle:
            self._on_toggle(enabled)

    def set_state(self, state: int) -> None:
        """
        Publish a numeric state code (see STATE_ENUM). The UI shows its label.
        """
        _publish(self._client, f"{DEVICE_BASE}/controls/state", str(state))

    def set_enabled(self, on: bool) -> None:
        _publish(self._client, f"{DEVICE_BASE}/controls/enabled", "1" if on else "0")

    def set_current_time(self, text: str) -> None:
        _publish(self._client, f"{DEVICE_BASE}/controls/current_time", text)

    def set_next_run(self, text: str) -> None:
        _publish(self._client, f"{DEVICE_BASE}/controls/next_execution", text)

    def set_error(self, on: bool) -> Any:
        """
        Toggle the WB error flag on the state control (non-empty = red in the UI).

        Returns the publish result, so a caller shutting down can wait for delivery.
        """
        return _publish(self._client, f"{DEVICE_BASE}/controls/state/meta/error", "r" if on else "")


class WateriusDevices:
    """
    Owns the integration device and one key device per entry in the config. The service's facade.
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
        self._key_devices: list[KeyDevice] = []
        if config is not None:
            restore = last_sent or []
            self._key_devices = [
                KeyDevice(client, index + 1, device, restore[index] if index < len(restore) else "")
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

    def clear(self, settle: float = 0.5) -> list[Any]:
        """
        Wipe every retained waterius device from the broker (clean-slate init).
        """
        return clear_all(self._client, settle)
