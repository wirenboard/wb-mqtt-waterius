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
from wb.mqtt_waterius.waterius_api import key_prefix

DEVICE_ID_PREFIX = "wb_mqtt_waterius"
INTEGRATION_DEVICE_BASE = f"/devices/{DEVICE_ID_PREFIX}"
DRIVER = "wb-mqtt-waterius"

_KEY_DEVICE_ID_RE = re.compile(rf"^{re.escape(DEVICE_ID_PREFIX)}_\d+$")

# Error flags for a control, see "Errors" in the WB conventions. Sending a reading out to the
# cloud is a write, so "w". A daemon that died can neither read nor write, so its Last Will
# says "rw".
_ERROR_FLAG = "w"
_WILL_ERROR_FLAG = "rw"

# Finding our devices needs meta topics only. The "#" catches the JSON meta and per-field
# meta subtopics alike.
_DISCOVERY_WILDCARDS = ["/devices/+/meta/#", "/devices/+/controls/+/meta/#"]

# Our own topic, outside the device tree so a scan of our devices cannot pick up its own
# marker. Published without retain, so the broker keeps nothing.
MARKER_TOPIC = "/wb_mqtt_waterius/marker"

# Guard for a broker that answers nothing. It bounds the whole retained burst, not just its
# first message, so it has room for a large installation.
DEFAULT_SCAN_TIMEOUT = 5.0

# Placeholder for a time control with nothing to show yet
NO_TIME = "--:--"


# data_type code -> (units, bilingual title). Titles follow Waterius's own cabinet naming.
DATA_TYPE_CONTROLS = {
    0: ("m^3", {"en": "Cold Water", "ru": "Холодная вода"}),
    1: ("m^3", {"en": "Hot Water", "ru": "Горячая вода"}),
    2: ("kWh", {"en": "Electricity", "ru": "Электричество"}),
    3: ("m^3", {"en": "Gas", "ru": "Газ"}),
    4: ("Gcal", {"en": "Heat", "ru": "Отопление"}),
    5: ("kWh", {"en": "Electricity (Day)", "ru": "Электричество (День)"}),
    6: ("kWh", {"en": "Electricity (Night)", "ru": "Электричество (Ночь)"}),
    7: ("kWh", {"en": "Electricity (Peak)", "ru": "Электричество (Пик)"}),
    8: ("kWh", {"en": "Electricity (Semi-Peak)", "ru": "Электричество (Полупик)"}),
    9: ("m^3", {"en": "Potable Water", "ru": "Питьевая вода"}),
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
    Static description of one WB control — the id it takes in the topic tree and its meta.

    The meta is the JSON the UI reads to render the control — ``type``, ``readonly``,
    ``order``, bilingual ``title``, plus ``units`` on the meter channels and ``enum`` on the
    state control.

    Values are not part of it. The status and per-key controls below are module-level
    constants shared by every device, while a value belongs to one device at one moment.
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


def _wipe_topics(client: MQTTClient, topics: list[str]) -> list[str]:
    """
    Publish an empty payload to each topic, which is how a retained topic is removed.
    """
    for topic in topics:
        _publish(client, topic, "")
    return topics


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

    client.message_callback_add(MARKER_TOPIC, _on_marker)
    client.subscribe(MARKER_TOPIC)
    client.publish(MARKER_TOPIC, marker, qos=1)
    confirmed = arrived.wait(timeout)
    client.unsubscribe(MARKER_TOPIC)
    client.message_callback_remove(MARKER_TOPIC)
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
    return _wipe_topics(client, other_topics + sorted(our_device_meta_topics))


class _PublishedDevice:
    """
    What both published devices share — the topic layout and the create/remove lifecycle.

    A subclass supplies its base topic, its controls and its title, then fills in the starting
    values and any topic outside the control list, such as an error flag or a command topic.
    """

    def __init__(self, client: MQTTClient, *, base: str, controls: list[Control], title: dict) -> None:
        self._client = client
        self._base = base
        self._controls = controls
        self._title = title

    def _control_topic(self, control: Control) -> str:
        return f"{self._base}/controls/{control.id}"

    def _publish_control(self, control: Control, value: str) -> None:
        _publish(self._client, self._control_topic(control), value)

    def _publish_control_meta(self, control: Control) -> None:
        _publish(self._client, f"{self._control_topic(control)}/meta", json.dumps(control.meta))

    def _set_control_error(self, control: Control, on: bool) -> None:
        _publish(self._client, f"{self._control_topic(control)}/meta/error", _ERROR_FLAG if on else "")

    def create(self) -> None:
        """
        Publish the device: meta for every control, then a starting value for each.
        """
        self._publish_meta()
        self._publish_initial_values()

    def _publish_meta(self) -> None:
        device_meta = {"driver": DRIVER, "title": self._title}
        _publish(self._client, f"{self._base}/meta", json.dumps(device_meta))
        for control in self._controls:
            self._publish_control_meta(control)

    def _publish_initial_values(self) -> None:
        raise NotImplementedError

    def remove(self) -> list[str]:
        """
        Take this device off the broker, returning the topics emptied.
        """
        return _wipe_topics(self._client, self._get_published_topics())

    def _get_published_topics(self) -> list[str]:
        """
        Every topic this device publishes, its own ``/meta`` last so an interrupted removal
        leaves the device discoverable.

        The error flag goes for every control, not only for the one that raises it today, and
        a writable control takes its command topic along, where the broker can hold a retained
        command that would arrive as a real one. Emptying a topic the broker never held costs
        one publish and nothing else.
        """
        topics: list[str] = []
        for control in self._controls:
            topic = self._control_topic(control)
            topics.extend([topic, f"{topic}/meta", f"{topic}/meta/error"])
            if not control.meta.get("readonly"):
                topics.append(f"{topic}/on")
        topics.append(f"{self._base}/meta")
        return topics


class PerKeyDevice(_PublishedDevice):
    """
    One WB device per Waterius key: read-only mirrors of its channels plus its send status.
    """

    def __init__(self, client: MQTTClient, index: int, device: Device, last_sent: str = "") -> None:
        self._config_device = device
        self._channels: list[Control] = []
        self._controls_by_source_topic: dict[str, list[Control]] = {}
        self._build_channels()
        # Restored from persistent state, kept current so a reconnect republishes it.
        self._last_sent = last_sent
        self._last_error = ""  # filled by mark_failed, republished the same way
        super().__init__(
            client,
            base=f"/devices/{build_key_device_id(index)}",
            controls=self._channels + [KEY_LAST_SENT, KEY_LAST_ERROR],
            title=self._build_title(),
        )

    def _build_title(self) -> dict:
        # Without a device name, fall back to the cut key — the full one is a write credential
        # and /devices/+/meta is readable by the whole LAN.
        title_suffix = self._config_device.name or f"{key_prefix(self._config_device.key)}*"
        return {"en": f"Waterius - {title_suffix}", "ru": f"Ватериус - {title_suffix}"}

    def _build_channels(self) -> None:
        type_counts = Counter(channel.data_type for channel in self._config_device.channels)
        for channel_index, channel in enumerate(self._config_device.channels):
            units, base = DATA_TYPE_CONTROLS.get(
                channel.data_type,
                ("", {"en": f"Type {channel.data_type}", "ru": f"Тип {channel.data_type}"}),
            )
            # Disambiguate a repeated meter type by the short control name, not the full
            # topic: the UI cell truncates a long title on the tail that distinguishes it.
            suffix = f" ({channel.control})" if type_counts[channel.data_type] > 1 else ""
            title = {lang: text + suffix for lang, text in base.items()}
            # A generic "value" control with an explicit unit
            meta = {"type": "value", "readonly": True, "order": channel_index + 2, "title": title}
            if units:
                meta["units"] = units
            control = Control(f"ch{channel_index}", meta)
            self._channels.append(control)
            self._controls_by_source_topic.setdefault(channel.mqtt_topic, []).append(control)

    def _publish_initial_values(self) -> None:
        for control in self._channels:
            self._publish_control(control, "")
        # Both come off the instance, so a reconnect brings the card back as it was. The stamp
        # starts from the state file, the error text lives as long as the process.
        self._publish_control(KEY_LAST_SENT, self._last_sent)
        self._publish_control(KEY_LAST_ERROR, self._last_error)
        self._set_error(bool(self._last_error))

    def update_channel(self, mqtt_topic: str, raw_value: Optional[str]) -> None:
        """
        Mirror a source reading onto its channel control(s), if this device owns it.

        The value is passed through untouched, so the mirror always shows what the source
        control shows.
        """
        controls = self._controls_by_source_topic.get(mqtt_topic)
        if not controls:
            return
        value = "" if raw_value is None else raw_value
        for control in controls:
            self._publish_control(control, value)

    def _set_error(self, on: bool) -> None:
        """
        Toggle the WB error flag on the "Errors" control, shown red in the UI.

        The flag is per-control in WB, and a failed send does not make the channel data
        wrong, so only this control carries it.
        """
        self._set_control_error(KEY_LAST_ERROR, on)

    def mark_sent(self, timestamp: str) -> None:
        """
        A successful send for this device: stamp Last Sent, clear the error.

        The stamp is kept in the instance too, so a reconnect republishes it.
        """
        self._last_sent = timestamp
        self._last_error = ""
        self._publish_control(KEY_LAST_SENT, timestamp)
        self._publish_control(KEY_LAST_ERROR, "")
        self._set_error(False)

    def mark_failed(self, detail: str) -> None:
        """
        A failed send (API error or unavailable channels): show detail on "Errors".

        The text is kept in the instance for the same reason the stamp is — the clean slate of a
        reconnect wipes the card, while the service keeps its verdict in memory.
        """
        self._last_error = detail
        self._publish_control(KEY_LAST_ERROR, detail)
        self._set_error(True)


class IntegrationDevice(_PublishedDevice):
    """
    The control/status device: automatic-sending switch and read-only status.
    """

    def __init__(
        self, client: MQTTClient, on_toggle: Optional[Callable[[bool], None]] = None, version: str = ""
    ) -> None:
        super().__init__(
            client,
            base=INTEGRATION_DEVICE_BASE,
            controls=STATUS_CONTROLS,
            title={"en": "Waterius Integration", "ru": "Интеграция с Ватериус"},
        )
        self._on_toggle = on_toggle
        self._version = version

    def set_last_will(self) -> None:
        """
        Register the connection's Last Will, before the client connects.

        A connection carries one will, so it goes to the state control. If the daemon dies
        without clearing the flag, the broker raises it and the UI shows the integration red.
        A healthy start wipes the flag along with everything else.
        """
        topic = f"{self._control_topic(STATUS_STATE)}/meta/error"
        self._client.will_set(topic, _WILL_ERROR_FLAG, retain=True)

    def _publish_initial_values(self) -> None:
        self.set_state(STATE_INITIALIZING)
        self.set_error(False)
        self.set_enabled(False)
        self.set_current_time(NO_TIME)
        self.set_next_run(NO_TIME)
        self._publish_control(STATUS_VERSION, self._version)

    def _enabled_topic(self) -> str:
        return f"{self._control_topic(STATUS_ENABLED)}/on"

    def subscribe_switch(self) -> None:
        self._client.subscribe(self._enabled_topic())
        self._client.message_callback_add(self._enabled_topic(), self._on_enabled)

    def unsubscribe_switch(self) -> None:
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
        self._publish_control(STATUS_STATE, str(state))

    def set_enabled(self, on: bool) -> None:
        self._publish_control(STATUS_ENABLED, "1" if on else "0")

    def set_current_time(self, text: str) -> None:
        self._publish_control(STATUS_CURRENT_TIME, text)

    def set_next_run(self, text: str) -> None:
        self._publish_control(STATUS_NEXT_EXECUTION, text)

    def set_error(self, on: bool) -> None:
        """
        Toggle the WB error flag on the state control (non-empty = red in the UI).
        """
        self._set_control_error(STATUS_STATE, on)


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

    def set_last_will(self) -> None:
        self._integration_device.set_last_will()

    def create(self) -> None:
        self._integration_device.create()
        for key_device in self._key_devices:
            key_device.create()

    def subscribe_switch(self) -> None:
        """
        Subscribe to the automatic-sending switch. Only the integration device has one.
        """
        self._integration_device.subscribe_switch()

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
        self._integration_device.unsubscribe_switch()
        return clear_all(self._client, timeout)

    def remove(self) -> list[str]:
        """
        Take our devices off the broker on a clean stop, key devices first.

        Unlike ``clear`` this asks the broker for nothing. A stop happens on every config
        save, and a scan there would pay its timeout just as the broker goes away with the
        rest of the system.
        """
        self._integration_device.unsubscribe_switch()
        topics: list[str] = []
        for key_device in self._key_devices:
            topics.extend(key_device.remove())
        topics.extend(self._integration_device.remove())
        return topics
