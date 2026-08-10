"""
Load and validate /etc/wb-mqtt-waterius.conf.
"""

import json
import re
from collections.abc import Iterable
from typing import Optional, Union

from wb.mqtt_waterius.waterius_api import mask_key

DEFAULT_PATH = "/etc/wb-mqtt-waterius.conf"
TIME_RE = re.compile(r"^([01][0-9]|2[0-3]):([0-5][0-9])$")

# The Waterius protocol holds ch0..ch3, and the configurator schema caps the table at four
# rows. This check is for a hand-edited config file.
MAX_CHANNELS = 4

# Waterius data-type codes. A code outside the range does not exist and the cloud renders an
# unknown one as water, so a typo would silently land in the wrong meter.
DATA_TYPES = range(10)

# Config weekday names -> datetime.weekday() index (Monday=0 .. Sunday=6).
WEEKDAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]


class ConfigError(Exception):
    """
    Raised when the configuration is missing or invalid.
    """


class Channel:
    """
    One meter channel — a source MQTT topic, its Waterius data-type and serial.

    The topic is kept in the short `<device>/<control>` form the configurator writes and is
    expanded to a full MQTT topic on demand.

    Args:
        topic: source control as `<device>/<control>`
        data_type: Waterius data-type code, an int (the configurator always writes one)
        serial: optional meter serial number, empty is stored as None

    Examples:
        >>> channel = Channel("waterius-demo/cold_water", "0")
        >>> channel.device
        'waterius-demo'
        >>> channel.control
        'cold_water'
        >>> channel.mqtt_topic
        '/devices/waterius-demo/controls/cold_water'
        >>> channel.data_type
        0
    """

    def __init__(self, topic: str, data_type: Union[int, str], serial: Optional[str] = None) -> None:
        self.topic: str = topic
        self.data_type: int = int(data_type)
        self.serial: Optional[str] = serial or None

    @property
    def device(self) -> str:
        """
        Device id part of the source topic.
        """
        return self.topic.split("/", 1)[0]

    @property
    def control(self) -> str:
        """
        Control id part of the source topic.
        """
        return self.topic.split("/", 1)[1]

    @property
    def mqtt_topic(self) -> str:
        """
        Full MQTT topic of the source control.
        """
        return f"/devices/{self.device}/controls/{self.control}"


class Device:
    """
    One Waterius key, its display name, and its up-to-four channels.

    An empty name leaves the name set in the Waterius cabinet as it is.
    """

    def __init__(self, key: str, channels: list[Channel], name: str = "") -> None:
        self.key: str = key
        self.channels: list[Channel] = channels
        self.name: str = name


class Config:
    """
    The whole configuration: send time, weekdays, and the list of devices.
    """

    def __init__(self, send_time: str, devices: list[Device], days: Iterable[int]) -> None:
        self.send_time: str = send_time
        self.devices: list[Device] = devices
        self.days: set[int] = set(days)

    @property
    def send_hour_minute(self) -> tuple[int, int]:
        match = TIME_RE.match(self.send_time)
        return int(match.group(1)), int(match.group(2))

    def all_topics(self) -> set[str]:
        topics: set[str] = set()
        for device in self.devices:
            for channel in device.channels:
                topics.add(channel.mqtt_topic)
        return topics


def _parse_channel(raw_channel: dict) -> Channel:
    topic = raw_channel.get("topic")
    if not topic or "/" not in topic:
        raise ConfigError(f"channel topic must be 'device/control', got {topic!r}")
    if "data_type" not in raw_channel:
        raise ConfigError(f"channel {topic!r} has no data_type")
    try:
        data_type = int(raw_channel["data_type"])
    except (TypeError, ValueError) as exc:
        raise ConfigError(
            f"channel {topic!r} has a non-numeric data_type {raw_channel['data_type']!r}"
        ) from exc
    if data_type not in DATA_TYPES:
        raise ConfigError(
            f"channel {topic!r} has unknown data_type {data_type}, "
            f"expected {min(DATA_TYPES)}..{max(DATA_TYPES)}"
        )
    return Channel(topic, data_type, raw_channel.get("serial"))


def _parse_device(raw_device: dict) -> Device:
    key = raw_device.get("key")
    if not key:
        raise ConfigError("device has no key")
    channels = [_parse_channel(raw_channel) for raw_channel in raw_device.get("channels", [])]
    if not channels:
        raise ConfigError(f"device {mask_key(key)!r} has no channels")
    if len(channels) > MAX_CHANNELS:
        raise ConfigError(f"device {mask_key(key)!r} has {len(channels)} channels, max is {MAX_CHANNELS}")
    name = raw_device.get("name") or ""
    return Device(key, channels, str(name).strip())


def _find_duplicate_key(devices: list[Device]) -> Optional[str]:
    """
    Return the first key used by more than one device, None when all keys are unique.

    Two devices sharing a key collapse the per-device daily dedup: the second one is skipped
    as already sent today and never updates its display.

    Examples:
        >>> _find_duplicate_key([Device("A", []), Device("B", []), Device("A", [])])
        'A'
        >>> _find_duplicate_key([Device("A", []), Device("B", [])]) is None
        True
    """
    seen: set[str] = set()
    for device in devices:
        if device.key in seen:
            return device.key
        seen.add(device.key)
    return None


def _parse_send_time(data: dict) -> str:
    """
    Pull the daily send time out of the raw config.

    Examples:
        >>> _parse_send_time({"send_time": "03:00"})
        '03:00'
    """
    send_time = data.get("send_time")
    if not isinstance(send_time, str):
        raise ConfigError("send_time is required")
    if not TIME_RE.match(send_time):
        raise ConfigError(f"send_time must be HH:MM, got {send_time!r}")
    return send_time


def _parse_days(data: dict) -> set[int]:
    """
    Convert the configured weekday names to datetime.weekday() indices.

    Days are required and at least one name must be known. An unknown name is dropped, but a
    list of nothing but unknown names is an error rather than a silent "every day".

    Examples:
        >>> sorted(_parse_days({"days": ["monday", "sunday"]}))
        [0, 6]
    """
    raw_days = data.get("days")
    if not isinstance(raw_days, list) or not raw_days:
        raise ConfigError("at least one send day must be selected")
    days = {WEEKDAYS.index(day) for day in raw_days if day in WEEKDAYS}
    if not days:
        raise ConfigError("send days contain no valid weekday")
    return days


def parse_config(data: dict) -> Config:
    """
    Build a Config from a plain dict, the already-parsed JSON of the config file.

    Examples:
        >>> config = parse_config(
        ...     {
        ...         "send_time": "03:00",
        ...         "days": ["monday", "friday"],
        ...         "devices": [
        ...             {
        ...                 "key": "KEY",
        ...                 "name": "Boiler",
        ...                 "channels": [{"topic": "wb-map12/ch1", "data_type": 0}],
        ...             }
        ...         ],
        ...     }
        ... )
        >>> config.send_hour_minute, sorted(config.days)
        ((3, 0), [0, 4])
        >>> config.devices[0].name, config.devices[0].channels[0].mqtt_topic
        ('Boiler', '/devices/wb-map12/controls/ch1')
    """
    send_time = _parse_send_time(data)
    raw_devices = data.get("devices")
    if not isinstance(raw_devices, list):
        raise ConfigError("devices is required, use an empty list when nothing is configured")
    # An empty list is the fresh-install state, the daemon idles instead of crash-looping.
    devices = [_parse_device(raw_device) for raw_device in raw_devices]
    duplicate = _find_duplicate_key(devices)
    if duplicate:
        raise ConfigError(f"duplicate device key {mask_key(duplicate)!r}")
    return Config(send_time=send_time, devices=devices, days=_parse_days(data))


def load_config(path: Optional[str] = None) -> Config:
    """
    Read and validate the config file, /etc/wb-mqtt-waterius.conf unless another path is given.

    Every failure comes out as ConfigError, so the caller has one exception to catch whether the
    file is missing, is not JSON, or is JSON that does not describe a usable configuration.
    """
    path = path or DEFAULT_PATH
    try:
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError as exc:
        raise ConfigError(f"config file not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ConfigError(f"invalid JSON in {path}: {exc}") from exc
    return parse_config(data)
