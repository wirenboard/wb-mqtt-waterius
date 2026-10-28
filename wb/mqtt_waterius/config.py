"""
Load and validate /etc/wb-mqtt-waterius.conf.
"""

import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Optional, Union

import jsonschema

from wb.mqtt_waterius.waterius_api import short_key

logger = logging.getLogger(__name__)

DEFAULT_PATH = "/etc/wb-mqtt-waterius.conf"
SCHEMA_PATH = "/usr/share/wb-mqtt-confed/schemas/wb-mqtt-waterius.schema.json"

# Config weekday names -> datetime.weekday() index (Monday=0 .. Sunday=6).
WEEKDAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]


class ConfigError(Exception):
    """
    Raised when the configuration is missing or invalid.
    """


class Channel:
    """
    One meter channel — a source control, its Waterius data-type and serial.

    The source is kept in the short `<device>/<control>` form the configurator writes and is
    expanded to a full MQTT topic on demand, which is what `mqtt_topic` is for.

    Args:
        source: source control as `<device>/<control>`
        data_type: Waterius data-type code, an int (the configurator always writes one)
        serial: optional meter serial number, empty is stored as None

    Examples:
        >>> channel = Channel("waterius-demo/cold_water", "0")
        >>> channel.control
        'cold_water'
        >>> channel.mqtt_topic
        '/devices/waterius-demo/controls/cold_water'
        >>> channel.data_type
        0
    """

    def __init__(self, source: str, data_type: Union[int, str], serial: Optional[str] = None) -> None:
        self.source: str = source
        self.data_type: int = int(data_type)
        self.serial: Optional[str] = serial or None

    @property
    def control(self) -> str:
        """
        Control id part of the source, the only part anything outside needs on its own.
        """
        return self.source.split("/", 1)[1]

    @property
    def mqtt_topic(self) -> str:
        """
        Full MQTT topic of the source control.
        """
        device, control = self.source.split("/", 1)
        return f"/devices/{device}/controls/{control}"


@dataclass(eq=False)
class Device:
    """
    One Waterius key, its display name, and its up-to-four channels.

    An empty name leaves the name set in the Waterius cabinet as it is.
    """

    # The key is a write credential, so it stays out of the generated repr.
    key: str = field(repr=False)
    channels: list[Channel]
    name: str = ""


class Config:
    """
    The whole configuration: send time, weekdays, and the list of devices.
    """

    def __init__(self, send_time: str, devices: list[Device], days_of_week: Iterable[int]) -> None:
        self.send_time: str = send_time
        self.devices: list[Device] = devices
        self.days_of_week: set[int] = set(days_of_week)

    @property
    def send_hour_minute(self) -> tuple[int, int]:
        hour, minute = self.send_time.split(":")
        return int(hour), int(minute)

    def all_topics(self) -> set[str]:
        topics: set[str] = set()
        for device in self.devices:
            for channel in device.channels:
                topics.add(channel.mqtt_topic)
        return topics


def _parse_channel(raw_channel: dict) -> Channel:
    return Channel(raw_channel["mqttTopicName"], raw_channel["dataType"], raw_channel.get("serial"))


def _parse_device(raw_device: dict) -> Device:
    channels = [_parse_channel(raw_channel) for raw_channel in raw_device["channels"]]
    return Device(raw_device["key"], channels, raw_device.get("name", "").strip())


def _find_duplicate_key(devices: list[Device]) -> Optional[str]:
    """
    Return the first key used by more than one device, None when all keys are unique.

    Two devices sharing a key collapse into one in the saved state, so the second one counts
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


def parse_config(data: dict) -> Config:
    """
    Build a Config from a plain dict, the already-parsed JSON of the config file.

    The shape of the data — types, required fields, ranges — is the schema's business, see
    load_config. Here only what a schema cannot say is checked: a key used by two devices.

    Examples:
        >>> config = parse_config(
        ...     {
        ...         "sendTime": "03:00",
        ...         "daysOfWeek": ["monday", "friday"],
        ...         "devices": [
        ...             {
        ...                 "key": "KEY",
        ...                 "name": "Boiler",
        ...                 "channels": [{"mqttTopicName": "wb-map12/ch1", "dataType": 0}],
        ...             }
        ...         ],
        ...     }
        ... )
        >>> config.send_hour_minute, sorted(config.days_of_week)
        ((3, 0), [0, 4])
        >>> config.devices[0].name, config.devices[0].channels[0].mqtt_topic
        ('Boiler', '/devices/wb-map12/controls/ch1')
    """
    # An empty list is the fresh-install state, the daemon flags it in the UI and exits with 7.
    devices = [_parse_device(raw_device) for raw_device in data["devices"]]
    duplicate = _find_duplicate_key(devices)
    if duplicate:
        raise ConfigError(f"duplicate device key {short_key(duplicate)}")
    days_of_week = {WEEKDAYS.index(day) for day in data["daysOfWeek"]}
    return Config(send_time=data["sendTime"], devices=devices, days_of_week=days_of_week)


def _check_against_schema(data: object, path: str, schema_path: str) -> None:
    """
    Reject what the confed schema forbids: types, required fields, ranges and lengths, which
    the parser takes on trust, as wb-mqtt-serial does. Without the schema (a dev box) nothing
    checks the shape of the file.
    """
    try:
        with open(schema_path, encoding="utf-8") as handle:
            jsonschema.validate(data, json.load(handle))
    except FileNotFoundError:
        logger.warning("Schema %s not found, the config is not checked against it", schema_path)
    except jsonschema.ValidationError as exc:
        where = ".".join(str(step) for step in exc.absolute_path) or "config"
        raise ConfigError(f"{path}: {where}: {exc.message}") from exc
    except (OSError, ValueError, jsonschema.SchemaError) as exc:
        raise ConfigError(f"cannot use schema {schema_path}: {exc}") from exc


def load_config(path: Optional[str] = None, schema_path: str = SCHEMA_PATH) -> Config:
    """
    Read and validate the config file, /etc/wb-mqtt-waterius.conf unless another path is given.

    Every failure comes out as ConfigError, so the caller has one exception to catch whether the
    file is missing, is not JSON, or is JSON that does not describe a usable configuration.
    """
    path = path or DEFAULT_PATH
    try:
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
    except OSError as exc:
        raise ConfigError(f"cannot read config file {path}: {exc}") from exc
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ConfigError(f"invalid JSON in {path}: {exc}") from exc
    _check_against_schema(data, path, schema_path)
    return parse_config(data)
