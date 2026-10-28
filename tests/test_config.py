import json
from pathlib import Path
from typing import Optional

import pytest

from wb.mqtt_waterius import config

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "wb-mqtt-waterius.schema.json"
ABSENT = object()


def _valid_config() -> dict:
    return {
        "sendTime": "03:30",
        "daysOfWeek": ["monday"],
        "devices": [
            {
                "key": "K",
                "name": "",
                "channels": [{"mqttTopicName": "waterius-demo/cold_water", "dataType": 0}],
            },
        ],
    }


def _with(base: dict, **fields: object) -> dict:
    # base with top-level fields replaced; ABSENT removes one
    for key, value in fields.items():
        if value is ABSENT:
            base.pop(key)
        else:
            base[key] = value
    return base


def _device(**fields: object) -> dict:
    return _with(_valid_config()["devices"][0], **fields)


def _channel(**fields: object) -> dict:
    return _with({"mqttTopicName": "d/c", "dataType": 0}, **fields)


def test_parse_valid_config() -> None:
    cfg = config.parse_config(_valid_config())
    assert cfg.send_hour_minute == (3, 30)
    assert len(cfg.devices) == 1
    assert cfg.devices[0].channels[0].source == "waterius-demo/cold_water"


def test_device_name_is_parsed_and_trimmed() -> None:
    raw = _valid_config()
    raw["devices"][0]["name"] = "  Котельная  "
    assert config.parse_config(raw).devices[0].name == "Котельная"


@pytest.mark.parametrize("raw_name", [None, "", "   "], ids=["absent", "empty", "blank"])
def test_device_name_defaults_to_empty(raw_name: Optional[str]) -> None:
    # The schema requires the field but allows an empty value: that is how the user says
    # "keep the name I set in the Waterius cabinet". A config without the key at all still
    # parses — the driver never depends on the name.
    raw = _valid_config()
    if raw_name is not None:
        raw["devices"][0]["name"] = raw_name
    assert config.parse_config(raw).devices[0].name == ""


def test_all_topics() -> None:
    cfg = config.parse_config(_valid_config())
    assert cfg.all_topics() == {"/devices/waterius-demo/controls/cold_water"}


@pytest.mark.parametrize(
    "bad",
    [
        pytest.param(_with(_valid_config(), sendTime="25:00"), id="bad-time"),
        pytest.param(_with(_valid_config(), sendTime=ABSENT), id="send-time-absent"),
        pytest.param(_with(_valid_config(), devices=ABSENT), id="devices-key-absent"),
        pytest.param(_with(_valid_config(), devices=["K1"]), id="device-not-an-object"),
        pytest.param(_with(_valid_config(), devices=[_device(channels=[])]), id="device-without-channels"),
        pytest.param(_with(_valid_config(), devices=[_device(key=ABSENT)]), id="device-without-key"),
        pytest.param(_with(_valid_config(), devices=[_device(key=["K"])]), id="key-not-a-string"),
        pytest.param(_with(_valid_config(), devices=[_device(name=5)]), id="name-not-a-string"),
        pytest.param(_with(_valid_config(), devices=[_device(channels=None)]), id="channels-null"),
        pytest.param(_with(_valid_config(), devices=[_device(channels=5)]), id="channels-not-a-list"),
        pytest.param(
            _with(_valid_config(), devices=[_device(channels=[_channel(mqttTopicName="nodash")])]),
            id="channel-bad-topic",
        ),
        pytest.param(
            _with(_valid_config(), devices=[_device(channels=[_channel(mqttTopicName=5)])]),
            id="channel-topic-not-a-string",
        ),
        pytest.param(
            _with(_valid_config(), devices=[_device(channels=[_channel(dataType=ABSENT)])]),
            id="channel-without-data-type",
        ),
        pytest.param(
            _with(_valid_config(), devices=[_device(channels=[_channel(dataType="abc")])]),
            id="channel-non-numeric-data-type",
        ),
        pytest.param(
            _with(_valid_config(), devices=[_device(channels=[_channel(dataType=42)])]),
            id="channel-unknown-data-type",
        ),
        pytest.param(
            _with(_valid_config(), devices=[_device(channels=[_channel(serial="1" * 16)])]),
            id="serial-too-long",
        ),
        pytest.param(
            _with(
                _valid_config(),
                devices=[_device(channels=[_channel(mqttTopicName=f"d/c{i}") for i in range(5)])],
            ),
            id="too-many-channels",
        ),
        pytest.param(_with(_valid_config(), daysOfWeek=[]), id="empty-days"),
        pytest.param(_with(_valid_config(), daysOfWeek=["notaday"]), id="days-all-invalid"),
        pytest.param(_with(_valid_config(), daysOfWeek=["monday", "tuesdya"]), id="days-partially-invalid"),
        pytest.param(_with(_valid_config(), daysOfWeek=["monday", 3]), id="days-not-a-name"),
    ],
)
def test_load_config_rejects_what_the_schema_forbids(tmp_path: Path, bad: dict) -> None:
    # Every way a hand-edited file can break the shape the web UI guarantees ends in the one
    # ConfigError the daemon maps to exit code 6, never in a traceback. The parser itself checks
    # none of this, the schema does, as in wb-mqtt-serial.
    path = tmp_path / "wb-mqtt-waterius.conf"
    path.write_text(json.dumps(bad), encoding="utf-8")
    with pytest.raises(config.ConfigError):
        config.load_config(str(path), schema_path=str(SCHEMA_PATH))


def test_duplicate_key_raises() -> None:
    # The one thing a schema cannot say, so the parser says it.
    data = _with(_valid_config(), devices=[_device(), _device(channels=[_channel(mqttTopicName="d/b")])])
    with pytest.raises(config.ConfigError, match="duplicate device key"):
        config.parse_config(data)


def test_empty_device_list_allowed() -> None:
    # Fresh-install state: must parse, the daemon exits with 7 instead of crash-looping.
    data = {"sendTime": "03:00", "daysOfWeek": ["monday"], "devices": []}
    assert config.parse_config(data).devices == []


def test_days_subset_parsed_to_weekday_indices() -> None:
    cfg = config.parse_config(
        {"sendTime": "03:00", "devices": [], "daysOfWeek": ["monday", "friday", "sunday"]}
    )
    assert cfg.days_of_week == {0, 4, 6}  # Monday=0, Friday=4, Sunday=6


def test_load_config_reads_file(tmp_path: Path) -> None:
    path = tmp_path / "wb-mqtt-waterius.conf"
    path.write_text(json.dumps(_valid_config()), encoding="utf-8")
    cfg = config.load_config(str(path), schema_path=str(SCHEMA_PATH))
    assert cfg.send_hour_minute == (3, 30)
    assert len(cfg.devices) == 1


def test_load_config_missing_file_raises(tmp_path: Path) -> None:
    # The daemon must exit with a config error, not a traceback, when the file was removed.
    with pytest.raises(config.ConfigError):
        config.load_config(str(tmp_path / "absent.conf"))


def test_load_config_broken_json_raises(tmp_path: Path) -> None:
    # Hand-edited file with a trailing comma or a missing brace: one ConfigError for the caller.
    path = tmp_path / "wb-mqtt-waterius.conf"
    path.write_text('{"sendTime": "03:00",}', encoding="utf-8")
    with pytest.raises(config.ConfigError):
        config.load_config(str(path))


def test_channel_serial_optional() -> None:
    data = {
        "sendTime": "03:00",
        "daysOfWeek": ["monday"],
        "devices": [
            {
                "key": "K",
                "channels": [
                    {"mqttTopicName": "d/a", "dataType": 0, "serial": "1001"},
                    {"mqttTopicName": "d/b", "dataType": 1},
                ],
            }
        ],
    }
    cfg = config.parse_config(data)
    assert cfg.devices[0].channels[0].serial == "1001"
    assert cfg.devices[0].channels[1].serial is None


@pytest.mark.parametrize("content", [b"[]", b"\xff\xfe not utf-8"], ids=["not-an-object", "not-utf8"])
def test_load_config_unusable_content_raises(tmp_path: Path, content: bytes) -> None:
    path = tmp_path / "wb-mqtt-waterius.conf"
    path.write_bytes(content)
    with pytest.raises(config.ConfigError):
        config.load_config(str(path), schema_path=str(SCHEMA_PATH))


def test_load_config_unreadable_path_raises(tmp_path: Path) -> None:
    # A directory where the file should be is an OSError other than "not found".
    with pytest.raises(config.ConfigError):
        config.load_config(str(tmp_path))
