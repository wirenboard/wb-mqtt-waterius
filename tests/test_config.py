import json
from pathlib import Path
from typing import Optional

import pytest

from wb.mqtt_waterius import config


def _valid_config() -> dict:
    return {
        "send_time": "03:30",
        "days": ["monday"],
        "devices": [
            {"key": "K", "channels": [{"topic": "waterius-demo/cold_water", "data_type": 0}]},
        ],
    }


def test_parse_valid_config() -> None:
    cfg = config.parse_config(_valid_config())
    assert cfg.send_hour_minute == (3, 30)
    assert len(cfg.devices) == 1
    assert cfg.devices[0].channels[0].topic == "waterius-demo/cold_water"


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
        pytest.param({"send_time": "25:00", "devices": []}, id="bad-time"),
        pytest.param({"days": ["monday"], "devices": []}, id="send-time-absent"),
        pytest.param({"send_time": "03:00", "days": ["monday"]}, id="devices-key-absent"),
        pytest.param(
            {"send_time": "03:00", "devices": [{"key": "K", "channels": []}]}, id="device-without-channels"
        ),
        pytest.param(
            {"send_time": "03:00", "devices": [{"channels": [{"topic": "d/c", "data_type": 0}]}]},
            id="device-without-key",
        ),
        pytest.param(
            {
                "send_time": "03:00",
                "devices": [{"key": "K", "channels": [{"topic": "nodash", "data_type": 0}]}],
            },
            id="channel-bad-topic",
        ),
        pytest.param(
            {"send_time": "03:00", "devices": [{"key": "K", "channels": [{"topic": "d/c"}]}]},
            id="channel-without-type",
        ),
        pytest.param(
            {
                "send_time": "03:00",
                "devices": [{"key": "K", "channels": [{"topic": "d/c", "data_type": "abc"}]}],
            },
            id="channel-non-numeric-type",
        ),
        pytest.param(
            {
                "send_time": "03:00",
                "devices": [{"key": "K", "channels": [{"topic": "d/c", "data_type": 42}]}],
            },
            id="channel-unknown-type",
        ),
        pytest.param({"send_time": "03:00", "devices": [], "days": []}, id="empty-days"),
        pytest.param({"send_time": "03:00", "devices": [], "days": ["notaday"]}, id="days-all-invalid"),
        pytest.param(
            {
                "send_time": "03:00",
                "days": ["monday"],
                "devices": [
                    {"key": "K", "channels": [{"topic": f"d/c{i}", "data_type": 0} for i in range(5)]}
                ],
            },
            id="too-many-channels",
        ),
        pytest.param(
            {
                "send_time": "03:00",
                "days": ["monday"],
                "devices": [
                    {"key": "DUP", "channels": [{"topic": "d/a", "data_type": 0}]},
                    {"key": "DUP", "channels": [{"topic": "d/b", "data_type": 1}]},
                ],
            },
            id="duplicate-key",
        ),
    ],
)
def test_invalid_config_raises(bad: dict) -> None:
    with pytest.raises(config.ConfigError):
        config.parse_config(bad)


def test_empty_device_list_allowed() -> None:
    # Fresh-install state: must parse, the daemon idles instead of crash-looping.
    data = {"send_time": "03:00", "days": ["monday"], "devices": []}
    assert config.parse_config(data).devices == []


def test_days_subset_parsed_to_weekday_indices() -> None:
    cfg = config.parse_config({"send_time": "03:00", "devices": [], "days": ["monday", "friday", "sunday"]})
    assert cfg.days == {0, 4, 6}  # Monday=0, Friday=4, Sunday=6


def test_load_config_reads_file(tmp_path: Path) -> None:
    path = tmp_path / "wb-mqtt-waterius.conf"
    path.write_text(json.dumps(_valid_config()), encoding="utf-8")
    cfg = config.load_config(str(path))
    assert cfg.send_hour_minute == (3, 30)
    assert len(cfg.devices) == 1


def test_load_config_missing_file_raises(tmp_path: Path) -> None:
    # The daemon must exit with a config error, not a traceback, when the file was removed.
    with pytest.raises(config.ConfigError):
        config.load_config(str(tmp_path / "absent.conf"))


def test_load_config_broken_json_raises(tmp_path: Path) -> None:
    # Hand-edited file with a trailing comma or a missing brace: one ConfigError for the caller.
    path = tmp_path / "wb-mqtt-waterius.conf"
    path.write_text('{"send_time": "03:00",}', encoding="utf-8")
    with pytest.raises(config.ConfigError):
        config.load_config(str(path))


def test_channel_serial_optional() -> None:
    data = {
        "send_time": "03:00",
        "days": ["monday"],
        "devices": [
            {
                "key": "K",
                "channels": [
                    {"topic": "d/a", "data_type": 0, "serial": "1001"},
                    {"topic": "d/b", "data_type": 1},
                ],
            }
        ],
    }
    cfg = config.parse_config(data)
    assert cfg.devices[0].channels[0].serial == "1001"
    assert cfg.devices[0].channels[1].serial is None
