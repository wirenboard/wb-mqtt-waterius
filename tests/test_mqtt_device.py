import json
from collections.abc import Callable
from typing import Optional

import pytest

from tests.conftest import ALL_DAYS, FakeClient
from wb.mqtt_waterius import mqtt_device
from wb.mqtt_waterius.config import Channel, Config, Device

INTEGRATION = "/devices/wb-mqtt-waterius"
KEY_DEVICE1 = "/devices/wb-mqtt-waterius_1"
KEY_DEVICE2 = "/devices/wb-mqtt-waterius_2"


class _Msg:
    def __init__(self, payload: str, topic: str = "") -> None:
        self.payload = payload.encode()
        self.topic = topic


def _matches(wildcard: str, topic: str) -> bool:
    wildcard_parts, topic_parts = wildcard.split("/"), topic.split("/")
    return len(wildcard_parts) == len(topic_parts) and all(
        part in ("+", topic_part) for part, topic_part in zip(wildcard_parts, topic_parts)
    )


class _DeliveringClient(FakeClient):
    """
    FakeClient that answers a subscription with retained messages, the way a broker does.
    """

    def __init__(self, retained: dict) -> None:
        super().__init__()
        self._retained = retained

    def subscribe(self, topic: str) -> None:
        super().subscribe(topic)
        callback = self.callbacks.get(topic)
        if callback is None:
            return
        for retained_topic, payload in self._retained.items():
            if _matches(topic, retained_topic):
                callback(self, None, _Msg(payload, retained_topic))


def _devices(
    client: FakeClient,
    config: Optional[Config] = None,
    on_toggle: Optional[Callable[[bool], None]] = None,
) -> mqtt_device.WateriusDevices:
    return mqtt_device.WateriusDevices(client, on_toggle=on_toggle, config=config, version="1.0.0")


def _single_config() -> Config:
    return Config(
        "03:00", [Device("K1", [Channel("dev/cold", 0), Channel("dev/elec", 2)])], days_of_week=ALL_DAYS
    )


def test_integration_meta_title_and_version() -> None:
    client = FakeClient()
    _devices(client, _single_config()).publish_meta()
    meta = json.loads(client.last(f"{INTEGRATION}/meta"))
    assert meta["driver"] == "wb-mqtt-waterius"
    assert meta["title"] == {"en": "Waterius Integration", "ru": "Интеграция с Ватериус"}
    assert client.last(f"{INTEGRATION}/controls/version") == "1.0.0"


def test_integration_error_flag_toggle() -> None:
    client = FakeClient()
    devices = _devices(client, _single_config())
    devices.set_integration_error(True)
    assert client.last(f"{INTEGRATION}/controls/state/meta/error") == "r"
    devices.set_state(mqtt_device.STATE_HAS_ERRORS)
    assert client.last(f"{INTEGRATION}/controls/state") == str(mqtt_device.STATE_HAS_ERRORS)
    devices.set_integration_error(False)
    assert client.last(f"{INTEGRATION}/controls/state/meta/error") == ""


def test_set_enabled_and_state_setters() -> None:
    client = FakeClient()
    devices = _devices(client, _single_config())
    devices.set_enabled(False)
    assert client.last(f"{INTEGRATION}/controls/enabled") == "0"
    devices.set_enabled(True)
    assert client.last(f"{INTEGRATION}/controls/enabled") == "1"
    devices.set_state(mqtt_device.STATE_DISABLED)
    assert client.last(f"{INTEGRATION}/controls/state") == str(mqtt_device.STATE_DISABLED)


def test_time_controls_publish() -> None:
    client = FakeClient()
    devices = _devices(client, _single_config())
    devices.set_current_time("Thursday 2026-07-16 18:37")
    devices.set_next_run("Friday 2026-07-17 12:00")
    assert client.last(f"{INTEGRATION}/controls/current_time") == "Thursday 2026-07-16 18:37"
    assert client.last(f"{INTEGRATION}/controls/next_execution") == "Friday 2026-07-17 12:00"


def test_enable_toggle_callback_fires() -> None:
    client = FakeClient()
    toggled = []
    devices = _devices(client, _single_config(), on_toggle=toggled.append)
    devices.subscribe()
    on_enabled = client.callbacks[f"{INTEGRATION}/controls/enabled/on"]
    on_enabled(None, None, _Msg("0"))
    on_enabled(None, None, _Msg("1"))
    assert toggled == [False, True]


def test_mark_device_sent_stamps_and_clears() -> None:
    client = FakeClient()
    devices = _devices(client, _single_config())
    devices.mark_device_failed(0, "HTTP 500")  # fail first, so there is an error to clear
    devices.mark_device_sent(0, "2026-01-01 03:00:00")
    assert client.last(f"{KEY_DEVICE1}/controls/last_sent") == "2026-01-01 03:00:00"
    assert client.last(f"{KEY_DEVICE1}/controls/last_error") == ""
    assert client.last(f"{KEY_DEVICE1}/controls/last_error/meta/error") == ""


def test_last_sent_is_restored_per_key_device() -> None:
    # The persisted stamps come back on startup so a restart does not blank "Last Sent".
    # A key added since the last send has no stamp yet, and its control stays empty.
    config = Config(
        "03:00",
        [Device("K1", [Channel("dev/a", 0)]), Device("K2", [Channel("dev/b", 0)])],
        days_of_week=ALL_DAYS,
    )
    client = FakeClient()
    devices = mqtt_device.WateriusDevices(
        client, config=config, version="1.0.0", last_sent=["2026-01-01 03:00:00"]
    )
    devices.publish_meta()
    assert client.last(f"{KEY_DEVICE1}/controls/last_sent") == "2026-01-01 03:00:00"
    assert client.last(f"{KEY_DEVICE2}/controls/last_sent") == ""


def test_failed_send_flags_only_last_error() -> None:
    client = FakeClient()
    devices = _devices(client, _single_config())
    devices.mark_device_failed(0, "No data from channels: dev/cold")
    assert client.last(f"{KEY_DEVICE1}/controls/last_error") == "No data from channels: dev/cold"
    assert client.last(f"{KEY_DEVICE1}/controls/last_error/meta/error") == "r"
    # channels and last_sent carry no error flag — the send failed, the data isn't wrong
    for control in ("ch0", "ch1", "last_sent"):
        assert client.last(f"{KEY_DEVICE1}/controls/{control}/meta/error") in (None, "")


def test_key_device_title_masks_key() -> None:
    # A real 32-char key: the title carries only its masked prefix, never the full credential.
    config = Config(
        "03:00", [Device("01234567890123456789012345678901", [Channel("dev/cold", 0)])], days_of_week=ALL_DAYS
    )
    client = FakeClient()
    _devices(client, config).publish_meta()
    meta = json.loads(client.last(f"{KEY_DEVICE1}/meta"))
    assert meta["title"] == {"en": "Waterius - 01234", "ru": "Ватериус - 01234"}


def test_key_device_title_prefers_device_name() -> None:
    # A configured name replaces the masked key, and the "Waterius - " prefix stays so our
    # devices remain recognizable in the flat device list.
    config = Config(
        "03:00",
        [Device("01234567890123456789012345678901", [Channel("dev/cold", 0)], name="Котельная")],
        days_of_week=ALL_DAYS,
    )
    client = FakeClient()
    _devices(client, config).publish_meta()
    meta = json.loads(client.last(f"{KEY_DEVICE1}/meta"))
    assert meta["title"] == {"en": "Waterius - Котельная", "ru": "Ватериус - Котельная"}


def test_each_key_becomes_its_own_device() -> None:
    # Two config entries become two devices, and each device gets the channel of its own entry.
    config = Config(
        "03:00",
        [Device("K1", [Channel("dev/a", 0)]), Device("K2", [Channel("dev/b", 2)])],
        days_of_week=ALL_DAYS,
    )
    client = FakeClient()
    _devices(client, config).publish_meta()
    assert client.last(f"{KEY_DEVICE1}/meta") is not None
    assert client.last(f"{KEY_DEVICE2}/meta") is not None
    first = json.loads(client.last(f"{KEY_DEVICE1}/controls/ch0/meta"))
    second = json.loads(client.last(f"{KEY_DEVICE2}/controls/ch0/meta"))
    assert first["title"]["en"] == "Cold Water"
    assert second["title"]["en"] == "Electricity"


@pytest.mark.parametrize(
    "data_type, units",
    [
        (0, "m^3"),
        (1, "m^3"),
        (2, "kWh"),
        (3, "m^3"),
        (4, "Gcal"),
        (5, "kWh"),
        (6, "kWh"),
        (7, "kWh"),
        (8, "kWh"),
        (9, "m^3"),
    ],
)
def test_key_device_channel_units_by_type(data_type: int, units: str) -> None:
    config = Config("03:00", [Device("K1", [Channel("dev/c", data_type)])], days_of_week=ALL_DAYS)
    client = FakeClient()
    _devices(client, config).publish_meta()
    meta = json.loads(client.last(f"{KEY_DEVICE1}/controls/ch0/meta"))
    assert meta["type"] == "value"
    assert meta["units"] == units


def test_duplicate_type_within_key_gets_source_suffix() -> None:
    config = Config(
        "03:00", [Device("K1", [Channel("dev/cold1", 0), Channel("dev/cold2", 0)])], days_of_week=ALL_DAYS
    )
    client = FakeClient()
    _devices(client, config).publish_meta()
    first = json.loads(client.last(f"{KEY_DEVICE1}/controls/ch0/meta"))
    second = json.loads(client.last(f"{KEY_DEVICE1}/controls/ch1/meta"))
    assert first["title"]["en"] == "Cold Water (cold1)"
    assert second["title"]["en"] == "Cold Water (cold2)"


def test_update_channel_routes_to_owning_key_device() -> None:
    config = Config(
        "03:00",
        [Device("K1", [Channel("d1/cold", 0)]), Device("K2", [Channel("d2/cold", 0)])],
        days_of_week=ALL_DAYS,
    )
    client = FakeClient()
    devices = _devices(client, config)
    devices.update_channel("/devices/d2/controls/cold", "84.20")
    assert client.last(f"{KEY_DEVICE2}/controls/ch0") == "84.20"
    assert client.last(f"{KEY_DEVICE1}/controls/ch0") is None


def test_update_channel_ignores_unconfigured_topic() -> None:
    # The service hands every message it receives to the facade, and a topic can outlive its
    # channel — a config edit removes it while the broker still delivers the old retained value.
    client = FakeClient()
    devices = _devices(client, _single_config())
    devices.update_channel("/devices/other/controls/x", "5")
    for topic, *_ in client.published:
        assert not topic.startswith(f"{KEY_DEVICE1}/controls/ch")


def test_clear_all_clears_control_meta_value_and_error() -> None:
    # Removing a control takes three publishes: meta, value and the error flag. A leftover
    # meta would keep the control in the UI, a leftover error flag would keep it red.
    control = f"{KEY_DEVICE1}/controls/ch0"
    client = _DeliveringClient({f"{control}/meta": '{"type": "value"}'})
    mqtt_device.clear_all(client, settle=0.2)
    for topic in (f"{control}/meta", control, f"{control}/meta/error"):
        assert client.last(topic) == ""


def test_clear_all_publishes_empty_integration_meta() -> None:
    client = FakeClient()
    publish_results = mqtt_device.clear_all(client, settle=0)
    assert (f"{INTEGRATION}/meta", "", True, 1) in client.published
    assert publish_results


def test_clear_all_wipes_our_leftovers_and_spares_foreign_devices() -> None:
    # Ids come from the broker, not from the config, so a leftover gets wiped too: the user had
    # five keys, device _5 is still retained in the broker, and the shortened config would never
    # mention it again. An already removed control comes with an empty payload and is skipped,
    # a device outside our namespace is never touched.
    client = _DeliveringClient(
        {
            f"{INTEGRATION}/meta": '{"driver": "wb-mqtt-waterius"}',
            "/devices/wb-mqtt-waterius_5/meta": '{"driver": "wb-mqtt-waterius"}',
            "/devices/wb-mqtt-waterius_5/controls/ch0/meta": '{"type": "value"}',
            "/devices/wb-mqtt-waterius_5/controls/ch1/meta": "",
            "/devices/wb-mqtt-serial/meta": '{"driver": "wb-mqtt-serial"}',
            "/devices/wb-mqtt-serial/controls/temp/meta": '{"type": "value"}',
        }
    )
    mqtt_device.clear_all(client, settle=0.2)
    published = [topic for topic, *_ in client.published]
    assert f"{INTEGRATION}/meta" in published
    assert "/devices/wb-mqtt-waterius_5/meta" in published
    assert "/devices/wb-mqtt-waterius_5/controls/ch0/meta" in published
    assert "/devices/wb-mqtt-waterius_5/controls/ch1/meta" not in published
    assert "/devices/wb-mqtt-serial/meta" not in published
    assert "/devices/wb-mqtt-serial/controls/temp/meta" not in published
