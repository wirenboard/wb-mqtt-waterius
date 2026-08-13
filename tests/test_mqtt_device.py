import json
import time
from collections.abc import Callable
from typing import Any, Optional

import pytest

from tests.conftest import ALL_DAYS, FakeClient, Message, topic_matches
from wb.mqtt_waterius import mqtt_device
from wb.mqtt_waterius.config import Channel, Config, Device

MARKER_TOPIC = "/wb_mqtt_waterius/marker"
INTEGRATION_BASE = mqtt_device.INTEGRATION_DEVICE_BASE
KEY_DEVICE1_BASE = f"/devices/{mqtt_device.build_key_device_id(1)}"
KEY_DEVICE2_BASE = f"/devices/{mqtt_device.build_key_device_id(2)}"


class _DeliveringClient(FakeClient):  # pylint: disable=too-few-public-methods
    """
    FakeClient that answers a subscription with retained messages, the way a broker does.
    """

    def __init__(self, retained: dict[str, str]) -> None:
        super().__init__()
        self._retained = retained

    def subscribe(self, topic: str) -> None:
        super().subscribe(topic)
        if topic not in self.callbacks:
            return
        for retained_topic, payload in self._retained.items():
            if not topic_matches(topic, retained_topic):
                continue
            for wildcard, callback in list(self.callbacks.items()):
                if topic_matches(wildcard, retained_topic):
                    callback(self, None, Message(payload, retained_topic))


class _StaleMarkerClient(_DeliveringClient):  # pylint: disable=too-few-public-methods
    """
    Delivering client that answers the marker topic with someone else's token.
    """

    def publish(self, topic: str, payload: Any, retain: bool = False, qos: int = 0) -> None:
        if topic == MARKER_TOPIC:
            payload = b"other-scan"
        super().publish(topic, payload, retain, qos)


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
    meta = json.loads(client.last(f"{INTEGRATION_BASE}/meta"))
    assert meta["driver"] == "wb-mqtt-waterius"
    assert meta["title"] == {"en": "Waterius Integration", "ru": "Интеграция с Ватериус"}
    assert client.last(f"{INTEGRATION_BASE}/controls/version") == "1.0.0"


def test_integration_error_flag_toggle() -> None:
    client = FakeClient()
    devices = _devices(client, _single_config())
    devices.set_integration_error(True)
    assert client.last(f"{INTEGRATION_BASE}/controls/state/meta/error") == "w"
    devices.set_state(mqtt_device.STATE_HAS_ERRORS)
    assert client.last(f"{INTEGRATION_BASE}/controls/state") == str(mqtt_device.STATE_HAS_ERRORS)
    devices.set_integration_error(False)
    assert client.last(f"{INTEGRATION_BASE}/controls/state/meta/error") == ""


def test_set_enabled_and_state_setters() -> None:
    client = FakeClient()
    devices = _devices(client, _single_config())
    devices.set_enabled(False)
    assert client.last(f"{INTEGRATION_BASE}/controls/enabled") == "0"
    devices.set_enabled(True)
    assert client.last(f"{INTEGRATION_BASE}/controls/enabled") == "1"
    devices.set_state(mqtt_device.STATE_DISABLED)
    assert client.last(f"{INTEGRATION_BASE}/controls/state") == str(mqtt_device.STATE_DISABLED)


def test_time_controls_publish() -> None:
    client = FakeClient()
    devices = _devices(client, _single_config())
    devices.set_current_time("Thursday 2026-07-16 18:37")
    devices.set_next_run("Friday 2026-07-17 12:00")
    assert client.last(f"{INTEGRATION_BASE}/controls/current_time") == "Thursday 2026-07-16 18:37"
    assert client.last(f"{INTEGRATION_BASE}/controls/next_execution") == "Friday 2026-07-17 12:00"


def test_enable_toggle_callback_fires() -> None:
    client = FakeClient()
    toggled = []
    devices = _devices(client, _single_config(), on_toggle=toggled.append)
    devices.subscribe()
    on_enabled = client.callbacks[f"{INTEGRATION_BASE}/controls/enabled/on"]
    on_enabled(None, None, Message("0"))
    on_enabled(None, None, Message("1"))
    assert toggled == [False, True]


def test_mark_device_sent_stamps_and_clears() -> None:
    client = FakeClient()
    devices = _devices(client, _single_config())
    devices.mark_device_failed(0, "HTTP 500")  # fail first, so there is an error to clear
    devices.mark_device_sent(0, "2026-01-01 03:00:00")
    assert client.last(f"{KEY_DEVICE1_BASE}/controls/last_sent") == "2026-01-01 03:00:00"
    assert client.last(f"{KEY_DEVICE1_BASE}/controls/last_error") == ""
    assert client.last(f"{KEY_DEVICE1_BASE}/controls/last_error/meta/error") == ""


def test_last_sent_is_restored_per_key_device() -> None:
    # A key added since the last send has no stamp yet, so its control stays empty.
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
    assert client.last(f"{KEY_DEVICE1_BASE}/controls/last_sent") == "2026-01-01 03:00:00"
    assert client.last(f"{KEY_DEVICE2_BASE}/controls/last_sent") == ""


def test_failed_send_flags_only_last_error() -> None:
    client = FakeClient()
    devices = _devices(client, _single_config())
    devices.mark_device_failed(0, "No data from channels: dev/cold")
    assert client.last(f"{KEY_DEVICE1_BASE}/controls/last_error") == "No data from channels: dev/cold"
    assert client.last(f"{KEY_DEVICE1_BASE}/controls/last_error/meta/error") == "w"
    # channels and last_sent carry no error flag — the send failed, the data isn't wrong
    for control in ("ch0", "ch1", "last_sent"):
        assert client.last(f"{KEY_DEVICE1_BASE}/controls/{control}/meta/error") in (None, "")


def test_key_device_title_masks_key() -> None:
    # A full-length key, so masking runs on the same input length as in production.
    config = Config(
        "03:00", [Device("01234567890123456789012345678901", [Channel("dev/cold", 0)])], days_of_week=ALL_DAYS
    )
    client = FakeClient()
    _devices(client, config).publish_meta()
    meta = json.loads(client.last(f"{KEY_DEVICE1_BASE}/meta"))
    assert meta["title"] == {"en": "Waterius - 01234", "ru": "Ватериус - 01234"}


def test_key_device_title_prefers_device_name() -> None:
    # The "Waterius - " prefix survives a configured name, so our devices stay recognizable
    # in the flat device list.
    config = Config(
        "03:00",
        [Device("01234567890123456789012345678901", [Channel("dev/cold", 0)], name="Котельная")],
        days_of_week=ALL_DAYS,
    )
    client = FakeClient()
    _devices(client, config).publish_meta()
    meta = json.loads(client.last(f"{KEY_DEVICE1_BASE}/meta"))
    assert meta["title"] == {"en": "Waterius - Котельная", "ru": "Ватериус - Котельная"}


def test_each_key_becomes_its_own_device() -> None:
    config = Config(
        "03:00",
        [Device("K1", [Channel("dev/a", 0)]), Device("K2", [Channel("dev/b", 2)])],
        days_of_week=ALL_DAYS,
    )
    client = FakeClient()
    _devices(client, config).publish_meta()
    assert client.last(f"{KEY_DEVICE1_BASE}/meta") is not None
    assert client.last(f"{KEY_DEVICE2_BASE}/meta") is not None
    first = json.loads(client.last(f"{KEY_DEVICE1_BASE}/controls/ch0/meta"))
    second = json.loads(client.last(f"{KEY_DEVICE2_BASE}/controls/ch0/meta"))
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
    meta = json.loads(client.last(f"{KEY_DEVICE1_BASE}/controls/ch0/meta"))
    assert meta["type"] == "value"
    assert meta["units"] == units


def test_duplicate_type_within_key_gets_source_suffix() -> None:
    config = Config(
        "03:00", [Device("K1", [Channel("dev/cold1", 0), Channel("dev/cold2", 0)])], days_of_week=ALL_DAYS
    )
    client = FakeClient()
    _devices(client, config).publish_meta()
    first = json.loads(client.last(f"{KEY_DEVICE1_BASE}/controls/ch0/meta"))
    second = json.loads(client.last(f"{KEY_DEVICE1_BASE}/controls/ch1/meta"))
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
    assert client.last(f"{KEY_DEVICE2_BASE}/controls/ch0") == "84.20"
    assert client.last(f"{KEY_DEVICE1_BASE}/controls/ch0") is None


def test_update_channel_mirrors_one_source_onto_every_channel_that_uses_it() -> None:
    # One control can feed several channels — the config allows the same mqttTopicName twice,
    # deliberately — and every mirror has to move, not just the first.
    source = "dev/cold"
    config = Config(
        "03:00",
        [Device("K1", [Channel(source, 0), Channel(source, 1)]), Device("K2", [Channel(source, 0)])],
        days_of_week=ALL_DAYS,
    )
    client = FakeClient()
    devices = _devices(client, config)
    devices.update_channel(f"/devices/{source.replace('/', '/controls/')}", "42.5")
    assert client.last(f"{KEY_DEVICE1_BASE}/controls/ch0") == "42.5"
    assert client.last(f"{KEY_DEVICE1_BASE}/controls/ch1") == "42.5"
    assert client.last(f"{KEY_DEVICE2_BASE}/controls/ch0") == "42.5"


def test_update_channel_ignores_unconfigured_topic() -> None:
    # The service hands every message it receives to the facade, and a topic can outlive its
    # channel — a config edit removes it while the broker still delivers the old retained value.
    client = FakeClient()
    devices = _devices(client, _single_config())
    devices.update_channel("/devices/other/controls/x", "5")
    for topic, *_ in client.published:
        assert not topic.startswith(f"{KEY_DEVICE1_BASE}/controls/ch")


def test_clear_all_wipes_whatever_the_broker_holds() -> None:
    # A device in our namespace can be published by someone else — a wb-rules virtual device
    # spreads its meta over subtopics — so the wipe takes what the broker holds.
    control = f"{KEY_DEVICE1_BASE}/controls/ch0"
    retained = {
        f"{KEY_DEVICE1_BASE}/meta/name": "Waterius - K1",
        f"{control}/meta/type": "value",
        f"{control}/meta/order": "2",
        control: "42.5",
    }
    client = _DeliveringClient(retained)
    mqtt_device.clear_all(client)
    for topic in retained:
        assert client.last(topic) == ""


def test_clear_all_wipes_a_stuck_retained_command() -> None:
    # A retained command comes back on every subscribe and would force the switch.
    command = f"{INTEGRATION_BASE}/controls/enabled/on"
    client = _DeliveringClient({f"{INTEGRATION_BASE}/meta": "{}", command: "1"})
    mqtt_device.clear_all(client)
    assert client.last(command) == ""


def test_clear_all_empties_device_meta_after_its_controls() -> None:
    # An interrupted run leaves the device discoverable, so the next scan finds it again.
    control_meta = f"{KEY_DEVICE1_BASE}/controls/ch0/meta"
    client = _DeliveringClient({f"{KEY_DEVICE1_BASE}/meta": "{}", control_meta: '{"type": "value"}'})
    mqtt_device.clear_all(client)
    published = [topic for topic, *_ in client.published]
    assert published.index(control_meta) < published.index(f"{KEY_DEVICE1_BASE}/meta")


def test_clear_all_publishes_empty_integration_meta() -> None:
    client = FakeClient()
    wiped_topics = mqtt_device.clear_all(client)
    assert (f"{INTEGRATION_BASE}/meta", "", True, 1) in client.published
    assert wiped_topics == [f"{INTEGRATION_BASE}/meta"]


def test_clear_all_wipes_our_leftovers_and_spares_foreign_devices() -> None:
    # Ids come from the broker, not from the config, so a key the user has already deleted from
    # the config still gets wiped. An empty payload means the control is gone and is skipped.
    our_device_meta = json.dumps({"driver": mqtt_device.DRIVER})
    dropped_key_base = f"/devices/{mqtt_device.build_key_device_id(5)}"
    foreign_base = "/devices/wb-mqtt-serial"
    client = _DeliveringClient(
        {
            f"{INTEGRATION_BASE}/meta": our_device_meta,
            f"{dropped_key_base}/meta": our_device_meta,
            f"{dropped_key_base}/controls/ch0/meta": '{"type": "value"}',
            f"{dropped_key_base}/controls/ch1/meta": "",
            f"{foreign_base}/meta": '{"driver": "wb-mqtt-serial"}',
            f"{foreign_base}/controls/temp/meta": '{"type": "value"}',
        }
    )
    mqtt_device.clear_all(client)
    published = [topic for topic, *_ in client.published]
    assert f"{INTEGRATION_BASE}/meta" in published
    assert f"{dropped_key_base}/meta" in published
    assert f"{dropped_key_base}/controls/ch0/meta" in published
    assert f"{dropped_key_base}/controls/ch1/meta" not in published
    assert f"{foreign_base}/meta" not in published
    assert f"{foreign_base}/controls/temp/meta" not in published


def test_clear_through_the_facade_cannot_toggle_the_switch() -> None:
    # The wipe scans our own devices, so the broker re-delivers a stuck retained command to
    # whatever still listens on it. Without dropping the subscription first, that flips
    # automatic sending behind the user's back — reproduced on the stand before the fix.
    command = f"{INTEGRATION_BASE}/controls/enabled/on"
    client = _DeliveringClient({f"{INTEGRATION_BASE}/meta": "{}", command: "0"})
    toggled: list[bool] = []
    devices = _devices(client, _single_config(), on_toggle=toggled.append)
    devices.subscribe()
    devices.clear()
    assert not toggled
    assert client.last(command) == ""  # and the command itself is gone


def test_clear_through_the_facade_wipes_the_same_topics() -> None:
    client = _DeliveringClient({f"{KEY_DEVICE1_BASE}/meta": "{}"})
    assert set(_devices(client, _single_config()).clear()) == {
        f"{KEY_DEVICE1_BASE}/meta",
        f"{INTEGRATION_BASE}/meta",
    }


def test_mark_device_out_of_range_is_a_no_op() -> None:
    # Indices come from enumerating the config, so a mismatch must not raise on the paho thread.
    client = FakeClient()
    devices = _devices(client, _single_config())
    devices.mark_device_sent(5, "2026-01-01 03:00:00")
    devices.mark_device_failed(-1, "HTTP 500")
    assert [topic for topic, *_ in client.published] == []


def test_no_configured_devices_publishes_the_integration_device_only() -> None:
    # The fresh-install state: no keys yet, the daemon idles instead of crash-looping.
    client = FakeClient()
    _devices(client, Config("03:00", [], days_of_week=ALL_DAYS)).publish_meta()
    assert client.last(f"{INTEGRATION_BASE}/meta") is not None
    assert not [topic for topic, *_ in client.published if topic.startswith(f"{INTEGRATION_BASE}_")]


def test_integration_status_controls_carry_meta_and_a_startup_state() -> None:
    client = FakeClient()
    _devices(client, _single_config()).publish_meta()
    state_meta = json.loads(client.last(f"{INTEGRATION_BASE}/controls/state/meta"))
    assert state_meta["readonly"] is True
    assert state_meta["enum"]["4"] == {"en": "Has Errors", "ru": "Есть ошибки"}
    enabled_meta = json.loads(client.last(f"{INTEGRATION_BASE}/controls/enabled/meta"))
    assert enabled_meta["type"] == "switch"
    assert "readonly" not in enabled_meta  # the switch is the one control the user writes
    assert client.last(f"{INTEGRATION_BASE}/controls/state") == str(mqtt_device.STATE_INITIALIZING)


def test_wait_for_broker_confirms_when_its_token_returns() -> None:
    client = FakeClient()
    assert mqtt_device.wait_for_broker(client, timeout=1) is True
    assert MARKER_TOPIC in [topic for topic, *_ in client.published]


def test_wait_for_broker_reports_an_unconfirmed_wait() -> None:
    # Someone else's token on the shared topic must not pass for ours.
    assert mqtt_device.wait_for_broker(_StaleMarkerClient({}), timeout=0.05) is False


def test_scan_ends_on_its_own_marker_and_not_on_the_timeout() -> None:
    client = _DeliveringClient({f"{KEY_DEVICE1_BASE}/meta": "{}"})
    started = time.monotonic()
    mqtt_device.clear_all(client, timeout=30)
    assert time.monotonic() - started < 1
    assert MARKER_TOPIC in [topic for topic, *_ in client.published]


def test_scan_falls_back_to_the_timeout_when_the_marker_is_not_ours() -> None:
    # A marker left over from a scan that hit its timeout must not end the next one.
    client = _StaleMarkerClient({f"{KEY_DEVICE1_BASE}/meta": "{}"})
    started = time.monotonic()
    wiped_topics = mqtt_device.clear_all(client, timeout=0.05)
    assert time.monotonic() - started >= 0.1  # both scans waited their guard out
    assert f"{KEY_DEVICE1_BASE}/meta" in wiped_topics
