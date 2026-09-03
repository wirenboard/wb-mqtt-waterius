# The daemon is a state machine, so its tests read and drive private state directly, and the
# send fakes have to repeat the real signature, unused arguments included.
# pylint: disable=protected-access, unused-argument

import datetime
import json
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any, NamedTuple, Optional

import pytest

from tests.conftest import ALL_DAYS, FakeClient, Message
from wb.mqtt_waterius import mqtt_device, service, state, waterius_api
from wb.mqtt_waterius.config import Channel, Config, Device

INTEGRATION_BASE = mqtt_device.INTEGRATION_DEVICE_BASE
KEY_DEVICE1_BASE = f"/devices/{mqtt_device.build_key_device_id(1)}"
KEY_DEVICE2_BASE = f"/devices/{mqtt_device.build_key_device_id(2)}"
NOW = datetime.datetime(2026, 7, 16, 12, 0)  # Thursday 12:00


@pytest.fixture(autouse=True)
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Point persistence at a tmp state file and make the send timing instant for tests.
    """
    monkeypatch.setattr(state, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(state, "STATE_FILE", str(tmp_path / "state.json"))
    monkeypatch.setattr(service, "SEND_GAP", 0)
    monkeypatch.setattr(service, "READINGS_TIMEOUT", 0)


def _config(*devices: Device, days_of_week: Optional[set[int]] = None) -> Config:
    return Config("12:00", list(devices), days_of_week=ALL_DAYS if days_of_week is None else days_of_week)


def _service(
    config: Config,
    values: Optional[dict[str, str]] = None,
    enabled: bool = True,
    now: datetime.datetime = NOW,
    stored_state: Optional[dict] = None,
) -> tuple[service.Service, FakeClient]:
    # Seed persisted state read in Service.__init__. stored_state overrides single keys, for the
    # tests that start from a state left by an earlier run.
    stored = {"enabled": enabled, "last_sent": {}}
    stored.update(stored_state or {})
    state.save_state(stored)
    client = FakeClient()
    service_instance = service.Service(
        config, endpoint="http://test", datetime_now_fn=lambda: now, client=client
    )
    service_instance._source_values = dict(values or {})
    return service_instance, client


class FakeApi:
    """
    Stand-in for WateriusClient with a single send(), usable as a context manager.

    A send batch builds its own client and closes it on the way out, so the fake only has
    to survive one with-block.
    """

    def __init__(self, send: Callable[..., Any]) -> None:
        self.send = send

    def __enter__(self) -> "FakeApi":
        return self

    def __exit__(self, *_exc: Any) -> None:
        return None


def _seeded_values(config: Config) -> dict[str, str]:
    """
    A value on every source topic of the config, so no device is skipped for missing data.

    The reading itself is never significant, a test that cares about a particular one sets it
    on its own.
    """
    return {channel.mqtt_topic: "10" for device in config.devices for channel in device.channels}


def _patch_send(monkeypatch: pytest.MonkeyPatch, send: Callable[..., Any]) -> None:
    """
    Make every send batch build a fake cloud client with the given send().
    """
    monkeypatch.setattr(service, "WateriusClient", lambda endpoint: FakeApi(send))


def _explode_on_client(_endpoint: str) -> None:
    """
    Stand-in for WateriusClient that fails the test if a dry run tries to build one.
    """
    raise AssertionError("a dry run must not build a cloud client")


def _patch_send_returning(monkeypatch: pytest.MonkeyPatch, ok: bool = True, status: int = 200) -> list[dict]:
    """
    Patch the cloud client with a send of one fixed outcome and collect what it was given.

    Args:
        monkeypatch: the test's monkeypatch
        ok: outcome every device of the batch gets
        status: HTTP status behind that outcome

    Returns:
        The payloads in send order, filled as the send runs. _sent_keys turns them into keys.
    """
    sent: list[dict] = []

    def send(payload: dict, stop_event: Optional[threading.Event] = None) -> waterius_api.DeliveryReport:
        sent.append(payload)
        return waterius_api.DeliveryReport(ok, status)

    _patch_send(monkeypatch, send)
    return sent


def _sent_keys(payloads: list[dict]) -> list[str]:
    """
    Keys of the collected payloads, in send order.
    """
    return [payload["key"] for payload in payloads]


def test_get_channel_value_coercion() -> None:
    service_instance, _ = _service(_config(Device("K1", [Channel("d/c", 0)])))
    channel = service_instance._config.devices[0].channels[0]
    service_instance._source_values = {}
    assert service_instance._get_channel_value(channel) is None
    service_instance._source_values = {channel.mqtt_topic: ""}
    assert service_instance._get_channel_value(channel) is None
    service_instance._source_values = {channel.mqtt_topic: "abc"}
    assert service_instance._get_channel_value(channel) is None
    service_instance._source_values = {channel.mqtt_topic: "12.5"}
    assert service_instance._get_channel_value(channel) == 12.5


@pytest.mark.parametrize(
    "devices, enabled, expected_state, expected_flag",
    [
        ([], True, mqtt_device.STATE_CONFIG_INVALID, "w"),
        ([Device("K1", [Channel("d/c", 0)])], False, mqtt_device.STATE_DISABLED, ""),
        ([Device("K1", [Channel("d/c", 0)])], True, mqtt_device.STATE_ACTIVE, ""),
    ],
    ids=["no_devices", "disabled", "active"],
)
def test_apply_resting_state(
    devices: list[Device], enabled: bool, expected_state: int, expected_flag: str
) -> None:
    # Only an invalid config raises the error flag, the disabled and active resting states clear it.
    service_instance, client = _service(_config(*devices), enabled=enabled)
    service_instance._apply_resting_state()
    assert client.last(f"{INTEGRATION_BASE}/controls/state") == str(expected_state)
    assert client.last(f"{INTEGRATION_BASE}/controls/state/meta/error") == expected_flag


def test_on_toggle_persists_and_wakes_the_loop() -> None:
    # The click arrives on the paho thread, which only records the new position and wakes the
    # main loop. Publishing from two threads would race over the displayed times.
    service_instance, client = _service(_config(Device("K1", [Channel("d/c", 0)])), enabled=True)
    service_instance._on_toggle(False)
    assert state.load_state()["enabled"] is False  # persisted
    assert service_instance._state["enabled"] is False
    assert service_instance._wake_event.is_set()
    assert not client.published  # nothing went out from the paho thread


def test_the_poll_pass_publishes_the_switch_position() -> None:
    service_instance, client = _service(_config(Device("K1", [Channel("d/c", 0)])), enabled=True)
    service_instance._on_toggle(False)
    service_instance._poll_once()
    assert client.last(f"{INTEGRATION_BASE}/controls/enabled") == "0"
    assert client.last(f"{INTEGRATION_BASE}/controls/state") == str(mqtt_device.STATE_DISABLED)


class _ScheduleCase(NamedTuple):
    """
    One row of the schedule table, packed so the test takes a single argument.
    """

    now: datetime.datetime
    days_of_week: set[int]
    enabled: bool
    sent_today: bool
    expected: list[str]


@pytest.mark.parametrize(
    "case",
    [
        _ScheduleCase(datetime.datetime(2026, 7, 16, 11, 59), ALL_DAYS, True, False, []),
        _ScheduleCase(datetime.datetime(2026, 7, 16, 12, 1), ALL_DAYS, True, False, ["K1"]),
        _ScheduleCase(datetime.datetime(2026, 7, 16, 12, 1), ALL_DAYS, True, True, []),
        _ScheduleCase(datetime.datetime(2026, 7, 16, 12, 1), {0}, True, False, []),
        _ScheduleCase(NOW, ALL_DAYS, False, False, []),
        _ScheduleCase(NOW, ALL_DAYS, True, False, ["K1"]),
        _ScheduleCase(NOW, ALL_DAYS, True, True, ["K1"]),
    ],
    ids=[
        "before_the_minute",
        "past_the_minute_catches_up",
        "past_the_minute_already_sent_today",
        "wrong_weekday",
        "automatic_sending_off",
        "at_the_minute_sends",
        "at_the_minute_sends_even_if_sent_today",
    ],
)
def test_send_scheduled(case: _ScheduleCase, monkeypatch: pytest.MonkeyPatch) -> None:
    # The schedule rules as a table. Send time is 12:00, today is Thursday, so ALL_DAYS allows
    # today and {0} (Monday) does not. The scheduled minute sends regardless of the marks, past
    # it only a device without today's mark goes out.
    now, days_of_week, enabled, sent_today, expected = case
    config = _config(Device("K1", [Channel("d/c", 0)]), days_of_week=days_of_week)
    service_instance, _ = _service(config, values=_seeded_values(config), enabled=enabled, now=now)
    if sent_today:
        service_instance._state["last_sent"][state.key_hash("K1")] = NOW.isoformat()
    sent = _patch_send_returning(monkeypatch)
    service_instance._send_scheduled()
    assert _sent_keys(sent) == expected


def test_send_scheduled_tries_a_failing_device_once_a_minute(monkeypatch: pytest.MonkeyPatch) -> None:
    # A reconnect wakes the poll early, so the same minute can come round twice. A device that
    # failed must not be re-POSTed seconds later, the retry cadence is a minute.
    config = _config(Device("K1", [Channel("d/c", 0)]))
    service_instance, _ = _service(config, values=_seeded_values(config), now=NOW)  # 12:00 exact
    sent = _patch_send_returning(monkeypatch, ok=False, status=503)
    service_instance._send_scheduled()
    service_instance._send_scheduled()
    assert _sent_keys(sent) == ["K1"]

    service_instance._datetime_now = lambda: datetime.datetime(2026, 7, 16, 12, 1)
    service_instance._send_scheduled()
    assert _sent_keys(sent) == ["K1", "K1"]  # the next minute tries again


@pytest.mark.parametrize(
    "config_time, expected_sent",
    [
        ("13:00", ["K1"]),
        ("12:00", []),
    ],
    ids=["a_later_time_is_a_new_slot", "the_same_time_keeps_it_sent"],
)
def test_a_moment_is_compared_with_the_slot(
    config_time: str, expected_sent: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    # The device sent at 12:00 today. Moving the send time to 13:00 puts that moment before the
    # new slot, so it owes again, while the unchanged 12:00 slot leaves it alone.
    config = Config(config_time, [Device("K1", [Channel("d/c", 0)])], days_of_week=ALL_DAYS)
    service_instance, _ = _service(
        config,
        values=_seeded_values(config),
        now=datetime.datetime(2026, 7, 16, 13, 5),
        stored_state={"last_sent": {state.key_hash("K1"): "2026-07-16T12:00:00"}},
    )
    sent = _patch_send_returning(monkeypatch)
    service_instance._send_scheduled()
    assert _sent_keys(sent) == expected_sent


def test_a_new_send_time_fires_again_the_same_day(monkeypatch: pytest.MonkeyPatch) -> None:
    # 12:00 went through, then the user moves the send time to 15:00 in the configurator. The
    # restart that follows the config write reopens the day, and 15:00 fires once more.
    config = _config(Device("K1", [Channel("d/c", 0)]))
    service_instance, _ = _service(config, values=_seeded_values(config), now=NOW)  # 12:00 exact
    sent = _patch_send_returning(monkeypatch)
    service_instance._send_scheduled()
    assert _sent_keys(sent) == ["K1"]

    moved = Config("15:00", config.devices, days_of_week=ALL_DAYS)
    restarted = service.Service(
        moved,
        endpoint="http://test",
        datetime_now_fn=lambda: datetime.datetime(2026, 7, 16, 15, 0),
        client=FakeClient(),
    )
    restarted._source_values = _seeded_values(config)
    restarted._send_scheduled()
    assert _sent_keys(sent) == ["K1", "K1"]


def test_send_now_all_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    config = _config(Device("K1", [Channel("d/c", 0)]))
    service_instance, client = _service(config, values=_seeded_values(config))
    _patch_send_returning(monkeypatch)
    assert service_instance.send_now() is True
    assert client.last(f"{INTEGRATION_BASE}/controls/state") == str(mqtt_device.STATE_ACTIVE)
    assert client.last(f"{INTEGRATION_BASE}/controls/state/meta/error") == ""
    assert client.last(f"{KEY_DEVICE1_BASE}/controls/last_sent")  # stamped
    assert service_instance._state["last_sent"][state.key_hash("K1")] == NOW.isoformat()


def test_send_now_one_device_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    config = _config(Device("K1", [Channel("d1/c", 0)]), Device("K2", [Channel("d2/c", 0)]))
    service_instance, client = _service(config, values=_seeded_values(config))

    def fake_send(payload: dict, stop_event: Optional[threading.Event] = None) -> waterius_api.DeliveryReport:
        ok = payload["key"] == "K1"
        return waterius_api.DeliveryReport(ok, 200 if ok else 503)

    _patch_send(monkeypatch, fake_send)
    assert service_instance.send_now() is False
    assert client.last(f"{INTEGRATION_BASE}/controls/state") == str(mqtt_device.STATE_HAS_ERRORS)
    assert client.last(f"{INTEGRATION_BASE}/controls/state/meta/error") == "w"
    # K1 carries today's mark and K2 does not, so the next poll re-sends K2 alone. That mark is
    # also what a restart reads, the retry does not live in memory.
    moments = service_instance._state["last_sent"]
    assert moments[state.key_hash("K1")] == NOW.isoformat()
    assert state.key_hash("K2") not in moments
    assert service_instance._failed_transient == {1}  # K2 shown red until it goes through
    assert client.last(f"{KEY_DEVICE2_BASE}/controls/last_error") == "HTTP 503"
    assert client.last(f"{KEY_DEVICE2_BASE}/controls/last_error/meta/error") == "w"


def test_send_now_passes_device_name_to_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    # The configured name travels all the way into the POST body, so Waterius renames the
    # device on every send. A device left unnamed sends no `name` at all.
    config = _config(
        Device("K1", [Channel("d1/c", 0)], name="Котельная"),
        Device("K2", [Channel("d2/c", 0)]),
    )
    service_instance, _ = _service(config, values=_seeded_values(config))
    sent = _patch_send_returning(monkeypatch)
    assert service_instance.send_now() is True
    assert sent[0]["name"] == "Котельная"
    assert sent[1]["name"] == ""  # always sent; empty keeps the name in the cabinet


def test_send_now_missing_channel_never_posts(monkeypatch: pytest.MonkeyPatch) -> None:
    config = _config(Device("K1", [Channel("d/c", 0)]))
    service_instance, client = _service(config, values={})  # value never arrived
    posted = _patch_send_returning(monkeypatch)
    assert service_instance.send_now() is False
    assert not posted  # partial/empty data is never sent
    assert "No data from channels" in client.last(f"{KEY_DEVICE1_BASE}/controls/last_error")
    assert client.last(f"{INTEGRATION_BASE}/controls/state") == str(mqtt_device.STATE_HAS_ERRORS)


def test_send_now_no_devices() -> None:
    service_instance, client = _service(_config())
    assert service_instance.send_now() is False
    assert client.last(f"{INTEGRATION_BASE}/controls/state") == str(mqtt_device.STATE_CONFIG_INVALID)
    assert client.last(f"{INTEGRATION_BASE}/controls/state/meta/error") == "w"


def test_dry_run_builds_no_client_and_has_no_side_effects(monkeypatch: pytest.MonkeyPatch) -> None:
    # A dry run posts nothing, so it must not open a cloud client at all, and it leaves the
    # status devices and the persisted state exactly as they were.
    monkeypatch.setattr(service, "WateriusClient", _explode_on_client)
    config = _config(Device("K1", [Channel("d/c", 0)]))
    service_instance, client = _service(config, values=_seeded_values(config))
    assert service_instance.dry_run_payloads() is True
    assert not service_instance._state["last_sent"]
    assert client.last(f"{KEY_DEVICE1_BASE}/controls/last_sent") is None


def test_a_dry_run_prints_the_body_it_would_post(capsys: pytest.CaptureFixture) -> None:
    # The point of a dry run is a body that can be replayed by hand, so the printed one carries
    # the real key. The journal, which travels in the diagnostic archive, gets the cut one.
    key = "0123456789abcdef0123456789abcdef"
    config = _config(Device(key, [Channel("d/c", 0)]))
    service_instance, _ = _service(config, values=_seeded_values(config))
    assert service_instance.dry_run_payloads() is True
    assert key in capsys.readouterr().out


def test_dry_run_reports_missing_readings() -> None:
    # A device whose readings never arrived cannot be built, and the dry run says so rather
    # than implying the real send would go through.
    service_instance, client = _service(_config(Device("K1", [Channel("d/c", 0)])), values={})
    assert service_instance.dry_run_payloads() is False
    assert client.last(f"{KEY_DEVICE1_BASE}/controls/last_error") is None  # status untouched by a dry run


def test_dry_run_with_no_devices_reports_nothing_to_send() -> None:
    # The state of every fresh install. There is nothing to build, and the answer is what the
    # CLI turns into its exit code.
    service_instance, client = _service(_config())
    assert service_instance.dry_run_payloads() is False
    assert not client.published  # a dry run leaves the status devices alone


@pytest.mark.parametrize(
    "has_value, timeout, poll, expected_waits",
    [
        (True, 5, 0.2, []),
        (False, 1, 0.25, [0.25] * 4),
    ],
    ids=["values_are_already_there", "gives_up_after_the_timeout"],
)
def test_await_readings(
    monkeypatch: pytest.MonkeyPatch, has_value: bool, timeout: int, poll: float, expected_waits: list[float]
) -> None:
    # WB values are retained and land right after SUBSCRIBE, so normally the first check passes and
    # nothing is waited for. A topic that never publishes must not hold the send forever either, the
    # wait polls out its timeout and lets the send report the missing channel.
    monkeypatch.setattr(service, "READINGS_TIMEOUT", timeout)
    monkeypatch.setattr(service, "READINGS_POLL", poll)
    config = _config(Device("K1", [Channel("d/c", 0)]))
    service_instance, _ = _service(config, values=_seeded_values(config) if has_value else {})
    waits: list[float] = []
    monkeypatch.setattr(service_instance._stop_event, "wait", lambda seconds: waits.append(seconds) or False)
    service_instance._await_readings()
    assert waits == expected_waits


def test_on_connect_requests_full_resetup() -> None:
    service_instance, _ = _service(_config(Device("K1", [Channel("d/c", 0)])))
    service_instance._on_connect(None, None, None, 0)
    assert service_instance._connected_event.is_set()
    assert service_instance._resetup_event.is_set()  # every (re)connect asks the loop to re-publish devices
    assert service_instance._wake_event.is_set()  # and wakes the poll sleep so it happens at once


def test_on_connect_failure_skips_resetup() -> None:
    service_instance, _ = _service(_config(Device("K1", [Channel("d/c", 0)])))
    service_instance._on_connect(None, None, None, 1)
    assert not service_instance._resetup_event.is_set()
    assert not service_instance._connected_event.is_set()


def test_setup_mqtt_republishes_devices_after_reconnect(monkeypatch: pytest.MonkeyPatch) -> None:
    # A broker restart drops our retained devices. _setup_mqtt runs on every reconnect
    # and must recreate the integration device and each key device (the broker-restart bug).
    monkeypatch.setattr("wb.mqtt_waterius.mqtt_device._scan_retained", lambda *_args: [])
    service_instance, client = _service(_config(Device("K1", [Channel("d/c", 0)])))
    client.published.clear()
    service_instance._setup_mqtt()
    assert (f"{INTEGRATION_BASE}/meta", "", True, 1) in client.published  # clean-slate wipe runs first
    assert client.last(f"{INTEGRATION_BASE}/meta")  # integration device re-published
    assert client.last(f"{KEY_DEVICE1_BASE}/meta")  # key device re-published
    assert client.last(f"{KEY_DEVICE1_BASE}/controls/ch0/meta")  # and its channel


def test_a_quiet_pass_writes_the_clock_only() -> None:
    # Nothing else changes between two ordinary minutes, so nothing else is rewritten.
    service_instance, client = _service(
        _config(Device("K1", [Channel("d/c", 0)])), now=datetime.datetime(2026, 7, 16, 9, 0)
    )
    client.published.clear()
    service_instance._poll_once()
    assert [topic for topic, *_ in client.published] == [f"{INTEGRATION_BASE}/controls/current_time"]


def test_a_reconnect_republishes_the_status(monkeypatch: pytest.MonkeyPatch) -> None:
    # The clean slate wipes the controls, so the pass that re-created the devices has to fill
    # them in again — there is no memo left to tell it what the broker is missing.
    monkeypatch.setattr("wb.mqtt_waterius.mqtt_device._scan_retained", lambda *_args: [])
    service_instance, client = _service(
        _config(Device("K1", [Channel("d/c", 0)])), now=datetime.datetime(2026, 7, 16, 9, 0)
    )
    service_instance._resetup_event.set()
    client.published.clear()
    service_instance._poll_once()
    published = [topic for topic, *_ in client.published]
    for control in ("enabled", "state", "state/meta/error", "next_execution", "current_time"):
        assert f"{INTEGRATION_BASE}/controls/{control}" in published
    assert client.last(f"{INTEGRATION_BASE}/controls/next_execution") == "Thursday 2026-07-16 12:00"


def test_a_missed_slot_minute_still_moves_next_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    # A long pass steps over the exact minute. The catch-up sends what has no mark anyway, and
    # the next-run text has to follow the slot the loop never saw.
    config = _config(Device("K1", [Channel("d/c", 0)]))
    service_instance, client = _service(
        config, values=_seeded_values(config), now=datetime.datetime(2026, 7, 16, 12, 1)
    )
    _patch_send_returning(monkeypatch)
    service_instance._send_scheduled()
    assert client.last(f"{INTEGRATION_BASE}/controls/next_execution") == "Friday 2026-07-17 12:00"


def test_the_next_run_text_goes_out_once_a_day(monkeypatch: pytest.MonkeyPatch) -> None:
    # Every later pass of the same day sees the same slot, so the control is left alone.
    config = _config(Device("K1", [Channel("d/c", 0)]))
    topic = config.devices[0].channels[0].mqtt_topic
    service_instance, client = _service(
        config, values=_seeded_values(config), now=datetime.datetime(2026, 7, 16, 12, 1)
    )
    _patch_send_returning(monkeypatch)
    service_instance._send_scheduled()
    client.published.clear()
    service_instance._datetime_now = lambda: datetime.datetime(2026, 7, 16, 12, 2)
    service_instance._send_scheduled()
    assert f"{INTEGRATION_BASE}/controls/next_execution" not in [topic for topic, *_ in client.published]


def test_the_scheduled_fire_moves_next_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    # The slot has just been used, so the control has to point at the next allowed day. Nothing
    # else would move it before the next reconnect.
    config = _config(Device("K1", [Channel("d/c", 0)]))
    service_instance, client = _service(config, values=_seeded_values(config))  # NOW is the send minute
    _patch_send_returning(monkeypatch)
    service_instance._send_scheduled()
    assert client.last(f"{INTEGRATION_BASE}/controls/next_execution") == "Friday 2026-07-17 12:00"


def test_run_loop_applies_resetup_then_stops(monkeypatch: pytest.MonkeyPatch) -> None:
    # The reconnect glue, run()'s loop must see the event, clear it and call _setup_mqtt.
    service_instance, _ = _service(_config(Device("K1", [Channel("d/c", 0)])), enabled=False)
    service_instance._connected_event.set()  # skip the initial connect wait
    service_instance._resetup_event.set()
    calls = []

    def fake_setup() -> None:
        calls.append(1)
        service_instance._stop_event.set()  # let the loop exit after this one iteration
        service_instance._wake_event.set()

    monkeypatch.setattr(service_instance, "_setup_mqtt", fake_setup)
    service_instance.run()
    assert calls == [1]
    assert not service_instance._resetup_event.is_set()  # the loop cleared it


def test_run_arms_the_last_will_before_connecting_and_stops_the_client() -> None:
    # A will registered after the connection is never sent, and a daemon that exits without
    # stopping the client leaves the socket to the garbage collector.
    service_instance, client = _service(_config(Device("K1", [Channel("d/c", 0)])), enabled=False)
    service_instance._stop_event.set()  # one pass through the loop and out
    service_instance._connected_event.set()
    service_instance.run()
    assert client.will_at_connect == (f"{INTEGRATION_BASE}/controls/state/meta/error", "rw", True)
    assert client.stopped


def test_stop_removes_the_devices_while_the_client_is_still_up() -> None:
    # Nothing else can take our devices off the broker, the package removal scripts run when the
    # service is already down. The wipe and its confirmation must go out before the client stops,
    # paho drops whatever is still on the way.
    service_instance, client = _service(_config(Device("K1", [Channel("d/c", 0)])), enabled=False)
    service_instance._stop_event.set()  # one pass through the loop and out
    service_instance._connected_event.set()
    service_instance.run()
    assert client.last(f"{KEY_DEVICE1_BASE}/meta") == ""
    assert client.last(f"{INTEGRATION_BASE}/meta") == ""
    before_stop = [topic for topic, *_ in client.published[: client.published_at_stop]]
    assert f"{INTEGRATION_BASE}/meta" in before_stop
    assert before_stop[-1] == mqtt_device.MARKER_TOPIC  # the broker confirmed the wipe


def test_stop_drops_the_source_subscriptions_before_the_wipe(monkeypatch: pytest.MonkeyPatch) -> None:
    # A reading arriving mid-removal would mirror itself back into a channel just emptied.
    monkeypatch.setattr("wb.mqtt_waterius.mqtt_device._scan_retained", lambda *_args: [])
    config = _config(Device("K1", [Channel("d/c", 0)]))
    source = config.devices[0].channels[0].mqtt_topic
    channel_topic = f"{KEY_DEVICE1_BASE}/controls/ch0"
    service_instance, client = _service(config, enabled=False)
    service_instance._setup_mqtt()
    publish = client.publish

    def publish_racing_a_reading(topic: str, payload: Any, retain: bool = False, qos: int = 0) -> None:
        publish(topic, payload, retain, qos)
        if topic == channel_topic and payload == "":
            publish(source, "5")  # the source updates in the middle of the removal

    monkeypatch.setattr(client, "publish", publish_racing_a_reading)
    service_instance._connected_event.set()
    service_instance._remove_devices()
    assert client.last(channel_topic) == ""
    assert source not in client.subscribed


def test_stop_survives_a_broker_that_fails_mid_removal(monkeypatch: pytest.MonkeyPatch) -> None:
    # Removal is the last thing a clean stop does, so a broker failing there can only be
    # reported. The bare call is the assertion, an exception would end the stop in a traceback.
    service_instance, _ = _service(_config(Device("K1", [Channel("d/c", 0)])), enabled=False)
    service_instance._connected_event.set()

    def broken_removal() -> list[str]:
        raise RuntimeError("broker went away")

    monkeypatch.setattr(service_instance._wb_devices, "remove", broken_removal)
    service_instance._remove_devices()


def test_a_crash_leaves_the_devices_on_the_broker(monkeypatch: pytest.MonkeyPatch) -> None:
    # Removal belongs to the clean path only. A daemon dying on an exception has to stay visible
    # and red in the UI, not disappear as if it had been stopped.
    monkeypatch.setattr("wb.mqtt_waterius.mqtt_device._scan_retained", lambda *_args: [])
    service_instance, client = _service(_config(Device("K1", [Channel("d/c", 0)])), enabled=False)
    service_instance._setup_mqtt()  # the devices are on the broker

    def boom() -> None:
        raise RuntimeError("poll blew up")

    monkeypatch.setattr(service_instance, "_poll_once", boom)
    with pytest.raises(RuntimeError):
        service_instance.run()
    assert client.last(f"{KEY_DEVICE1_BASE}/meta")  # still published, not wiped
    assert client.last(f"{INTEGRATION_BASE}/meta")
    assert not client.stopped


def test_stop_without_a_connection_publishes_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    # Never connected means nothing of ours reached the broker, so there is nothing to remove and
    # no reason to wait out the confirmation timeout on the way down.
    monkeypatch.setattr(service, "CONNECT_TIMEOUT", 0)
    service_instance, client = _service(_config(Device("K1", [Channel("d/c", 0)])), enabled=False)
    monkeypatch.setattr(client, "start", lambda: None)  # no CONNACK, so no connected event
    service_instance._stop_event.set()
    service_instance.run()
    assert not client.published
    assert client.stopped


def test_send_now_interrupted_leaves_the_rest_unsent(monkeypatch: pytest.MonkeyPatch) -> None:
    # Shutdown landing in the inter-device gap must leave the untouched devices without today's
    # mark, or they are silently skipped until tomorrow.
    config = _config(Device("K1", [Channel("d1/c", 0)]), Device("K2", [Channel("d2/c", 0)]))
    service_instance, _ = _service(config, values=_seeded_values(config))
    sent = _patch_send_returning(monkeypatch)
    monkeypatch.setattr(service_instance._stop_event, "wait", lambda _seconds: True)  # SIGTERM in the gap
    assert service_instance.send_now() is False
    assert _sent_keys(sent) == ["K1"]  # only the first device was sent before the abort
    # The mark of the device that DID send is on disk and K2 has none, so K2 is still unsent.
    persisted = state.load_state()["last_sent"]
    assert persisted[state.key_hash("K1")] == NOW.isoformat()
    assert state.key_hash("K2") not in persisted
    assert service_instance._get_unsent_devices(NOW) == [1]


def test_per_device_timestamp_persisted_and_restored(monkeypatch: pytest.MonkeyPatch) -> None:
    config = _config(Device("K1", [Channel("d/c", 0)]))
    service_instance, _ = _service(config, values=_seeded_values(config))
    _patch_send_returning(monkeypatch)
    service_instance.send_now()
    digest = state.key_hash("K1")
    stamp = NOW.strftime(service.TIMESTAMP_FORMAT)
    assert state.load_state()["last_sent"][digest] == NOW.isoformat()  # persisted to the state file

    # A fresh Service (same config) restores that timestamp onto the key device.
    client2 = FakeClient()
    service_instance2 = service.Service(
        config, endpoint="http://test", datetime_now_fn=lambda: NOW, client=client2
    )
    service_instance2._wb_devices.create()
    assert client2.last(f"{KEY_DEVICE1_BASE}/controls/last_sent") == stamp


def test_timestamp_is_taken_per_device_not_per_batch(monkeypatch: pytest.MonkeyPatch) -> None:
    # Devices are sent one at a time and a retried one answers minutes later, so each stamp
    # must be the moment that device answered. A single batch-wide stamp would report the
    # slow device as sent at the time the fast one was.
    config = _config(Device("K1", [Channel("d1/c", 0)]), Device("K2", [Channel("d2/c", 0)]))
    service_instance, client = _service(config, values=_seeded_values(config))
    clock = [NOW]

    def advancing_now() -> datetime.datetime:
        clock[0] += datetime.timedelta(minutes=1)
        return clock[0]

    monkeypatch.setattr(service_instance, "_datetime_now", advancing_now)
    _patch_send_returning(monkeypatch)
    service_instance.send_now()
    assert client.last(f"{KEY_DEVICE1_BASE}/controls/last_sent") != client.last(
        f"{KEY_DEVICE2_BASE}/controls/last_sent"
    )
    persisted = state.load_state()["last_sent"]
    assert persisted[state.key_hash("K1")] != persisted[state.key_hash("K2")]


def test_stale_device_timestamps_pruned() -> None:
    stale = state.key_hash("OLD")
    keep = state.key_hash("K1")
    kept_moment = "2026-07-16T12:00:00"
    state.save_state({"enabled": True, "last_sent": {stale: "2026-07-15T10:00:00", keep: kept_moment}})
    config = Config("12:00", [Device("K1", [Channel("d/c", 0)])], days_of_week=ALL_DAYS)
    service_instance = service.Service(
        config, endpoint="http://test", datetime_now_fn=lambda: NOW, client=FakeClient()
    )
    service_instance._prune_sent_moments()
    assert service_instance._state["last_sent"] == {keep: kept_moment}
    assert state.load_state()["last_sent"] == {keep: kept_moment}  # written through


def test_manual_send_ignores_todays_marks(monkeypatch: pytest.MonkeyPatch) -> None:
    # The manual send has no rules: every device goes out with fresh values, even one already
    # sent earlier today.
    config = _config(Device("K1", [Channel("d1/c", 0)]), Device("K2", [Channel("d2/c", 0)]))
    service_instance, _ = _service(config, values=_seeded_values(config))
    service_instance._state["last_sent"][state.key_hash("K1")] = "2026-07-16T08:00:00"
    sent = _patch_send_returning(monkeypatch)
    assert service_instance.send_now() is True
    assert _sent_keys(sent) == ["K1", "K2"]  # both sent, K1 not skipped despite today's mark


def test_send_now_permanent_404_holds_the_device(monkeypatch: pytest.MonkeyPatch) -> None:
    # A 404 (key not registered) is held red and left out of the catch-up, chasing an
    # unregistered key for the rest of the day is pointless.
    config = _config(Device("K1", [Channel("d1/c", 0)]), Device("K2", [Channel("d2/c", 0)]))
    service_instance, client = _service(config, values=_seeded_values(config))

    def fake_send(payload: dict, stop_event: Optional[threading.Event] = None) -> waterius_api.DeliveryReport:
        # Mirror the real send, a 404 carries the KEY_NOT_FOUND_ERROR message.
        if payload["key"] == "K1":
            return waterius_api.DeliveryReport(True, 200)
        return waterius_api.DeliveryReport(False, 404, waterius_api.KEY_NOT_FOUND_ERROR)

    _patch_send(monkeypatch, fake_send)
    assert service_instance.send_now() is False
    assert client.last(f"{KEY_DEVICE2_BASE}/controls/last_error") == waterius_api.KEY_NOT_FOUND_ERROR
    assert client.last(f"{KEY_DEVICE2_BASE}/controls/last_error/meta/error") == "w"
    assert client.last(f"{INTEGRATION_BASE}/controls/state") == str(mqtt_device.STATE_HAS_ERRORS)
    assert service_instance._failed_hold == {1}  # K2 held
    assert service_instance._failed_transient == set()
    assert not service_instance._get_unsent_devices(NOW)  # held, not retried today


def test_catch_up_resends_only_the_unsent_device(monkeypatch: pytest.MonkeyPatch) -> None:
    # After a fire leaves K2 transiently failed, the next poll past the minute re-POSTs only K2.
    # On success it clears and the integration device returns to Active.
    config = _config(Device("K1", [Channel("d1/c", 0)]), Device("K2", [Channel("d2/c", 0)]))
    service_instance, client = _service(config, values=_seeded_values(config))
    calls = []
    fail_k2 = {"on": True}

    def fake_send(payload: dict, stop_event: Optional[threading.Event] = None) -> waterius_api.DeliveryReport:
        calls.append(payload["key"])
        if payload["key"] == "K2" and fail_k2["on"]:
            return waterius_api.DeliveryReport(False, 503)
        return waterius_api.DeliveryReport(True, 200)

    _patch_send(monkeypatch, fake_send)
    service_instance.send_now()
    assert service_instance._failed_transient == {1}

    calls.clear()
    fail_k2["on"] = False  # K2 recovers
    service_instance._datetime_now = lambda: datetime.datetime(2026, 7, 16, 12, 1)
    service_instance._send_scheduled()
    assert calls == ["K2"]  # only the unsent device retried, not K1
    assert service_instance._failed_transient == set()
    assert client.last(f"{INTEGRATION_BASE}/controls/state") == str(mqtt_device.STATE_ACTIVE)


def test_no_catch_up_after_the_day_rolls_over(monkeypatch: pytest.MonkeyPatch) -> None:
    # Past midnight yesterday's reading is stale, so a device that failed is not chased any
    # more. The next scheduled fire rebuilds everything anyway.
    config = _config(Device("K1", [Channel("d/c", 0)]))
    service_instance, _ = _service(config, values=_seeded_values(config))
    calls = _patch_send_returning(monkeypatch, ok=False, status=503)
    service_instance.send_now()
    calls.clear()
    service_instance._datetime_now = lambda: datetime.datetime(2026, 7, 17, 0, 5)  # before 12:00
    service_instance._send_scheduled()
    assert not calls


def test_a_restart_resumes_the_device_left_unsent(monkeypatch: pytest.MonkeyPatch) -> None:
    # The point of the per-device marks. K1 went through and K2 did not, and nothing about the
    # retry lives in memory — a fresh Service on the same state file sends K2 alone.
    config = _config(Device("K1", [Channel("d1/c", 0)]), Device("K2", [Channel("d2/c", 0)]))
    service_instance, _ = _service(config, values=_seeded_values(config))
    calls = []
    fail_k2 = {"on": True}

    def fake_send(payload: dict, stop_event: Optional[threading.Event] = None) -> waterius_api.DeliveryReport:
        calls.append(payload["key"])
        if payload["key"] == "K2" and fail_k2["on"]:
            return waterius_api.DeliveryReport(False, 503)
        return waterius_api.DeliveryReport(True, 200)

    _patch_send(monkeypatch, fake_send)
    service_instance.send_now()

    restarted = service.Service(
        config,
        endpoint="http://test",
        datetime_now_fn=lambda: datetime.datetime(2026, 7, 16, 12, 5),
        client=FakeClient(),
    )
    restarted._source_values = _seeded_values(config)
    calls.clear()
    fail_k2["on"] = False  # K2 recovers while the service was down
    restarted._send_scheduled()
    assert calls == ["K2"]


class _FakeSignals:  # pylint: disable=too-few-public-methods
    """
    Stand-in for the signal module, so the test runs where SIGHUP does not exist.
    """

    SIGTERM, SIGINT, SIGHUP = 15, 2, 1

    def __init__(self) -> None:
        self.handlers: dict[int, Callable] = {}

    def signal(self, signum: int, handler: Callable) -> None:
        self.handlers[signum] = handler


def test_a_source_value_arrives_through_the_client() -> None:
    # The daemon rests on this wiring: the subscription has to hand incoming values to the
    # reading callback, or every device reports no data and nothing is ever sent.
    config = _config(Device("K1", [Channel("d/c", 0)]))
    topic = config.devices[0].channels[0].mqtt_topic
    service_instance, client = _service(config, values={})
    service_instance._subscribe_readings()
    client.publish(topic, "12.5")
    assert service_instance._source_values[topic] == "12.5"
    assert client.last(f"{KEY_DEVICE1_BASE}/controls/ch0") == "12.5"  # mirrored on the card too


def test_the_daemon_stops_on_every_signal_it_takes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # SIGHUP is a stop here, not the config reload the convention suggests, so the whole set is
    # worth pinning. Each handler has to reach stop_service, which is what ends the poll loop.
    (tmp_path / "wb.conf").write_text(
        '{"sendTime": "03:00", "daysOfWeek": ["monday"], '
        '"devices": [{"key": "K1", "channels": [{"mqttTopicName": "d/c", "dataType": 0}]}]}',
        encoding="utf-8",
    )
    signals = _FakeSignals()
    monkeypatch.setattr(service, "signal", signals)
    started: dict[str, service.Service] = {}
    monkeypatch.setattr(service.Service, "run", lambda self: started.setdefault("service", self))

    assert service.main_daemon(str(tmp_path / "wb.conf"), client=FakeClient()) == service.EXIT_NOT_RUNNING
    assert set(signals.handlers) == {signals.SIGTERM, signals.SIGINT, signals.SIGHUP}
    for signum, handler in signals.handlers.items():
        started["service"]._stop_event.clear()
        handler(signum, None)
        assert started["service"]._stop_event.is_set()
        assert started["service"]._wake_event.is_set()  # the loop wakes instead of sleeping out


def test_the_slot_fires_again_the_next_day(monkeypatch: pytest.MonkeyPatch) -> None:
    # The guard remembers the minute it fired in, so it has to remember the day as well —
    # otherwise a long-lived daemon fires on its first day and never again.
    config = _config(Device("K1", [Channel("d/c", 0)]))
    service_instance, _ = _service(config, values=_seeded_values(config))  # NOW is the send minute
    sent = _patch_send_returning(monkeypatch)
    service_instance._send_scheduled()
    service_instance._datetime_now = lambda: NOW + datetime.timedelta(days=1)
    service_instance._send_scheduled()
    assert _sent_keys(sent) == ["K1", "K1"]


def test_main_daemon_config_error_reports_invalid_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A missing config exits 6 (EXIT_CONFIG_ERROR) instead of crash-looping (the unit's
    # RestartPreventExitStatus=6 keeps it down until a fixed config is saved), and the control
    # the integration device is set to "Config Not Valid" so the web UI shows the dead state, not the
    # previous run's stale "Active".
    monkeypatch.setattr("wb.mqtt_waterius.mqtt_device._scan_retained", lambda *_args: [])
    monkeypatch.setattr(service, "signal", _FakeSignals())
    client = FakeClient()
    assert service.main_daemon(str(tmp_path / "nope.conf"), client=client) == service.EXIT_CONFIG_ERROR
    assert client.last(f"{INTEGRATION_BASE}/controls/state") == str(mqtt_device.STATE_CONFIG_INVALID)
    assert client.last(f"{INTEGRATION_BASE}/controls/state/meta/error") == "w"


def test_main_send_config_error_on_invalid_json(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("wb.mqtt_waterius.mqtt_device._scan_retained", lambda *_args: [])
    bad_config = tmp_path / "bad.conf"
    bad_config.write_text("{not json", encoding="utf-8")
    assert service.main_send_once(str(bad_config), client=FakeClient()) == service.EXIT_CONFIG_ERROR


def _write_valid_config(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "sendTime": "12:00",
                "daysOfWeek": ["monday"],
                "devices": [{"key": "K1", "channels": [{"mqttTopicName": "d/c", "dataType": 0}]}],
            }
        ),
        encoding="utf-8",
    )


def test_main_send_maps_run_once_result(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # main_send_once maps run_once's bool onto the CLI exit-code contract.
    conf = tmp_path / "w.conf"
    _write_valid_config(conf)
    monkeypatch.setattr(service.Service, "run_once", lambda self, dry_run=False: True)
    assert service.main_send_once(str(conf), client=FakeClient()) == service.EXIT_SUCCESS
    monkeypatch.setattr(service.Service, "run_once", lambda self, dry_run=False: False)
    assert service.main_send_once(str(conf), client=FakeClient()) == service.EXIT_FAILURE


def test_run_once_sends_and_returns_result(monkeypatch: pytest.MonkeyPatch) -> None:
    # run_once connects, waits for retained values, sends once, returns send_now's result.
    config = _config(Device("K1", [Channel("d/c", 0)]))
    service_instance, _ = _service(config, values=_seeded_values(config))
    _patch_send_returning(monkeypatch)
    assert service_instance.run_once() is True


def test_stop_interrupts_the_initial_connection_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    service_instance, client = _service(_config(Device("K1", [Channel("d/c", 0)])))
    monkeypatch.setattr(client, "start", lambda: None)
    service_instance.stop_service()
    service_instance.run()
    assert client.stopped


def test_a_real_send_writes_the_pruned_state_and_a_dry_run_does_not(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A dry run promises to change nothing, and rewriting the state file behind it would break
    # that. The write belongs to the daemon start and to a real send.
    stale = state.key_hash("OLD")
    config = _config(Device("K1", [Channel("d/c", 0)]))
    service_instance, _ = _service(
        config,
        values=_seeded_values(config),
        stored_state={"last_sent": {stale: "2026-07-15T10:00:00"}},
    )
    monkeypatch.setattr(service, "WateriusClient", _explode_on_client)
    assert service_instance.run_once(dry_run=True) is True
    assert stale in state.load_state()["last_sent"]

    _patch_send_returning(monkeypatch)  # replaces the exploding client, undo() would drop the env too
    assert service_instance.run_once() is True
    assert stale not in state.load_state()["last_sent"]


def test_run_once_dry_run_prints_instead_of_posting(monkeypatch: pytest.MonkeyPatch) -> None:
    # The dry run and the real send differ by one ternary, and getting it backwards would post
    # real readings from a command that promises to only print them.
    monkeypatch.setattr(service, "WateriusClient", _explode_on_client)
    config = _config(Device("K1", [Channel("d/c", 0)]))
    service_instance, client = _service(config, values=_seeded_values(config))
    assert service_instance.run_once(dry_run=True) is True
    assert client.stopped
    assert not service_instance._state["last_sent"]  # nothing was sent, so nothing was stamped


@pytest.mark.parametrize("entry_point", [service.main_daemon, service.main_send_once], ids=["daemon", "send"])
def test_an_unreachable_broker_is_reported_not_raised(
    entry_point: Callable, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # After= only orders the start, so at boot the socket may not be there yet. That is not a
    # bug of ours and must not reach the journal as a traceback.
    conf = tmp_path / "w.conf"
    _write_valid_config(conf)
    monkeypatch.setattr(service, "signal", _FakeSignals())
    client = FakeClient()

    def refuse_connection() -> None:
        raise ConnectionRefusedError("mosquitto is not listening yet")

    monkeypatch.setattr(client, "start", refuse_connection)
    assert entry_point(str(conf), client=client) == service.EXIT_FAILURE


def test_on_reading_survives_a_payload_that_is_not_utf8() -> None:
    # The callback runs on the paho thread, where an exception kills the reading flow for good,
    # so a foreign topic publishing bytes that are not UTF-8 must decode with replacements.
    config = _config(Device("K1", [Channel("d/c", 0)]))
    topic = config.devices[0].channels[0].mqtt_topic
    service_instance, client = _service(config)
    service_instance._wb_devices.create()
    service_instance._on_reading(None, None, Message(b"12.5\xff", topic))
    assert service_instance._source_values[topic] == "12.5\ufffd"
    assert client.last(f"{KEY_DEVICE1_BASE}/controls/ch0") == "12.5\ufffd"


def test_main_cleanup_clears_devices(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("wb.mqtt_waterius.mqtt_device._scan_retained", lambda *_args: [])
    client = FakeClient()
    assert service.main_cleanup(client=client) == service.EXIT_SUCCESS
    assert (f"{INTEGRATION_BASE}/meta", "", True, 1) in client.published  # integration device wiped


def test_main_cleanup_reports_a_broker_error(monkeypatch: pytest.MonkeyPatch) -> None:
    # A broker that is down must not turn a manual cleanup into a traceback, but the exit code
    # has to say it failed, nobody is coming after this command.
    def boom(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("broker down")

    monkeypatch.setattr(service, "clear_all", boom)
    assert service.main_cleanup(client=FakeClient()) == service.EXIT_FAILURE


def test_main_cleanup_reports_an_unconfirmed_wipe(monkeypatch: pytest.MonkeyPatch) -> None:
    # The publishes went out but the broker never confirmed them, so the devices may still be there.
    monkeypatch.setattr("wb.mqtt_waterius.mqtt_device._scan_retained", lambda *_args: [])
    monkeypatch.setattr(service, "wait_for_broker", lambda *_args, **_kwargs: False)
    assert service.main_cleanup(client=FakeClient()) == service.EXIT_FAILURE


def test_main_cleanup_uses_distinct_client_id(monkeypatch: pytest.MonkeyPatch) -> None:
    # The cleanup path must not reuse the daemon's client id (would knock it off the broker).
    created = {}

    def fake_mqtt(client_id: str) -> FakeClient:
        created["id"] = client_id
        return FakeClient()

    monkeypatch.setattr(service, "MQTTClient", fake_mqtt)
    monkeypatch.setattr("wb.mqtt_waterius.mqtt_device._scan_retained", lambda *_args: [])
    assert service.main_cleanup() == service.EXIT_SUCCESS
    assert created["id"] == "wb-mqtt-waterius-cleanup"


def test_main_send_uses_distinct_client_id(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # A manual send builds its own "-send" client id so it does not collide with the daemon.
    created = {}

    def fake_mqtt(client_id: str) -> FakeClient:
        created["id"] = client_id
        return FakeClient()

    monkeypatch.setattr(service, "MQTTClient", fake_mqtt)
    monkeypatch.setattr("wb.mqtt_waterius.mqtt_device._scan_retained", lambda *_args: [])
    # A missing config takes the _report_config_error branch, which builds the fallback client.
    assert service.main_send_once(str(tmp_path / "nope.conf")) == service.EXIT_CONFIG_ERROR
    assert created["id"] == "wb-mqtt-waterius-send"


def test_a_stop_during_startup_never_reaches_the_service(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The handlers go up before the config is read, so a stop that early ends the startup.
    signals = _FakeSignals()
    monkeypatch.setattr(service, "signal", signals)

    def stop_while_loading(_path: Optional[str] = None) -> Any:
        signals.handlers[signals.SIGINT](signals.SIGINT, None)
        raise AssertionError("startup went on after the stop")

    monkeypatch.setattr(service, "load_config", stop_while_loading)
    with pytest.raises(SystemExit) as exit_info:
        service.main_daemon(str(tmp_path / "wb.conf"), client=FakeClient())
    assert exit_info.value.code == service.EXIT_NOT_RUNNING


def test_the_daemon_clears_stale_topics_and_exits_when_no_devices_are_configured(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "wb.conf").write_text(
        '{"sendTime": "03:00", "daysOfWeek": ["monday"], "devices": []}', encoding="utf-8"
    )
    monkeypatch.setattr(service, "signal", _FakeSignals())
    monkeypatch.setattr(service.Service, "run", lambda self: pytest.fail("run() must not start"))
    monkeypatch.setattr(
        "wb.mqtt_waterius.mqtt_device._scan_retained",
        lambda *_args: [f"{INTEGRATION_BASE}/controls/state/meta/error"],
    )
    client = FakeClient()
    assert service.main_daemon(str(tmp_path / "wb.conf"), client=client) == service.EXIT_NOT_RUNNING
    assert client.last(f"{INTEGRATION_BASE}/controls/state/meta/error") == ""
    assert client.stopped


def test_an_unchanged_state_file_is_not_rewritten(monkeypatch: pytest.MonkeyPatch) -> None:
    # Every start and every manual send prunes, and a write with nothing to drop costs an eMMC
    # block erase for no change at all.
    config = _config(Device("K1", [Channel("d/c", 0)]))
    service_instance, _ = _service(
        config, stored_state={"last_sent": {state.key_hash("K1"): "2026-07-16T12:00:00"}}
    )
    writes: list[dict] = []
    monkeypatch.setattr(service, "save_state", writes.append)
    service_instance._prune_sent_moments()
    assert not writes
