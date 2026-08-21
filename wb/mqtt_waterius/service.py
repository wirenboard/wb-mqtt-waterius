"""
wb-mqtt-waterius daemon.

Reads WB meter topics, sends them to Waterius on the scheduled days and time, and
publishes the WB status devices — the integration device and one device per key.
"""

import datetime
import logging
import signal
import threading
from collections.abc import Callable
from types import FrameType
from typing import Any, Optional

from wb_common.mqtt_client import MQTTClient

from wb.mqtt_waterius.config import Channel, Config, ConfigError, Device, load_config
from wb.mqtt_waterius.mqtt_device import (
    NO_TIME,
    STATE_ACTIVE,
    STATE_CONFIG_INVALID,
    STATE_DISABLED,
    STATE_HAS_ERRORS,
    IntegrationDevice,
    WateriusDevices,
    clear_all,
    wait_for_broker,
)
from wb.mqtt_waterius.schedule import (
    format_datetime,
    get_due_send_time,
    get_unsent_device_positions,
    next_run,
)
from wb.mqtt_waterius.state import State, key_hash, load_state, save_state
from wb.mqtt_waterius.version import get_version
from wb.mqtt_waterius.waterius_api import (
    DEFAULT_ENDPOINT,
    KEY_NOT_FOUND_CODE,
    ChannelData,
    WateriusClient,
    build_payload,
    key_prefix,
)

CLIENT_ID = "wb-mqtt-waterius"
POLL_INTERVAL = 60  # seconds between schedule checks
SEND_GAP = 1.5  # seconds between per-device POSTs, to dodge the nginx 503 rate-limit
READINGS_TIMEOUT = 5  # seconds to wait for the source values of a send
READINGS_POLL = 0.2  # seconds between checks while waiting for them
CONNECT_TIMEOUT = 10  # seconds to wait for the MQTT connection
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"  # the Last Sent display value

# _send_device outcomes. A permanent failure (HTTP 404, the key is not registered) is not
# retried again today — nothing changes without editing the key. A transient failure
# (missing source data, network, 503) keeps the day open so the poll retries.
SEND_OK = "ok"
SEND_TRANSIENT = "transient"
SEND_PERMANENT = "permanent"

# Exit codes. Code 6 (config error) is excluded from systemd auto-restart by the unit's
# RestartPreventExitStatus, so a broken config stops cleanly instead of crash-looping.
# Saving a fixed config in the web UI makes confed restart the service.
EXIT_SUCCESS = 0
EXIT_FAILURE = 1
EXIT_CONFIG_ERROR = 6

logger = logging.getLogger(__name__)


def _display_stamp(moment: Optional[str]) -> str:
    """
    Format a stored moment for the Last Sent control, empty string when there is none.
    """
    return datetime.datetime.fromisoformat(moment).strftime(TIMESTAMP_FORMAT) if moment else ""


def _wait_connected(connected_event: threading.Event, timeout: int) -> None:
    if not connected_event.wait(timeout):
        logger.warning("MQTT not connected within %ss, proceeding anyway", timeout)


def connect_and_wait(client: MQTTClient, timeout: int = CONNECT_TIMEOUT) -> None:
    """
    Start the client and block until it is connected.

    paho drops pre-CONNACK subscribes, so the following scan, subscribe and publish
    would otherwise race and under-discover.
    """
    connected_event = threading.Event()

    def _on_connect(_client: MQTTClient, _userdata: Any, _flags: Any, rc: int) -> None:
        if rc == 0:
            connected_event.set()

    client.on_connect = _on_connect
    client.start()
    _wait_connected(connected_event, timeout)


class Service:  # pylint: disable=too-many-instance-attributes
    """
    The daemon itself: schedule, sending, persistent state and the WB devices around them.

    Lives in two threads. The poll loop owns the schedule, the sending and every publish to the
    status controls, while the paho thread only brings in source readings and switch clicks and
    hands the rest to the loop through events.

    Args:
        config: parsed configuration
        endpoint: Waterius universal API URL
        datetime_now_fn: source of the current time, injected so tests can freeze or advance it
        client: MQTT client, injected by the entry points that need their own id and by tests
    """

    def __init__(
        self,
        config: Config,
        endpoint: str = DEFAULT_ENDPOINT,
        datetime_now_fn: Optional[Callable[[], datetime.datetime]] = None,
        client: Optional[MQTTClient] = None,
    ) -> None:
        self._config = config
        self._endpoint = endpoint
        self._send_hour, self._send_minute = config.send_hour_minute

        # The current time comes from an injectable function so tests can freeze or advance it.
        # Everything time-dependent goes through it, the schedule check, the displayed times
        # and the Last Sent stamps.
        self._datetime_now = datetime_now_fn or datetime.datetime.now

        self._send_lock = threading.Lock()
        self._state_lock = threading.Lock()  # serializes state mutation and save across threads

        self._stop_event = threading.Event()
        self._connected_event = threading.Event()  # set once the first CONNACK arrives
        self._resetup_event = threading.Event()  # set on every (re)connect, re-publish devices
        self._toggle_event = threading.Event()  # set by the switch, the loop republishes the status
        self._wake_event = threading.Event()  # interrupts the poll sleep (reconnect or stop)

        self._source_values: dict[str, str] = {}  # mqtt topic -> raw string payload
        self._failed_transient: set[int] = set()  # device indices that failed their last attempt
        self._failed_hold: set[int] = set()  # device indices errored but not retried (404 or gave up)
        self._last_schedule_run: Optional[tuple[str, int, int]] = None  # (date, hour, minute) already run

        self._state: State = load_state()
        stamps = self._get_sent_stamps()
        self._enabled = self._state["enabled"]

        # client is injectable for tests and for the entry points that need a distinct id
        # (a manual send must not reuse the daemon's id, see main_send_once). Default is the daemon.
        self._client = client or MQTTClient(CLIENT_ID)
        self._wb_devices = WateriusDevices(
            self._client,
            on_toggle=self._on_toggle,
            config=config,
            version=get_version(),
            last_sent=stamps,
        )

    def _get_sent_stamps(self) -> list[str]:
        """
        Display stamps of the configured devices in config order, empty where none.
        """
        return [
            _display_stamp(self._state["last_sent"].get(key_hash(device.key)))
            for device in self._config.devices
        ]

    def _prune_sent_moments(self) -> None:
        """
        Drop the moments of devices no longer in the config, in memory and on disk.

        A config edit restarts the service, so a key removed in the configurator takes its
        moment out of the state file with it. A daemon start and a real send call this, the
        constructor does not — it also runs on a config save and on a dry run.
        """
        key_hashes = {key_hash(device.key) for device in self._config.devices}
        configured_moments = {
            hashed_key: moment
            for hashed_key, moment in self._state["last_sent"].items()
            if hashed_key in key_hashes
        }
        dropped = len(self._state["last_sent"]) - len(configured_moments)
        if not dropped:
            return
        # A device that comes back the same day sends again, and the journal has to say why.
        logger.info("Dropped %d sent marks of devices no longer in the config", dropped)
        self._save_state_values(last_sent=configured_moments)

    def _apply_resting_state(self) -> None:
        """
        Set the steady-state status.

        Invalid config raises the error flag on the State control, the disabled and active
        resting states clear it.
        """
        if not self._config.devices:
            self._wb_devices.set_state(STATE_CONFIG_INVALID)
            self._wb_devices.set_integration_error(True)
        elif not self._enabled:
            self._wb_devices.set_integration_error(False)
            self._wb_devices.set_state(STATE_DISABLED)
        else:
            self._wb_devices.set_integration_error(False)
            self._wb_devices.set_state(STATE_ACTIVE)

    def _save_state_values(self, **values: Any) -> None:
        """
        Put the given keys into the persistent state and write the file, in one step.

        Every change of the state file goes through here. The lock serializes the paho thread
        against the main-thread send, so a reader never sees half of an update and two writers
        never interleave.
        """
        with self._state_lock:
            self._state.update(values)
            save_state(self._state)

    def _on_toggle(self, enabled: bool) -> None:
        """
        Take the switch position, on the paho thread.

        Records it, persists it and wakes the poll loop, which publishes what the switch
        changed. Nothing goes out from here, the poll pass is the only writer of those controls.
        """
        self._enabled = enabled
        self._save_state_values(enabled=enabled)
        logger.info("Automatic sending %s", "enabled" if enabled else "disabled")
        self._toggle_event.set()
        self._wake_event.set()

    def _publish_current_time(self) -> None:
        """
        Publish the clock control. Its value changes every minute, so it goes out every pass.
        """
        self._wb_devices.set_current_time(format_datetime(self._datetime_now()))

    def _publish_next_run(self) -> None:
        """
        Publish the next-run control.

        Three occasions move it — a (re)connect, which wipes the control, the first schedule
        pass of the day past the slot, which takes it to the next allowed day, and the switch,
        which turns the text into a dash and back. Published at those three points, so nothing
        has to remember what the broker was given last.
        """
        now = self._datetime_now()
        if self._enabled and self._config.devices:
            next_dt = next_run(self._send_hour, self._send_minute, self._config.days_of_week, now)
            next_text = format_datetime(next_dt) if next_dt else NO_TIME
        else:
            next_text = NO_TIME
        self._wb_devices.set_next_run(next_text)

    def _publish_status(self) -> None:
        """
        Publish everything on the integration device except the clock.

        The switch position, the state with its error flag and the next-run text. Called after a
        (re)connect, which wipes them, and after a switch click, which changes all three.
        """
        self._wb_devices.set_enabled(self._enabled)
        self._update_integration_error()
        self._publish_next_run()

    def _on_reading(self, _client: MQTTClient, _userdata: Any, message: Any) -> None:
        raw = message.payload.decode(errors="replace")
        self._source_values[message.topic] = raw
        self._wb_devices.update_channel(message.topic, raw)

    def _subscribe_readings(self) -> None:
        for topic in self._config.all_topics():
            self._client.subscribe(topic)
            self._client.message_callback_add(topic, self._on_reading)

    def _unsubscribe_readings(self) -> None:
        """
        Drop the source subscriptions, before the devices are taken off the broker.

        The callback goes first, so a reading already on its way cannot mirror itself into a
        channel the removal has just emptied.
        """
        for topic in self._config.all_topics():
            self._client.message_callback_remove(topic)
            self._client.unsubscribe(topic)

    def _get_channel_value(self, channel: Channel) -> Optional[float]:
        raw = self._source_values.get(channel.mqtt_topic)

        if raw is None or raw == "":
            return None
        try:
            return float(raw)
        except ValueError:
            logger.warning("Value of %s is not a number: %r", channel.mqtt_topic, raw)
            return None

    def _await_readings(self) -> None:
        """
        Wait until every configured source topic has a value, or the timeout runs out.

        WB publishes channel values retained, so normally they are already here and nothing
        is waited for. The wait covers one case, a send starting seconds after the service
        did. A catch-up batch waits the same way, bounded by READINGS_TIMEOUT and free when
        the values are already in.

        A value that never arrives is not reported here. The device that needs it is skipped
        by the send itself, which names both the device and the topics.
        """
        topics = self._config.all_topics()
        for _ in range(int(READINGS_TIMEOUT / READINGS_POLL)):
            if all(self._source_values.get(topic) for topic in topics):
                return
            if self._stop_event.wait(READINGS_POLL):
                return

    def _get_snapshot(self, device: Device) -> tuple[list[ChannelData], list[str]]:
        """
        Read every channel of one device once.

        The paho thread keeps writing into the source values, so reading them twice could
        give two different sets. Reading once means the check and the payload see the same
        data.

        Args:
            device: the device to read

        Returns:
            (readings, missing topics). Readings are empty when anything is missing, because
            Waterius maps values by channel number and a device with a gap would arrive with
            the remaining values shifted onto the wrong meters.
        """
        values = [(channel, self._get_channel_value(channel)) for channel in device.channels]
        missing = [channel.source for channel, value in values if value is None]
        if missing:
            return [], missing
        return [ChannelData(ch.source, ch.data_type, value, ch.serial) for ch, value in values], []

    def dry_run_payloads(self) -> bool:
        """
        Build and print the payload of every configured device without sending anything.

        The cloud is not touched. Bodies go to the terminal so they can be replayed by hand,
        while the journal gets the fact and the cut key — it is readable in the web UI and
        travels in the diagnostic archive. What the command around this method does touch is
        listed in run_once.

        Returns:
            True when every device had a full set of readings
        """
        if not self._config.devices:
            logger.info("No devices configured, nothing to send")
            return False
        self._await_readings()
        complete = True

        for device in self._config.devices:
            key_label = f"{key_prefix(device.key)}*"
            readings, missing = self._get_snapshot(device)
            if missing:
                logger.warning("Device %s: no data from channels: %s", key_label, ", ".join(missing))
                complete = False
                continue
            payload = build_payload(device.key, device.name, readings)
            print(f"[dry-run] Would POST to {self._endpoint}, payload {payload}")
            logger.info("[dry-run] Device %s, %d channels ready", key_label, len(readings))
        return complete

    def _send_device(self, api: WateriusClient, index: int, device: Device) -> str:
        """
        Send one device.

        Args:
            api: cloud client of the current send batch
            index: 0-based position of the device in the config
            device: the device to send

        Returns:
            One of SEND_OK, SEND_TRANSIENT (retry today), SEND_PERMANENT
        """
        key_label = f"{key_prefix(device.key)}*"
        readings, missing = self._get_snapshot(device)

        if missing:
            detail = "No data from channels: " + ", ".join(missing)
            logger.warning("Device %s: %s, skipping send", key_label, detail)
            self._wb_devices.mark_device_failed(index, detail)
            return SEND_TRANSIENT  # the value may arrive later, so keep retrying today

        payload = build_payload(device.key, device.name, readings)
        # Handing over the stop event makes the retries interruptible, so a shutdown during
        # the linear backoff aborts the remaining attempts instead of blocking.
        report = api.send(payload, stop_event=self._stop_event)
        logger.info("Device %s, delivery %s", key_label, report)

        if report.ok:
            # Stamped when this device answered, not when the batch started, because devices
            # are sent one at a time and a retried one answers minutes later.
            now = self._datetime_now()
            self._wb_devices.mark_device_sent(index, now.strftime(TIMESTAMP_FORMAT))
            moment = now.isoformat(timespec="seconds")
            self._save_state_values(last_sent={**self._state["last_sent"], key_hash(device.key): moment})
            return SEND_OK

        self._wb_devices.mark_device_failed(index, report.error or f"HTTP {report.status_code}")
        # A 404 means the key is not registered, so retrying it today is pointless and it is
        # marked permanent. Any other failure (network, 503, non-404 HTTP) is transient.
        return SEND_PERMANENT if report.status_code == KEY_NOT_FOUND_CODE else SEND_TRANSIENT

    def _send_devices_data(self, indices: list[int]) -> tuple[set[int], set[int], bool]:
        """
        Send the given device indices once.

        The client, and with it the HTTP session, lives exactly one batch. The devices of a
        batch go out seconds apart and share one connection, while between batches there is
        nothing worth keeping open.

        Args:
            indices: 0-based positions in the config, in send order

        Returns:
            (pending, held, aborted). Pending are transient errors to retry, held are
            permanent 404s, shown red and not retried.
        """
        pending: set[int] = set()
        held: set[int] = set()
        aborted = False
        sent_any = False
        with WateriusClient(self._endpoint) as api:
            for index in indices:
                device = self._config.devices[index]
                # Space out sends (bursts get a 503), but stay interruptible so a mid-send
                # SIGTERM does not hit TimeoutStopSec.
                if sent_any and self._stop_event.wait(SEND_GAP):
                    aborted = True
                    logger.warning("Send interrupted at device %d", index + 1)
                    break
                outcome = self._send_device(api, index, device)
                if outcome == SEND_TRANSIENT:
                    pending.add(index)
                elif outcome == SEND_PERMANENT:
                    held.add(index)
                sent_any = True
        return pending, held, aborted

    def send_now(self) -> bool:
        """
        Send every configured device once, today's marks ignored and rewritten.

        Used by the scheduled minute and by the manual `wb-mqtt-waterius send`, both of which
        mean "put the current readings in the cabinet" regardless of what went out earlier
        today. Safe from any thread.
        """
        return self._send_batch(list(range(len(self._config.devices))))

    def _send_batch(self, indices: list[int]) -> bool:
        """
        Send the given devices once and reflect the outcome on the WB devices.

        A device that went through is marked for today in _send_device, and that mark is what
        keeps the next poll pass, and a restart, from sending it again.

        Args:
            indices: 0-based positions in the config, in send order

        Returns:
            True when every device of the batch was accepted
        """
        with self._send_lock:
            if not self._config.devices:
                logger.info("No devices configured, nothing to send")
                self._wb_devices.set_state(STATE_CONFIG_INVALID)
                self._wb_devices.set_integration_error(True)
                return False

            self._await_readings()
            pending, held, aborted = self._send_devices_data(indices)
            # Only the devices of this batch change verdict, the ones left out keep theirs.
            batch = set(indices)
            self._failed_transient = (self._failed_transient - batch) | pending
            self._failed_hold = (self._failed_hold - batch) | held

            self._update_integration_error()
            if aborted:
                return False
            return not (pending or held)

    def _get_unsent_devices(self, send_time: datetime.datetime) -> list[int]:
        """
        Devices that have not sent since the given send time, in config order.

        The state side of schedule.get_unsent_device_positions.
        """
        moments = self._state["last_sent"]
        last_sent_by_device = [moments.get(key_hash(device.key)) for device in self._config.devices]
        return get_unsent_device_positions(last_sent_by_device, send_time, self._failed_hold)

    def _update_integration_error(self) -> None:
        """
        Aggregate device errors onto the integration device.

        Red plus Has Errors if anything failed its last attempt or is held, otherwise the resting
        state — Active, Disabled or Config Not Valid.
        """
        if self._failed_transient or self._failed_hold:
            self._wb_devices.set_integration_error(True)
            self._wb_devices.set_state(STATE_HAS_ERRORS)
        else:
            self._apply_resting_state()

    def _send_scheduled(self) -> None:
        """
        The automatic send of one poll pass.

        The scheduled minute itself always sends every device — that is the promise of the
        schedule, and a changed send time is a new slot that fires again the same day. Only
        the repeats after that minute go by the marks: a device without one for today is sent
        again, which covers a failure, a minute missed by a restart and a restart itself, while
        a device held after a 404 waits for the next slot. The manual send obeys none of this,
        see send_now.
        """
        if not self._enabled or not self._config.devices:
            return
        now = self._datetime_now()
        send_time = get_due_send_time(now, self._send_hour, self._send_minute, self._config.days_of_week)
        if send_time is None:
            return
        today = now.date().isoformat()
        # The first pass of the day to get here is the one that finds today's slot behind it,
        # whether it landed on the exact minute or stepped over it, so the next-run text follows.
        first_pass_today = self._last_schedule_run is None or self._last_schedule_run[0] != today
        # A reconnect wakes the poll early, so the same minute can come round twice. Without
        # this the slot would fire twice and a failed device would be re-POSTed seconds apart.
        if self._last_schedule_run == (today, now.hour, now.minute):
            return
        self._last_schedule_run = (today, now.hour, now.minute)
        if first_pass_today:
            self._publish_next_run()
        if now.replace(second=0, microsecond=0) == send_time:
            self.send_now()
            return
        unsent_devices = self._get_unsent_devices(send_time)
        if unsent_devices:
            self._send_batch(unsent_devices)

    def _on_connect(self, _client: MQTTClient, _userdata: Any, _flags: Any, rc: int) -> None:
        """
        Runs on the initial connect and on every automatic reconnect.

        Requests a full re-setup so our devices survive a broker restart. The broker drops
        retained state, and paho does not re-subscribe by itself.
        """
        if rc != 0:
            logger.warning("MQTT connect failed, rc=%s", rc)
            return
        self._connected_event.set()
        self._resetup_event.set()
        self._wake_event.set()

    def _on_disconnect(self, _client: MQTTClient, _userdata: Any, rc: int) -> None:
        if rc != 0:
            logger.warning("MQTT disconnected (rc=%s), paho will reconnect", rc)

    def _setup_mqtt(self) -> None:
        """
        Establish our MQTT presence, on the first connect and on every reconnect.

        Runs from the main thread, so the retained scan inside clear() works while the
        network loop keeps delivering messages. Clean-slate, our retained devices are wiped
        and then recreated from the config. Seeding the values is not its job, the poll pass
        that called it publishes them right after.
        """
        self._wb_devices.clear()
        self._wb_devices.create()
        self._wb_devices.subscribe_switch()
        self._subscribe_readings()

    def _log_startup(self) -> None:
        if not self._config.devices:
            logger.info("Started, no devices configured yet")
        else:
            logger.info(
                "Started, send at %02d:%02d, automatic=%s",
                self._send_hour,
                self._send_minute,
                self._enabled,
            )

    def _poll_once(self) -> None:
        """
        One pass of the poll loop, once a minute and right after a (re)connect or a click.

        The MQTT setup is repeated only when a connect asked for it, and the status controls
        go out with it, because the clean slate wiped them. A switch click asks for the same
        republish through its event, so nothing is published from the paho thread. The clock is
        the only control written on every pass, and then the schedule decides what to send.
        """
        if self._resetup_event.is_set():
            self._resetup_event.clear()
            self._toggle_event.clear()  # the status goes out right below
            self._setup_mqtt()
            self._publish_status()

        if self._toggle_event.is_set():
            self._toggle_event.clear()
            self._publish_status()

        self._publish_current_time()

        self._send_scheduled()

    def run(self) -> None:
        self._prune_sent_moments()
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect

        # Covers an ungraceful death (SIGKILL, OOM), the broker raises the error flag for us.
        # Must come before the connection, paho only sends a will registered by then.
        self._wb_devices.set_last_will()
        self._client.start()
        _wait_connected(self._connected_event, CONNECT_TIMEOUT)
        self._log_startup()

        while not self._stop_event.is_set():
            self._poll_once()
            self._wake_event.wait(POLL_INTERVAL)  # wakes early on a reconnect, a click or a stop
            self._wake_event.clear()

        self._remove_devices()
        self._client.stop()

    def _remove_devices(self) -> None:
        """
        Take our devices off the broker, the last thing a clean stop does.

        Nothing else can, the package scripts run when the service is already down. Only on
        the clean path, a crash has to leave the devices and their error flag up.
        """
        if not self._connected_event.is_set():
            return  # never connected, so nothing of ours reached the broker
        try:
            self._unsubscribe_readings()
            removed = self._wb_devices.remove()
            if not wait_for_broker(self._client):
                logger.warning("Broker did not confirm removing %d topics", len(removed))
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Could not remove the devices: %s", exc)

    def stop_service(self) -> None:
        self._stop_event.set()
        self._wake_event.set()

    def run_once(self, dry_run: bool = False) -> bool:
        """
        Connect, send once, disconnect. For manual use from the CLI.

        Waiting for the source values belongs to the send itself, so this only connects,
        subscribes and dispatches. Unlike the daemon it makes a single pass with no retry, so
        a value that has not arrived by then is reported instead of being picked up later.

        The command shares the devices and the state file with the daemon, in both modes. The
        subscription mirrors the source values onto the key devices and the outcome of a real
        send lands on the integration device, so a manual send shows up on the card the same way
        a scheduled one does. A dry run touches neither the cloud nor the state file.

        Args:
            dry_run: build and log the payloads instead of posting them

        Returns:
            True when every device was sent, or on a dry run had a full set of readings
        """
        if not dry_run:
            self._prune_sent_moments()
        connect_and_wait(self._client)
        self._subscribe_readings()
        ok = self.dry_run_payloads() if dry_run else self.send_now()
        self._client.stop()
        return ok


def _report_config_error(client: MQTTClient) -> None:
    """
    Reflect a broken config in the web UI.

    The daemon exits with EXIT_CONFIG_ERROR and stays down, but its devices are retained in
    the broker and would keep showing "Active", so the integration would look alive while it
    is dead. Wipes the key devices, which the broken config no longer describes, and marks
    the integration device "Config Not Valid" in red. Best-effort, never blocks the exit.
    """
    try:
        connect_and_wait(client)
        clear_all(client)
        integration = IntegrationDevice(client, None, get_version())
        integration.create()
        integration.set_state(STATE_CONFIG_INVALID)
        integration.set_error(True)
        client.stop()
    except Exception as exc:  # pylint: disable=broad-except
        logger.warning("Could not publish config-error status: %s", exc)


def _prepare_service(config_path: Optional[str], client: Optional[MQTTClient]) -> Optional[Service]:
    """
    Shared entry-point prefix, loads the config and builds the service.

    On a config error reflects it in the UI and returns None, so the caller exits with
    EXIT_CONFIG_ERROR.
    """
    try:
        config = load_config(config_path)
    except ConfigError as exc:
        logger.error("Configuration error: %s", exc)
        _report_config_error(client or MQTTClient(CLIENT_ID))
        return None
    return Service(config, client=client)


def main_daemon(config_path: Optional[str] = None, client: Optional[MQTTClient] = None) -> int:
    service: Optional[Service] = None

    def _handle_stop(_signum: int, _frame: Optional[FrameType]) -> None:
        logger.info("Stopping")
        if service is None:
            raise SystemExit(EXIT_SUCCESS)
        service.stop_service()

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGHUP, _handle_stop)

    service = _prepare_service(config_path, client)
    if service is None:
        return EXIT_CONFIG_ERROR

    try:
        service.run()
    except OSError as exc:
        logger.error("MQTT transport error: %s", exc)
        return EXIT_FAILURE

    return EXIT_SUCCESS


def main_send_once(
    config_path: Optional[str] = None, client: Optional[MQTTClient] = None, dry_run: bool = False
) -> int:
    # A distinct client id so a manual `send` while the daemon runs does not knock the
    # daemon off the broker (same id = the broker drops the incumbent session).
    service = _prepare_service(config_path, client or MQTTClient(f"{CLIENT_ID}-send"))
    if service is None:
        return EXIT_CONFIG_ERROR
    try:
        sent = service.run_once(dry_run=dry_run)
    except OSError as exc:
        logger.error("MQTT transport error: %s", exc)
        return EXIT_FAILURE
    return EXIT_SUCCESS if sent else EXIT_FAILURE


def main_cleanup(client: Optional[MQTTClient] = None) -> int:
    """
    Remove the integration device and every key device from the broker.

    Scans the broker for our device ids, empties what it finds under them and waits for the
    confirmation. Run by hand after a service killed without a clean stop, the only way our
    devices are left behind. Ids come from the scan because the config may no longer describe
    them, which is also why a stop, knowing what it created, does not scan.
    """
    client = client or MQTTClient(f"{CLIENT_ID}-cleanup")
    try:
        connect_and_wait(client)
        wiped_topics = clear_all(client)
        # Nothing retries after this command, so make the broker confirm the wipe.
        confirmed = wait_for_broker(client)
        client.stop()
    except Exception as exc:  # pylint:disable=broad-except
        logger.error("Cleanup failed: %s", exc)
        return EXIT_FAILURE
    if not confirmed:
        logger.warning("Broker did not confirm clearing %d topics", len(wiped_topics))
        return EXIT_FAILURE
    logger.info("Waterius devices cleared, %d topics", len(wiped_topics))
    return EXIT_SUCCESS
