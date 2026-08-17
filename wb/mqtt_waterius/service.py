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
from wb.mqtt_waterius.schedule import format_datetime, next_run
from wb.mqtt_waterius.state import key_hash, load_state, save_state
from wb.mqtt_waterius.version import get_version
from wb.mqtt_waterius.waterius_api import (
    DEFAULT_ENDPOINT,
    KEY_NOT_FOUND_CODE,
    ChannelReading,
    WateriusClient,
    build_payload,
    mask_key,
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
        self._wake_event = threading.Event()  # interrupts the poll sleep (reconnect or stop)

        self._source_values: dict[str, str] = {}  # mqtt topic -> raw string payload
        self._pending_retry: set[int] = set()  # device indices with a transient error, retried each poll
        self._failed_hold: set[int] = set()  # device indices errored but not retried (404 or gave up)
        self._pending_day: Optional[datetime.date] = None  # day of the send batch, scopes the retry
        self._last_fire: Optional[tuple[str, int, int]] = None  # (date, hour, minute) of the last send
        self._last_next_run: Optional[str] = None  # last published "Next Execution", skips repeats

        self._state = load_state()
        self._reset_day_marker_if_time_changed()
        restore = self._prune_display_timestamps()
        self._enabled = self._state["enabled"]

        # client is injectable for tests and for the entry points that need a distinct id
        # (a manual send must not reuse the daemon's id, see main_send_once). Default is the daemon.
        self._client = client or MQTTClient(CLIENT_ID)
        self._wb_devices = WateriusDevices(
            self._client,
            on_toggle=self._on_toggle,
            config=config,
            version=get_version(),
            last_sent=restore,
        )

    def _reset_day_marker_if_time_changed(self) -> None:
        """
        A new scheduled time is a new slot, so the day stops counting as sent.

        This matters in one case only, when the new time has already passed today and today
        was already sent. Without the reset that edit would take effect tomorrow.
        """
        if self._state["schedule_time"] != self._config.send_time:
            self._save_state_values(last_sent_date=None, schedule_time=self._config.send_time)

    def _prune_display_timestamps(self) -> list[str]:
        """
        Drop the stamps of devices no longer in the config, then persist what is left.

        Returns:
            Stamps of the configured devices in config order, empty for a device with none
        """
        key_hashes = [key_hash(device.key) for device in self._config.devices]
        kept = {
            hashed_key: stamp
            for hashed_key, stamp in self._state["last_sent"].items()
            if hashed_key in key_hashes
        }
        self._save_state_values(last_sent=kept)
        return [kept.get(hashed_key, "") for hashed_key in key_hashes]

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
        # Runs on the paho thread, the write goes through the lock.
        self._enabled = enabled
        self._save_state_values(enabled=enabled)
        self._wb_devices.set_enabled(enabled)
        self._apply_resting_state()
        self._refresh_time_display()
        logger.info("Automatic sending %s", "enabled" if enabled else "disabled")

    def _refresh_time_display(self) -> None:
        """
        Refresh the two clock controls of the integration device.

        The next-run text changes once a day at most, so it goes out only when it differs from
        the one published last, instead of 1440 identical writes. The clean slate in
        _setup_mqtt wipes the control, so it clears the memo as well.
        """
        now = self._datetime_now()
        self._wb_devices.set_current_time(format_datetime(now))

        if self._enabled and self._config.devices:
            next_dt = next_run(self._send_hour, self._send_minute, self._config.days_of_week, now)
            next_text = format_datetime(next_dt) if next_dt else NO_TIME
        else:
            next_text = NO_TIME

        if next_text != self._last_next_run:
            self._wb_devices.set_next_run(next_text)
            self._last_next_run = next_text

    def _on_reading(self, _client: MQTTClient, _userdata: Any, message: Any) -> None:
        raw = message.payload.decode(errors="replace")
        self._source_values[message.topic] = raw
        self._wb_devices.update_channel(message.topic, raw)

    def _subscribe_readings(self) -> None:
        for topic in self._config.all_topics():
            self._client.subscribe(topic)
            self._client.message_callback_add(topic, self._on_reading)

    def _channel_value(self, channel: Channel) -> Optional[float]:
        raw = self._source_values.get(channel.mqtt_topic)

        if raw is None or raw == "":
            logger.warning("No value yet for %s", channel.mqtt_topic)
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
        did. The minute retries do not wait again, by then a missing value is really missing.

        A value that never arrives is not reported here. The device that needs it is skipped
        by the send itself, which names both the device and the topics.
        """
        topics = self._config.all_topics()
        missing = [topic for topic in topics if not self._source_values.get(topic)]

        for _ in range(int(READINGS_TIMEOUT / READINGS_POLL)):
            if not missing:
                return
            if self._stop_event.wait(READINGS_POLL):
                return
            missing = [topic for topic in topics if not self._source_values.get(topic)]

    def _get_snapshot(self, device: Device) -> tuple[list[ChannelReading], list[str]]:
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
        values = [(channel, self._channel_value(channel)) for channel in device.channels]
        missing = [channel.topic for channel, value in values if value is None]
        if missing:
            return [], missing
        return [ChannelReading(ch.topic, ch.data_type, value, ch.serial) for ch, value in values], []

    def dry_run_payloads(self) -> bool:
        """
        Build and log the payload of every configured device without sending anything.

        Nothing outside is touched, not the cloud, not the WB devices, not the saved state.

        Returns:
            True when every device had a full set of readings
        """
        if not self._config.devices:
            logger.info("No devices configured, nothing to send")
            return False
        self._await_readings()
        complete = True

        for device in self._config.devices:
            key = mask_key(device.key)
            readings, missing = self._get_snapshot(device)
            if missing:
                logger.warning("Device %s: no data from channels: %s", key, ", ".join(missing))
                complete = False
                continue
            payload = dict(build_payload(device.key, device.name, readings), key=key)
            logger.info("[dry-run] Would POST to %s, payload %s", self._endpoint, payload)
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
        key = mask_key(device.key)
        readings, missing = self._get_snapshot(device)

        if missing:
            detail = "No data from channels: " + ", ".join(missing)
            logger.warning("Device %s: %s, skipping send", key, detail)
            self._wb_devices.mark_device_failed(index, detail)
            return SEND_TRANSIENT  # the value may arrive later, so keep retrying today

        payload = build_payload(device.key, device.name, readings)
        # Handing over the stop event makes the retries interruptible, so a shutdown during
        # the linear backoff aborts the remaining attempts instead of blocking.
        result = api.send(payload, stop_event=self._stop_event)
        logger.info("Device %s, send result %s", key, result)

        if result.ok:
            # Stamped when this device answered, not when the batch started, because devices
            # are sent one at a time and a retried one answers minutes later.
            timestamp = self._datetime_now().strftime(TIMESTAMP_FORMAT)
            self._wb_devices.mark_device_sent(index, timestamp)
            stamps = {**self._state["last_sent"], key_hash(device.key): timestamp}
            self._save_state_values(last_sent=stamps)
            return SEND_OK

        self._wb_devices.mark_device_failed(index, result.error or f"HTTP {result.status_code}")
        # A 404 means the key is not registered, so retrying it today is pointless and it is
        # marked permanent. Any other failure (network, 503, non-404 HTTP) is transient.
        return SEND_PERMANENT if result.status_code == KEY_NOT_FOUND_CODE else SEND_TRANSIENT

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
        Full scheduled send, POSTs every configured device once.

        Devices are not skipped one by one. The scheduled time decides, so a device already
        sent earlier today goes out again with fresh values. Transient failures go to the
        retry queue, a 404 is held red without retry. Marks the day as sent and returns True
        only if nothing errored. Safe from any thread.
        """
        with self._send_lock:
            if not self._config.devices:
                logger.info("No devices configured, nothing to send")
                self._wb_devices.set_state(STATE_CONFIG_INVALID)
                self._wb_devices.set_integration_error(True)
                return False

            self._await_readings()
            now = self._datetime_now()
            today = now.date().isoformat()
            all_device_indices = list(range(len(self._config.devices)))

            pending, held, aborted = self._send_devices_data(all_device_indices)
            self._pending_retry = pending
            self._failed_hold = held
            self._pending_day = now.date()

            # Mark the day as sent, with the minute it happened, so the schedule does not
            # start over today. Transient devices retry via the poll loop instead.
            # An aborted send (SIGTERM mid-way) leaves the day open so a restart catches up.
            if not aborted:
                self._last_fire = (today, now.hour, now.minute)
                self._save_state_values(last_sent_date=today, schedule_time=self._config.send_time)

            self._update_integration_error()
            if aborted:
                return False
            return not (pending or held)

    def _retry_pending(self) -> None:
        """
        Re-POST only the devices that had a transient error, once.

        Runs on the poll loop every minute until they succeed. A success clears the device,
        a 404 moves it to hold.
        """
        with self._send_lock:
            if not self._pending_retry:
                return
            pending, held, _aborted = self._send_devices_data(sorted(self._pending_retry))
            self._pending_retry = pending
            self._failed_hold |= held
        self._update_integration_error()

    def _update_integration_error(self) -> None:
        """
        Aggregate device errors onto the integration device.

        Red plus Has Errors if anything is pending-retry or held, otherwise the resting
        state — Active, Disabled or Config Not Valid.
        """
        if self._pending_retry or self._failed_hold:
            self._wb_devices.set_integration_error(True)
            self._wb_devices.set_state(STATE_HAS_ERRORS)
        else:
            self._apply_resting_state()

    def _should_send_now(self) -> bool:
        """
        Whether a full scheduled send should fire now.

        At exactly the scheduled minute it always fires, even if already sent today, but
        only once for that minute. Past the minute, which means a missed poll, it catches
        up once if today has not sent yet.
        """
        if not self._enabled or not self._config.devices:
            return False
        now = self._datetime_now()
        if now.weekday() not in self._config.days_of_week:
            return False
        today = now.date().isoformat()
        if (now.hour, now.minute) == (self._send_hour, self._send_minute):
            return self._last_fire != (today, self._send_hour, self._send_minute)
        if (now.hour, now.minute) > (self._send_hour, self._send_minute):
            return self._state["last_sent_date"] != today
        return False

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

    def _retry_pending_if_same_day(self) -> None:
        """
        Retry the pending devices, but only within the day of the send.

        At a day rollover the reading is stale, so retrying stops and the still-failed
        devices are held as errored, shown red, until the next scheduled send rebuilds
        everything.
        """
        if self._datetime_now().date() == self._pending_day:
            self._retry_pending()
        else:
            self._failed_hold |= self._pending_retry
            self._pending_retry = set()
            self._update_integration_error()

    def _setup_mqtt(self) -> None:
        """
        Establish our MQTT presence, on the first connect and on every reconnect.

        Runs from the main thread, so the retained scan inside clear() works while the
        network loop keeps delivering messages. Clean-slate, our retained devices are wiped
        and then recreated from the config.
        """
        self._wb_devices.clear()
        self._wb_devices.create()
        self._wb_devices.subscribe_switch()
        self._subscribe_readings()
        self._wb_devices.set_enabled(self._enabled)
        self._last_next_run = None  # the clean slate wiped the control, so publish it again
        self._refresh_time_display()
        self._apply_resting_state()

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
        One pass of the poll loop, once a minute and right after a (re)connect.

        The MQTT setup is repeated only when a connect asked for it, not on every pass. Then
        the displayed times are refreshed, and then either the scheduled send fires or the
        devices left over from it are retried.
        """
        if self._resetup_event.is_set():
            self._resetup_event.clear()
            self._setup_mqtt()

        self._refresh_time_display()

        if self._should_send_now():
            self.send_now()
        elif self._pending_retry:
            self._retry_pending_if_same_day()

    def run(self) -> None:
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
            self._wake_event.wait(POLL_INTERVAL)  # wakes early on reconnect or stop
            self._wake_event.clear()

        self._client.stop()

    def stop_service(self) -> None:
        self._stop_event.set()
        self._wake_event.set()

    def run_once(self, dry_run: bool = False) -> bool:
        """
        Connect, send once, disconnect. For manual use from the CLI.

        Waiting for the source values belongs to the send itself, so this only connects,
        subscribes and dispatches. Unlike the daemon it makes a single pass with no retry, so
        a value that has not arrived by then is reported instead of being picked up later.

        Args:
            dry_run: build and log the payloads instead of posting them

        Returns:
            True when every device was sent, or on a dry run had a full set of readings
        """
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
    service = _prepare_service(config_path, client)
    if service is None:
        return EXIT_CONFIG_ERROR

    def _handle_stop(_signum: int, _frame: Optional[FrameType]) -> None:
        logger.info("Stopping")
        service.stop_service()

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGHUP, _handle_stop)
    service.run()

    return EXIT_SUCCESS


def main_send_once(
    config_path: Optional[str] = None, client: Optional[MQTTClient] = None, dry_run: bool = False
) -> int:
    # A distinct client id so a manual `send` while the daemon runs does not knock the
    # daemon off the broker (same id = the broker drops the incumbent session).
    service = _prepare_service(config_path, client or MQTTClient(f"{CLIENT_ID}-send"))
    if service is None:
        return EXIT_CONFIG_ERROR
    return EXIT_SUCCESS if service.run_once(dry_run=dry_run) else EXIT_FAILURE


def main_cleanup(client: Optional[MQTTClient] = None) -> int:
    """
    Remove the integration device and every key device from the broker. Used on removal.
    """
    client = client or MQTTClient(f"{CLIENT_ID}-cleanup")
    try:
        connect_and_wait(client)
        wiped_topics = clear_all(client)
        # Nothing will retry after an uninstall, so make the broker confirm the wipe.
        confirmed = wait_for_broker(client)
        client.stop()
        if confirmed:
            logger.info("Waterius devices cleared, %d topics", len(wiped_topics))
        else:
            logger.warning("Broker did not confirm clearing %d topics", len(wiped_topics))
    except Exception as exc:  # pylint:disable=broad-except
        logger.warning("Cleanup failed: %s", exc)
    return EXIT_SUCCESS
