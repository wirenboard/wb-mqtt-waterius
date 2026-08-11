"""
Fakes shared by the tests.
"""

from collections.abc import Callable
from typing import Optional

from wb.mqtt_waterius.config import WEEKDAYS

ALL_DAYS = set(range(len(WEEKDAYS)))


class FakeMessageInfo:  # pylint: disable=too-few-public-methods
    """
    Stand-in for paho's MQTTMessageInfo returned by publish().

    wait_for_publish is a no-op — the fake delivers synchronously, so there is nothing
    to flush.
    """

    def wait_for_publish(self, timeout: Optional[float] = None) -> None:  # pylint: disable=unused-argument
        return None


class FakeClient:
    """
    In-memory MQTT client stand-in shared by the tests.

    Records every publish as a (topic, payload, retain, qos) tuple and remembers
    subscriptions and callbacks.
    """

    def __init__(self) -> None:
        self.published = []
        self.subscribed = []
        self.callbacks = {}
        self.on_connect: Optional[Callable] = None

    def publish(self, topic: str, payload: str, retain: bool = False, qos: int = 0) -> FakeMessageInfo:
        self.published.append((topic, payload, retain, qos))
        return FakeMessageInfo()

    def subscribe(self, topic: str) -> None:
        self.subscribed.append(topic)

    def unsubscribe(self, topic: str) -> None:
        pass

    def message_callback_add(self, topic: str, callback: Callable) -> None:
        self.callbacks[topic] = callback

    def message_callback_remove(self, topic: str) -> None:
        self.callbacks.pop(topic, None)

    def start(self) -> None:
        # Simulate the broker's CONNACK so a caller waiting on the connection event does not
        # block. Real paho fires on_connect on the network thread.
        if self.on_connect is not None:
            self.on_connect(self, None, {}, 0)

    def stop(self) -> None:
        pass

    def last(self, topic: str) -> Optional[str]:
        for published_topic, payload, _, _ in reversed(self.published):
            if published_topic == topic:
                return payload
        return None
