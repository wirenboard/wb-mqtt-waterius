"""
Fakes shared by the tests.
"""

from collections.abc import Callable
from typing import Any, Optional, Union

from wb.mqtt_waterius.config import WEEKDAYS

ALL_DAYS = set(range(len(WEEKDAYS)))


def topic_matches(wildcard: str, topic: str) -> bool:
    """
    Match a topic against an MQTT wildcard, "+" for one level and "#" for the rest.

    Examples:
        >>> topic_matches("/devices/+/meta", "/devices/lamp/meta")
        True
        >>> topic_matches("/devices/lamp/#", "/devices/lamp/controls/x/meta")
        True
        >>> topic_matches("/devices/lamp/#", "/devices/lamp")
        True
        >>> topic_matches("/devices/lamp/#", "/devices/other/meta")
        False
    """
    wildcard_parts, topic_parts = wildcard.split("/"), topic.split("/")
    if wildcard_parts[-1] == "#":
        wildcard_parts = wildcard_parts[:-1]
        if len(topic_parts) < len(wildcard_parts):
            return False
    elif len(wildcard_parts) != len(topic_parts):
        return False
    return all(part in ("+", topic_part) for part, topic_part in zip(wildcard_parts, topic_parts))


class Message:  # pylint: disable=too-few-public-methods
    """
    Stand-in for paho's MQTTMessage handed to a callback.
    """

    def __init__(self, payload: Union[str, bytes], topic: str = "") -> None:
        self.payload = payload.encode() if isinstance(payload, str) else payload
        self.topic = topic


class FakeClient:
    """
    In-memory MQTT client stand-in shared by the tests.

    Records every publish as a (topic, payload, retain, qos) tuple, remembers subscriptions
    and callbacks, and hands a published message back to a matching callback the way a broker
    does with its own subscribers.
    """

    def __init__(self) -> None:
        self.published: list[tuple[str, Any, bool, int]] = []
        self.subscribed: list[str] = []
        self.callbacks: dict[str, Callable] = {}
        self.on_connect: Optional[Callable] = None
        self.will: Optional[tuple] = None
        self.will_at_connect: Optional[tuple] = None
        self.stopped = False

    def publish(self, topic: str, payload: Any, retain: bool = False, qos: int = 0) -> None:
        self.published.append((topic, payload, retain, qos))
        for wildcard, callback in list(self.callbacks.items()):
            if wildcard in self.subscribed and topic_matches(wildcard, topic):
                callback(self, None, Message(payload, topic))

    def subscribe(self, topic: str) -> None:
        self.subscribed.append(topic)

    def unsubscribe(self, topic: str) -> None:
        if topic in self.subscribed:
            self.subscribed.remove(topic)

    def message_callback_add(self, topic: str, callback: Callable) -> None:
        self.callbacks[topic] = callback

    def message_callback_remove(self, topic: str) -> None:
        self.callbacks.pop(topic, None)

    def will_set(self, topic: str, payload: str, retain: bool = False) -> None:
        self.will = (topic, payload, retain)

    def start(self) -> None:
        # Real paho sends only the will registered before the connection, so remember what was
        # armed by then. Then simulate the broker's CONNACK, otherwise a caller waiting on the
        # connection event would block. Real paho fires on_connect on the network thread.
        self.will_at_connect = self.will
        if self.on_connect is not None:
            self.on_connect(self, None, {}, 0)

    def stop(self) -> None:
        self.stopped = True

    def last(self, topic: str) -> Any:
        for published_topic, payload, _, _ in reversed(self.published):
            if published_topic == topic:
                return payload
        return None
