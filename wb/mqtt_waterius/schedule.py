"""
Daily send scheduling helpers (pure, testable).
"""

import datetime
from typing import Optional

# Display names indexed by datetime.weekday() (Monday=0). English, the name is part of a
# plain-text value shown in the UI.
WEEKDAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def next_run(hour: int, minute: int, days: set[int], now: datetime.datetime) -> Optional[datetime.datetime]:
    """
    Next occurrence of the given hour and minute on an allowed weekday, strictly after now.

    Args:
        hour: hour of the send time
        minute: minute of the send time
        days: allowed weekdays as datetime.weekday() indices, Monday=0
        now: moment to search forward from

    Returns:
        The next moment, or None when no weekday is allowed

    Examples:
        >>> import datetime
        >>> next_run(12, 0, {3, 4}, datetime.datetime(2026, 7, 16, 12, 0))
        datetime.datetime(2026, 7, 17, 12, 0)
        >>> next_run(12, 0, set(), datetime.datetime(2026, 7, 16, 9, 0)) is None
        True
    """
    if not days:
        return None
    for offset in range(8):
        candidate = (now + datetime.timedelta(days=offset)).replace(
            hour=hour, minute=minute, second=0, microsecond=0
        )
        if candidate.weekday() in days and candidate > now:
            return candidate
    return None


def get_due_send_time(
    now: datetime.datetime, send_hour: int, send_minute: int, send_days: set[int]
) -> Optional[datetime.datetime]:
    """
    Today's send time once it is behind us on an allowed weekday, otherwise None.

    Args:
        send_days: allowed weekdays as datetime.weekday() indices, Monday=0

    Examples:
        >>> import datetime
        >>> get_due_send_time(datetime.datetime(2026, 7, 16, 12, 1), 12, 0, {3})
        datetime.datetime(2026, 7, 16, 12, 0)
        >>> get_due_send_time(datetime.datetime(2026, 7, 16, 11, 59), 12, 0, {3}) is None
        True
        >>> get_due_send_time(datetime.datetime(2026, 7, 16, 12, 1), 12, 0, {0}) is None
        True
    """
    if now.weekday() not in send_days:
        return None
    if (now.hour, now.minute) < (send_hour, send_minute):
        return None
    return now.replace(hour=send_hour, minute=send_minute, second=0, microsecond=0)


def _did_send_before(moment: Optional[str], send_time: datetime.datetime) -> bool:
    """
    Whether the moment is missing or earlier than the send time.
    """
    return not moment or datetime.datetime.fromisoformat(moment) < send_time


def get_unsent_device_positions(
    last_sent_by_device: list[Optional[str]], send_time: datetime.datetime, held_positions: set[int]
) -> list[int]:
    """
    Config positions of the devices with no send since the send time, the held ones left out.

    A failure, a value that had not arrived and a service that was down all look the same here,
    so a restart resumes the day.

    Args:
        last_sent_by_device: ISO moment of each device's last send in config order, None where
            the device has never sent
        held_positions: positions to leave out, held after a permanent failure

    Examples:
        >>> import datetime
        >>> moments = ["2026-07-16T12:00:00", None, None]
        >>> held = {2}
        >>> get_unsent_device_positions(moments, datetime.datetime(2026, 7, 16, 12, 0), held)
        [1]
    """
    return [
        position
        for position, moment in enumerate(last_sent_by_device)
        if position not in held_positions and _did_send_before(moment, send_time)
    ]


def format_datetime(dt: datetime.datetime) -> str:
    """
    Format a moment for a text control in the UI.

    Args:
        dt: moment to format

    Returns:
        String as 'Weekday YYYY-MM-DD HH:MM' with an English weekday name

    Examples:
        >>> import datetime
        >>> format_datetime(datetime.datetime(2026, 7, 16, 18, 37))
        'Thursday 2026-07-16 18:37'
    """
    return f"{WEEKDAY_NAMES[dt.weekday()]} {dt.strftime('%Y-%m-%d %H:%M')}"
