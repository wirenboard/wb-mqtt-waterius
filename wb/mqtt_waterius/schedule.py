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
