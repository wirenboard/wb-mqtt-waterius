import datetime

from tests.conftest import ALL_DAYS
from wb.mqtt_waterius import schedule


def test_next_run_same_day_future() -> None:
    now = datetime.datetime(2026, 7, 16, 10, 0)  # Thursday
    assert schedule.next_run(12, 0, ALL_DAYS, now) == datetime.datetime(2026, 7, 16, 12, 0)


def test_next_run_skips_to_allowed_day() -> None:
    now = datetime.datetime(2026, 7, 16, 18, 0)  # Thursday, past 12:00
    assert schedule.next_run(12, 0, {4}, now) == datetime.datetime(2026, 7, 17, 12, 0)  # Friday


def test_next_run_wraps_to_next_week() -> None:
    now = datetime.datetime(2026, 7, 16, 18, 0)  # Thursday
    assert schedule.next_run(12, 0, {0}, now) == datetime.datetime(2026, 7, 20, 12, 0)  # Monday


def test_next_run_empty_days_returns_none() -> None:
    assert schedule.next_run(12, 0, set(), datetime.datetime(2026, 7, 16, 10, 0)) is None
