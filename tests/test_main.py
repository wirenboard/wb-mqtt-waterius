from typing import Any, Optional

import pytest

from wb.mqtt_waterius import main


@pytest.fixture(name="dispatched")
def dispatched_fixture(monkeypatch: pytest.MonkeyPatch) -> dict:
    """
    Replace the three entry points with recorders and return what they were called with.

    Each one answers with its own exit code, none of which main() can produce by itself, so a
    test that sees the code knows it came through from the entry point.
    """
    calls: dict[str, dict] = {}

    def recorder(command: str, code: int) -> Any:
        def entry_point(config: Optional[str] = None, **kwargs: Any) -> int:
            calls[command] = {"config": config, **kwargs}
            return code

        return entry_point

    monkeypatch.setattr(main, "main_daemon", recorder("daemon", 4))
    monkeypatch.setattr(main, "main_send_once", recorder("send", 3))
    monkeypatch.setattr(main, "main_cleanup", recorder("cleanup", 5))
    return calls


@pytest.mark.parametrize(
    "argv, expected_command, expected_arguments, expected_code",
    [
        (["daemon"], "daemon", {"config": None}, 4),
        (["-c", "/tmp/w.conf", "daemon"], "daemon", {"config": "/tmp/w.conf"}, 4),
        (["send"], "send", {"config": None, "dry_run": False}, 3),
        (["-c", "/x", "send", "--dry-run"], "send", {"config": "/x", "dry_run": True}, 3),
        (["cleanup"], "cleanup", {"config": None}, 5),
    ],
    ids=["daemon", "daemon_with_config", "send", "send_dry_run", "cleanup"],
)
def test_argv_dispatch(
    dispatched: dict,
    argv: list[str],
    expected_command: str,
    expected_arguments: dict,
    expected_code: int,
) -> None:
    assert main.main(argv) == expected_code
    assert dispatched == {expected_command: expected_arguments}


def test_no_command_starts_the_daemon(dispatched: dict) -> None:
    assert main.main([]) == 4
    assert dispatched == {"daemon": {"config": None}}
