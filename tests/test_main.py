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
        (["daemon", "-c", "/tmp/w.conf"], "daemon", {"config": "/tmp/w.conf"}, 4),
        (["send"], "send", {"config": None, "dry_run": False}, 3),
        (["send", "-c", "/x", "--dry-run"], "send", {"config": "/x", "dry_run": True}, 3),
        (["cleanup"], "cleanup", {"config": None}, 5),
    ],
    ids=[
        "daemon",
        "daemon_config",
        "send",
        "send_config",
        "cleanup",
    ],
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


@pytest.mark.parametrize("argv", [[], ["-c", "/tmp/w.conf"], ["cleanup", "-c", "/tmp/w.conf"]])
def test_invalid_command_line_is_rejected(argv: list[str], dispatched: dict) -> None:
    with pytest.raises(SystemExit) as exc_info:
        main.main(argv)
    assert exc_info.value.code == 2
    assert dispatched == {}
