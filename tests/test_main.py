from typing import Optional

import pytest

from wb.mqtt_waterius import main


def test_no_command_prints_help(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    calls = {}

    def fake_daemon(config: Optional[str] = None) -> int:
        calls["daemon"] = config
        return 0

    monkeypatch.setattr(main, "main_daemon", fake_daemon)
    assert main.main([]) == 1
    assert "usage: wb-mqtt-waterius" in capsys.readouterr().out
    assert not calls


def test_daemon_subcommand(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {}

    def fake_daemon(config: Optional[str] = None) -> int:
        calls["daemon"] = config
        return 0

    monkeypatch.setattr(main, "main_daemon", fake_daemon)
    assert main.main(["daemon"]) == 0
    assert calls == {"daemon": None}


def test_config_flag_passes_through(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {}

    def fake_daemon(config: Optional[str] = None) -> int:
        calls["daemon"] = config
        return 0

    monkeypatch.setattr(main, "main_daemon", fake_daemon)
    assert main.main(["-c", "/tmp/w.conf", "daemon"]) == 0
    assert calls == {"daemon": "/tmp/w.conf"}


def test_send_subcommand(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {}

    def fake_send(config: Optional[str] = None, dry_run: bool = False) -> int:
        calls.update(config=config, dry_run=dry_run)
        return 3

    monkeypatch.setattr(main, "main_send_once", fake_send)
    assert main.main(["send"]) == 3
    assert calls == {"config": None, "dry_run": False}


def test_send_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {}

    def fake_send(config: Optional[str] = None, dry_run: bool = False) -> int:
        calls.update(config=config, dry_run=dry_run)
        return 0

    monkeypatch.setattr(main, "main_send_once", fake_send)
    assert main.main(["-c", "/x", "send", "--dry-run"]) == 0
    assert calls == {"config": "/x", "dry_run": True}


def test_cleanup_subcommand(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {}

    def fake_cleanup() -> int:
        calls["cleanup"] = True
        return 0

    monkeypatch.setattr(main, "main_cleanup", fake_cleanup)
    assert main.main(["cleanup"]) == 0
    assert calls == {"cleanup": True}
