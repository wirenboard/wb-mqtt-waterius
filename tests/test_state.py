from pathlib import Path
from typing import Optional

import pytest

from wb.mqtt_waterius import state


@pytest.fixture(autouse=True)
def state_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Point persistence at a tmp state file.
    """
    monkeypatch.setattr(state, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(state, "STATE_FILE", str(tmp_path / "state.json"))


def test_state_roundtrip() -> None:
    mark = {"date": "2026-07-16", "stamp": "Thursday 2026-07-16 12:00"}
    state.save_state({"enabled": False, "schedule_time": "12:00", "last_sent": {"a1b2c3d4e5f6": mark}})
    loaded = state.load_state()
    assert loaded["enabled"] is False
    assert loaded["schedule_time"] == "12:00"
    assert loaded["last_sent"] == {"a1b2c3d4e5f6": mark}


@pytest.mark.parametrize(
    "content",
    [
        pytest.param(None, id="missing-file"),
        pytest.param("{not json", id="corrupt-json"),
        pytest.param("[]", id="valid-json-not-dict"),
    ],
)
def test_load_state_defaults_on_bad_input(content: Optional[str], tmp_path: Path) -> None:
    # No file, unparseable JSON, or JSON that parses but isn't an object (a bare list would
    # break the .get() calls) all fall back to safe defaults instead of crashing the daemon.
    if content is not None:
        (tmp_path / "state.json").write_text(content)
    loaded = state.load_state()
    assert loaded["enabled"] is True
    assert loaded["schedule_time"] is None
    assert not loaded["last_sent"]


def test_save_state_swallows_oserror(monkeypatch: pytest.MonkeyPatch) -> None:
    # A read-only/full /var/lib must not crash the daemon on every state write.
    monkeypatch.setattr(state.os, "makedirs", lambda *args, **kwargs: (_ for _ in ()).throw(OSError("x")))
    state.save_state({"enabled": True})  # should log and return, not raise


def test_save_state_atomic_leaves_valid_file_no_tmp(tmp_path: Path) -> None:
    state.save_state({"enabled": False, "schedule_time": "12:00", "last_sent": {}})
    assert state.load_state()["enabled"] is False  # complete, valid file
    assert not (tmp_path / "state.json.tmp").exists()  # temp cleaned up by os.replace
