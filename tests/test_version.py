"""Version wiring guard.

setup.py derives the package version from debian/changelog, and that value is the
single source of truth (wb.mqtt_waterius.__version__ resolves to it via
importlib.metadata once the distribution is installed). These tests keep the
"no hardcoded version" contract intact.
"""

from __future__ import annotations

import importlib.util
import re
import types
from pathlib import Path

import pytest
import setuptools

REPO_ROOT = Path(__file__).resolve().parent.parent


def _changelog_version() -> str:
    first_line = (REPO_ROOT / "debian" / "changelog").read_text(encoding="utf-8").splitlines()[0]
    match = re.match(r"^wb-mqtt-waterius \(([^)]+)\)", first_line)
    assert match, f"unexpected changelog header: {first_line!r}"
    return match.group(1)


def _load_setup(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    # setup.py calls setuptools.setup() at import and reads debian/changelog relative
    # to the cwd, so stub the call and run from the repo root.
    monkeypatch.chdir(REPO_ROOT)
    monkeypatch.setattr(setuptools, "setup", lambda **kwargs: None)
    spec = importlib.util.spec_from_file_location("_waterius_setup", REPO_ROOT / "setup.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_setup_version_matches_changelog(monkeypatch: pytest.MonkeyPatch) -> None:
    setup_module = _load_setup(monkeypatch)
    assert setup_module.get_version() == _changelog_version()


def test_changelog_version_is_pep440ish() -> None:
    assert re.match(r"^\d+\.\d+\.\d+", _changelog_version())
