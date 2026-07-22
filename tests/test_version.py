"""Version contract guard.

setup.py derives the package version from debian/changelog, which is the single
source of truth. This test checks the version is well-formed (MAJOR.MINOR.PATCH),
which the WB Debian packaging expects.
"""

from __future__ import annotations

import importlib.util
import re
import types
from pathlib import Path

import pytest
import setuptools

REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_setup(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    # setup.py calls setuptools.setup() at import and reads debian/changelog relative
    # to the cwd, so stub the call and run from the repo root.
    monkeypatch.chdir(REPO_ROOT)
    monkeypatch.setattr(setuptools, "setup", lambda **kwargs: None)
    spec = importlib.util.spec_from_file_location("_waterius_setup", REPO_ROOT / "setup.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_changelog_version_is_three_numbers(monkeypatch: pytest.MonkeyPatch) -> None:
    version = _load_setup(monkeypatch).get_version()
    assert re.match(r"^\d+\.\d+\.\d+$", version), f"expected MAJOR.MINOR.PATCH, got {version!r}"
