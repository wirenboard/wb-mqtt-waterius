"""Runs the doctests of every module in the package.

The package modules are not collected directly: pybuild invokes pytest with an explicit
`tests` path, which overrides `testpaths`, so `--doctest-modules` would never reach `wb/`.
Running doctest from inside a regular test works under every invocation.
"""

import doctest
import importlib
import pkgutil

import pytest

import wb.mqtt_waterius

MODULES = [name for _, name, _ in pkgutil.iter_modules(wb.mqtt_waterius.__path__, "wb.mqtt_waterius.")]


@pytest.mark.parametrize("module_name", MODULES)
def test_module_doctests(module_name: str) -> None:
    result = doctest.testmod(importlib.import_module(module_name), verbose=False)
    assert result.failed == 0, f"{result.failed} doctest(s) failed in {module_name}"
