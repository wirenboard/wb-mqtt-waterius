"""wb-mqtt-waterius — send Wiren Board meter readings to the waterius.ru cloud."""

try:
    from importlib.metadata import PackageNotFoundError
    from importlib.metadata import version as _version

    # Single source of truth: the installed distribution's version, which setup.py
    # derives from debian/changelog. No hardcoded duplicate to drift out of sync.
    __version__ = _version("wb-mqtt-waterius")
except (ImportError, PackageNotFoundError):
    # Not installed as a distribution (e.g. running from a source checkout in tests).
    __version__ = "0.0.0"
