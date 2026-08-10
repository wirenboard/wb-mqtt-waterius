import re

from wb.mqtt_waterius.version import get_version

# WB Debian packaging expects exactly MAJOR.MINOR.PATCH. Neither dpkg nor PEP 440 enforces it,
# both accept 1.0 and 1.2.3.4, so this test is the only thing that does.
RELEASE_VERSION_PATTERN = re.compile(r"\d+\.\d+\.\d+")


def _is_release_version(package_version: str) -> bool:
    return RELEASE_VERSION_PATTERN.fullmatch(package_version) is not None


def test_installed_version_is_release_version() -> None:
    package_version = get_version()
    assert _is_release_version(
        package_version
    ), f"package version '{package_version}' is not MAJOR.MINOR.PATCH, fix debian/changelog"


def test_four_part_version_is_rejected() -> None:
    assert not _is_release_version("1.0.0.1")
