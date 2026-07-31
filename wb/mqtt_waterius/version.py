"""
Single place that knows where the package version comes from.

At runtime the version is read from the installed package metadata. That metadata is filled by
setup.py at build time, which is the only moment when debian/changelog is available.
"""

from importlib.metadata import version

# Relative to the source root, which is the working directory setup.py is always run from.
CHANGELOG_FILEPATH = "debian/changelog"


def get_version() -> str:
    """
    Version of the installed package. Use this at runtime.
    """
    return version(__package__)


def parse_changelog_version(changelog_line: str) -> str:
    """
    Pull the version out of the first line of debian/changelog.

    Everything after ~ is the suffix CI adds on dev branches, and PEP 440 allows no ~ in a
    version, so it is dropped.

    Examples:
        >>> parse_changelog_version("wb-foo (1.0.0) stable; urgency=medium")
        '1.0.0'
        >>> parse_changelog_version("wb-foo (10.20.300) stable; urgency=low")
        '10.20.300'
        >>> parse_changelog_version("wb-foo (1.0.0~exp~branch~2~gdeadbee) stable; urgency=medium")
        '1.0.0'
    """
    return changelog_line.split()[1][1:-1].split("~")[0]


def get_version_from_changelog() -> str:
    """
    Version for the packaging metadata. Build time only, called by setup.py.
    """
    with open(CHANGELOG_FILEPATH, "r", encoding="utf-8") as f:
        return parse_changelog_version(f.readline())
