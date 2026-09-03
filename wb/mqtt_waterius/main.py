"""
Command line entry point for wb-mqtt-waterius.
"""

import argparse
import logging
import sys
from collections.abc import Sequence
from importlib.metadata import PackageNotFoundError
from typing import Any, Optional

from wb.mqtt_waterius.service import (
    CLIENT_ID,
    EXIT_FAILURE,
    main_cleanup,
    main_daemon,
    main_send_once,
)
from wb.mqtt_waterius.version import get_version


def _setup_logging() -> None:
    """
    Configure logging for the process, once, before any command runs.

    basicConfig sets up the root logger and every module logger propagates to it, so this
    one call covers the whole package. The name in the format keeps our lines recognizable
    in an aggregated log, not only under journalctl -u.
    """
    logging.basicConfig(level=logging.INFO, format=f"%(asctime)s {CLIENT_ID} %(levelname)s %(message)s")


class _PrintVersionAction(argparse.Action):
    """
    Reads the version only when the flag is actually used.
    """

    def __init__(self, option_strings: Sequence[str], dest: str, **kwargs: Any) -> None:
        super().__init__(option_strings, dest, nargs=0, **kwargs)

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Any,
        option_string: Optional[str] = None,
    ) -> None:
        try:
            print(get_version())
        except PackageNotFoundError:
            parser.exit(EXIT_FAILURE, "Package metadata not found, install the package to use --version\n")
        parser.exit()


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="wb-mqtt-waterius", description="Send WB meter readings to Waterius"
    )
    parser.add_argument("--version", action=_PrintVersionAction, help="show package version and exit")
    parser.add_argument("-c", "--config", help="path to config file", default=None)
    subparsers = parser.add_subparsers(dest="command")
    daemon_parser = subparsers.add_parser("daemon", help="run the service")
    daemon_parser.add_argument("-c", "--config", help="path to config file", default=argparse.SUPPRESS)
    send_parser = subparsers.add_parser("send", help="send readings once and exit")
    send_parser.add_argument("-c", "--config", help="path to config file", default=argparse.SUPPRESS)
    send_parser.add_argument(
        "--dry-run", action="store_true", help="build and print payloads without sending"
    )
    subparsers.add_parser("cleanup", help="remove all Waterius devices from MQTT")

    _setup_logging()
    args = parser.parse_args(argv)
    if args.command in (None, "daemon"):
        return main_daemon(args.config)
    if args.command == "send":
        return main_send_once(args.config, dry_run=args.dry_run)
    if args.command == "cleanup":
        return main_cleanup()
    return EXIT_FAILURE


if __name__ == "__main__":
    sys.exit(main())
