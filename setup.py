#!/usr/bin/env python3

from setuptools import find_namespace_packages, setup

from wb.mqtt_waterius.version import get_version_from_changelog

setup(
    name="wb-mqtt-waterius",
    version=get_version_from_changelog(),
    maintainer="Wiren Board Team",
    maintainer_email="info@wirenboard.com",
    description="Send Wiren Board meter readings to the Waterius cloud",
    url="https://github.com/wirenboard/wb-mqtt-waterius",
    license="MIT",
    # Matches every subpackage of wb. "wb" itself stays out, it comes from the base package.
    packages=find_namespace_packages(include=["wb.*"]),
    scripts=["bin/wb-mqtt-waterius"],
)
