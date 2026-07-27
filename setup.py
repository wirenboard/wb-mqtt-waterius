#!/usr/bin/env python3

from setuptools import setup


def get_version():
    with open("debian/changelog", "r", encoding="utf-8") as f:
        return f.readline().split()[1][1:-1].split("~")[0]


setup(
    name="wb-mqtt-waterius",
    version=get_version(),
    maintainer="Wiren Board Team",
    maintainer_email="info@wirenboard.com",
    description="Send Wiren Board meter readings to the Waterius cloud",
    url="https://github.com/wirenboard/wb-mqtt-waterius",
    license="MIT",
    packages=[
        # "wb"                     # Explicitly excluded: provided by base package
        "wb.mqtt_waterius",
    ],
    scripts=["bin/wb-mqtt-waterius"],
)
