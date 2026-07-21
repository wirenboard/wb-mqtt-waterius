from setuptools import find_namespace_packages, setup


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
    packages=find_namespace_packages(include=["wb.*"]),
    scripts=["bin/wb-mqtt-waterius"],
    license="MIT",
)
