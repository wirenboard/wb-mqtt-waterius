# wb-mqtt-waterius

Send Wiren Board meter readings to the [Waterius](https://waterius.ru) cloud.

`wb-mqtt-waterius` reads impulse meter values published to MQTT by Wiren Board and
sends them on the scheduled days and time to the waterius.ru personal cabinet through
the universal HTTP API. Configuration is done in the web UI (Settings → Configuration
files) via a json-editor schema.

The implementation lands through a series of reviewable pull requests.
