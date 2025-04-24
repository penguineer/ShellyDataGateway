# Shelly Data Gateway

> A lightweight gateway to collect, retain, and forward data
> from [Shelly](https://www.shelly.com/) IoT devices.

Shelly Data Gateway enables efficient data collection from Shelly
devices when using MQTT instead of the cloud. It retains data sent
spontaneously by devices and organizes it
into [Cloud Events](https://cloudevents.io/), which can be forwarded to
various sinks (currently, MQTT is supported).

- Collects and retains data from Shelly IoT devices.
- Converts data into Cloud Events format.
- Supports forwarding to MQTT sinks.

## Contributions

Contributions are welcome!
Please submit your changes via a pull request (PR)

## License

This project is licensed under the [MIT License](LICENSE.txt).

© 2021-2025 [Stefan Haun](https://github.com/penguineer) and contributors.
