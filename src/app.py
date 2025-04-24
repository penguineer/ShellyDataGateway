#!/usr/bin/env python3

import signal
import sys
import os

import mqtt

import logging
import asyncio

running = True
mqtt_client: mqtt.MqttClient | None = None

def sigint_handler(_signal, _frame):
    global running

    if running:
        logging.info("SIGINT received. Stopping the queue.")
        running = False
    else:
        logging.info("Receiving SIGINT the second time. Exit.")
        sys.exit(0)


def mqtt_disconnect_handler(rc):
    if running:  # Only warn if the app is still running
        logging.warning("MQTT client disconnected unexpectedly with code %s", rc)


def get_log_level():
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    levels = {
        "CRITICAL": logging.CRITICAL,
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG,
        "NOTSET": logging.NOTSET,
    }
    return levels.get(log_level, logging.INFO)


def setup():
    global running, mqtt_client

    # Exit signal handler
    signal.signal(signal.SIGINT, sigint_handler)

    # Logging
    logging.basicConfig(
        level=get_log_level(),  # Set the logging level to INFO
        format='%(asctime)s - %(levelname)s - %(message)s',  # Define the log message format
        handlers=[logging.StreamHandler()]  # Add a handler to output logs to the console
    )

    # MQTT
    try:
        mqtt_config = mqtt.MqttConfig.from_env("MQTT_")
        logging.info("Running with MQTT config: %s", mqtt_config)

        mqtt_client = mqtt.create_client(mqtt_config, on_disconnect_cb=mqtt_disconnect_handler)
    except KeyError as e:
        logging.error("Error while setting up the MQTT client: %s", e)
        # This will lead to a fatal error in the loop
        mqtt_client = None


async def loop():
    global running, mqtt_client

    if mqtt_client is None:
        logging.fatal("MQTT client is not initialized.")
        return

    while running:
        # Sleep briefly to avoid busy-waiting
        await asyncio.sleep(0.5)


def teardown():
    global mqtt_client

    if mqtt_client and mqtt_client.is_connected():
        mqtt_client.disconnect()
        mqtt_client.loop_stop()


if __name__ == '__main__':
    setup()

    try:
        asyncio.run(loop())
    finally:
        teardown()
