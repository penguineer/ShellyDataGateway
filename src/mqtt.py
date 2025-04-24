import asyncio
import logging
import os
import time
from typing import TYPE_CHECKING, Callable, List

import paho.mqtt.client as paho_mqtt

# Pass-through for the Paho MQTT client
if TYPE_CHECKING:
    from paho.mqtt.client import Client as MqttClient, MQTTMessage as MqttMessage
else:
    MqttClient = paho_mqtt.Client
    MqttMessage = paho_mqtt.MQTTMessage


class MqttConfig:
    @staticmethod
    def from_env(env_prefix=""):
        host = os.getenv(env_prefix + "HOST")
        if host is None:
            raise KeyError("Missing HOST configuration for MQTT")

        prefix = os.getenv(env_prefix + "PREFIX")

        return MqttConfig(host, prefix)

    def __init__(self, host, prefix=None):
        self._host = host
        self._prefix = prefix

    @property
    def host(self):
        return self._host

    @property
    def prefix(self):
        return self._prefix

    def __str__(self):
        return f"MqttConfig(host={self._host}, prefix={self._prefix})"


MQTT_TOPICS = []


def append_topic(topic):
    MQTT_TOPICS.append(topic)


def add_topic_callback(mqttc, topic, cb):
    mqttc.subscribe(topic)
    MQTT_TOPICS.append(topic)

    mqttc.message_callback_add(topic, cb)


def on_connect(mqttc, _userdata, _flags, rc):
    logging.info("MQTT client connected with code %s", rc)

    for topic in MQTT_TOPICS:
        mqttc.subscribe(topic)


def on_disconnect(_mqttc, userdata, rc):
    logging.info("MQTT client disconnected with code %s", rc)
    if userdata and callable(userdata.get("on_disconnect_cb")):
        userdata["on_disconnect_cb"](rc)


def create_client(mqtt_config, on_disconnect_cb=None):
    client = MqttClient()
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.user_data_set({"on_disconnect_cb": on_disconnect_cb})  # Pass callback via userdata
    try:
        client.connect(mqtt_config.host, 1883, 60)
    except ConnectionRefusedError as e:
        logging.error(f"Connection refused for MQTT client: %s", e)
        return None

    client.loop_start()

    return client


def join_topics(prefix, *subtopics):
    """
    Joins an MQTT topic prefix with one or more subtopics, ensuring exactly one slash between each part.
    Preserves leading slashes in the topic prefix.

    Args:
        prefix (str): The MQTT topic prefix.
        *subtopics (str): One or more MQTT subtopics.

    Returns:
        str: The combined MQTT topic.
    """
    # Strip slashes from the ends of subtopics and join them with a single slash
    middle = "/".join(subtopic.strip("/") for subtopic in subtopics)
    # Combine prefix and middle, preserving leading slashes
    return f"{prefix.rstrip('/')}/{middle}" if middle else prefix


class MqttBurstCollector:
    """
    A class to collect MQTT messages in bursts and trigger a callback after a timeout.

    The `MqttBurstCollector` listens to specified MQTT topics and collects messages.
    It triggers a callback function with the collected messages when a timeout occurs
    after the last received message. The timeout resets each time a new message is received.

    Attributes:
        mqtt_client (MqttClient): The MQTT client used for subscribing to topics and receiving messages.
        topics (List[str]): A list of MQTT topics to listen to.
        callback (Callable[[List], None]): A function to handle the collected messages when the timeout is reached.
        timeout_s (float): The timeout duration in seconds after the last received message.
        _last_message_timestamp (float | None): The timestamp of the last received message.
        messages (List[MqttMessage]): A list to store the collected messages.
        _timeout_task (asyncio.Task): An asyncio task to monitor the timeout and trigger the callback.
    """

    def __init__(self,
                 mqtt_client: MqttClient,
                 topics: List[str],
                 callback: Callable[[List], None],
                 timeout_s: float = 1.0):
        """
        Initialize the burst collector and start listening to the specified topics.

        Args:
            mqtt_client (MqttClient): The MQTT client to use.
            topics (list): List of MQTT topics to listen to.
            callback (function): Callback function to handle a list of messages.
            timeout_s (int): Timeout in seconds after the latest message before a burst is delivered.
        """
        if callback is None:
            raise ValueError("Callback function must not be None")

        self.mqtt_client = mqtt_client
        self.topics = topics
        self.callback = callback
        self.timeout_s = timeout_s if timeout_s > 0 else 0

        self._last_message_timestamp = None
        self._timeout_task = None
        self._lock = asyncio.Lock()

        # To store collected messages
        self.messages = []

        # Subscribe to the topics and set up message callbacks
        for topic in self.topics:
            self.mqtt_client.subscribe(topic)
            self.mqtt_client.message_callback_add(topic, self._on_message)

    def stop(self) -> List[MqttMessage]:
        """
        Stop listening to the specified topics and return the retained messages.

        Returns:
            list: The list of retained messages.
        """
        self._cancel_timeout_task()

        for topic in self.topics:
            self.mqtt_client.message_callback_remove(topic)
            self.mqtt_client.unsubscribe(topic)

        retained_messages = self.messages[:]
        self._clear_messages()
        return retained_messages

    def _on_message(self, _client, _userdata, message: MqttMessage):
        """
        Internal method to handle incoming MQTT messages.

        Args:
            message: The received MQTT message.
        """
        asyncio.create_task(self._handle_message(message))

    async def _handle_message(self, message: MqttMessage):
        async with self._lock:
            self.messages.append(message)
            self._last_message_timestamp = time.time()

            # Start a timeout task if not already running
            self._start_timeout_task()

    def _clear_messages(self):
        """
        Clear the list of collected messages.
        This method is called when the timeout is reached and the callback is triggered.
        """
        self.messages.clear()
        self._last_message_timestamp = None

    def _start_timeout_task(self):
        """
        Start the timeout monitoring task if it is not already running.
        This method is called when the collector is started.
        """
        if not self._timeout_task or self._timeout_task.done():
            self._timeout_task = asyncio.create_task(self._monitor_timeout())

    def _cancel_timeout_task(self):
        """
        Cancel the timeout monitoring task if it is running.
        This method is called when the collector is stopped.
        """
        if self._timeout_task:
            self._timeout_task.cancel()
            self._timeout_task = None

    async def _monitor_timeout(self):
        try:
            while True:
                # Check periodically
                async with self._lock:
                    if self._last_message_timestamp is None:
                        break

                    # Calculate remaining time until timeout
                    elapsed_time = time.time() - self._last_message_timestamp
                    remaining_time = self.timeout_s - elapsed_time

                    # Check if the timeout has been reached
                    if remaining_time <= 0:
                        self._trigger_callback(self.messages)
                        self._clear_messages()
                        break

                # Wait for the remaining time
                await asyncio.sleep(min(remaining_time, 0.1))
        except asyncio.CancelledError:
            # Task was cancelled
            pass
        except Exception as e:
            logging.error(f"Unexpected error in _monitor_timeout: {e}")
        finally:
            self._timeout_task = None

    def _trigger_callback(self, messages):
        """
        Trigger the callback with the collected messages.

        Args:
            messages: The list of collected messages.
        """
        try:
            if self.callback:
                self.callback(messages[:])  # Trigger callback with a shallow copy
        except Exception as e:
            logging.error(f"Error in callback: {e}")
