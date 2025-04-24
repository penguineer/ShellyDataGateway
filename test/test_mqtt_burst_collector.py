import unittest
from unittest.mock import MagicMock
import asyncio
from mqtt import MqttBurstCollector, MqttMessage


class TestMqttBurstCollector(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.timeout_s = 0.2 # Must be at least 0.2

        self.mock_mqtt_client = MagicMock()
        self.mock_callback = MagicMock()

        self.collector = MqttBurstCollector(
            mqtt_client=self.mock_mqtt_client,
            topics=["test/topic"],
            callback=self.mock_callback,
            timeout_s=self.timeout_s,
        )

    def tearDown(self):
        self.collector.stop()

    async def test_subscribes_to_topics_on_init(self):
        # Verify topics are subscribed
        self.mock_mqtt_client.subscribe.assert_called_once_with("test/topic")
        self.mock_mqtt_client.message_callback_add.assert_called_once_with("test/topic", self.collector._on_message)

    async def test_message_collection_and_callback(self):
        # Simulate receiving a message
        message = MagicMock(spec=MqttMessage)
        await self.collector._handle_message(message)

        # Verify the message is collected
        self.assertEqual(len(self.collector.messages), 1)
        self.assertIn(message, self.collector.messages)

        # Simulate timeout
        await asyncio.sleep(self.timeout_s + 0.1)
        self.mock_callback.assert_called_once_with([message])
        self.assertEqual(len(self.collector.messages), 0)

    async def test_timeout_resets_on_new_message(self):
        # Simulate receiving the first message
        message1 = MagicMock(spec=MqttMessage)
        await self.collector._handle_message(message1)

        # Simulate receiving a second message before timeout
        message2 = MagicMock(spec=MqttMessage)
        await asyncio.sleep(self.timeout_s - 0.1)  # Less than the timeout
        await self.collector._handle_message(message2)

        # Simulate timeout
        await asyncio.sleep(self.timeout_s + 0.1)
        self.mock_callback.assert_called_once_with([message1, message2])

    async def test_no_callback_if_no_messages(self):
        # Simulate timeout without any messages
        await asyncio.sleep(self.timeout_s + 0.1)
        self.mock_callback.assert_not_called()

    async def test_stop_cancels_task_and_clears_messages(self):
        # Simulate receiving a message
        message = MagicMock(spec=MqttMessage)
        await self.collector._handle_message(message)

        # Stop the collector
        retained_messages = self.collector.stop()

        # Verify the task is canceled
        self.assertIsNone(self.collector._timeout_task)

        # Verify messages are cleared and returned
        self.assertEqual(retained_messages, [message])
        self.assertEqual(len(self.collector.messages), 0)

    async def test_monitor_timeout_handles_cancel(self):
        # Simulate task cancellation
        self.collector._start_timeout_task()
        self.collector._cancel_timeout_task()

        # Verify the task is canceled
        self.assertIsNone(self.collector._timeout_task)


if __name__ == "__main__":
    unittest.main()
