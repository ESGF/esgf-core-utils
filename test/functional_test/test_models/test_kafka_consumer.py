"""
Functional-style tests for KafkaConsumer using unittest.

These tests simulate the behaviour of the start() loop, ensuring:
- subscription happens
- ingest is called for valid messages
- commits occur
- control flow behaves as expected
- exception handlers behave correctly
"""

import unittest
from unittest.mock import MagicMock, patch

from confluent_kafka import KafkaException

from esgf_core_utils.models.kafka.consumer import KafkaConsumer


class TestKafkaConsumerFunctional(unittest.TestCase):
    """Functional test suite for KafkaConsumer."""

    @patch("kafka_consumer.logging")
    @patch("kafka_consumer.consumer_settings")
    @patch("kafka_consumer.Consumer")
    def test_start_processes_messages(self, mock_consumer, mock_settings, mock_logging):
        """Simulate two poll cycles: one None message, one valid message."""
        mock_settings.topics = ["topicA"]
        mock_settings.timeout = 3
        mock_settings.config.model_dump.return_value = {"bootstrap.servers": "localhost"}

        consumer_instance = mock_consumer.return_value

        # First poll returns None (skip)
        # Second poll returns a message (process)
        msg = MagicMock()
        consumer_instance.poll.side_effect = [None, msg, KeyboardInterrupt()]

        mock_processor_class = MagicMock()
        mock_processor_instance = mock_processor_class.return_value

        consumer = KafkaConsumer(mock_processor_class)

        consumer.start()

        consumer_instance.subscribe.assert_called_once_with(["topicA"])
        mock_processor_instance.ingest.assert_called_once_with(msg)
        consumer_instance.commit.assert_called_once_with(
            message=msg, asynchronous=False
        )
        consumer_instance.close.assert_called_once()

    @patch("kafka_consumer.logging")
    @patch("kafka_consumer.consumer_settings")
    @patch("kafka_consumer.Consumer")
    def test_start_handles_kafka_exception(self, mock_consumer, mock_settings, mock_logging):
        """Ensure KafkaException inside loop triggers error logging and closes properly."""
        mock_settings.topics = ["topicA"]
        mock_settings.timeout = 3
        mock_settings.config.model_dump.return_value = {"bootstrap.servers": "localhost"}

        consumer_instance = mock_consumer.return_value

        consumer_instance.poll.side_effect = KafkaException("boom")

        mock_processor_class = MagicMock()
        consumer = KafkaConsumer(mock_processor_class)

        consumer.start()

        mock_logging.error.assert_called()  # should log KafkaException
        consumer_instance.close.assert_called_once()

    @patch("kafka_consumer.logging")
    @patch("kafka_consumer.consumer_settings")
    @patch("kafka_consumer.Consumer")
    def test_start_keyboard_interrupt(self, mock_consumer, mock_settings, mock_logging):
        """Ensure KeyboardInterrupt in the loop triggers shutdown."""
        mock_settings.topics = ["topicA"]
        mock_settings.timeout = 2
        mock_settings.config.model_dump.return_value = {"bootstrap.servers": "localhost"}

        consumer_instance = mock_consumer.return_value
        consumer_instance.poll.side_effect = KeyboardInterrupt()

        mock_processor_class = MagicMock()
        consumer = KafkaConsumer(mock_processor_class)

        consumer.start()

        mock_logging.info.assert_called()  # Logs interruption message
        consumer_instance.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
    unittest.main()
