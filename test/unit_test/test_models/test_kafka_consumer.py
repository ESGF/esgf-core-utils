"""
Unit tests for KafkaConsumer using unittest and mocks.

These tests verify constructor behaviour, commit behaviour,
and correct usage of underlying Consumer methods.
"""

import unittest
from unittest.mock import MagicMock, patch

from esgf_core_utils.models.kafka.consumer import KafkaConsumer


class TestKafkaConsumerUnit(unittest.TestCase):
    """Unit tests for KafkaConsumer."""

    @patch("kafka_consumer.consumer_settings")
    @patch("kafka_consumer.Consumer")
    def test_constructor_initialises_consumer(self, mock_consumer, mock_settings):
        """Ensure KafkaConsumer initialises the confluent_kafka.Consumer correctly."""
        mock_settings.config.model_dump.return_value = {"bootstrap.servers": "localhost"}
        mock_settings.topics = ["topicA"]
        mock_settings.timeout = 5.0

        mock_processor_class = MagicMock()
        consumer = KafkaConsumer(mock_processor_class)

        mock_processor_class.assert_called_once()
        mock_consumer.assert_called_once_with({"bootstrap.servers": "localhost"})

        self.assertIsNotNone(consumer.consumer)
        self.assertIsNotNone(consumer.message_processor)

    @patch("kafka_consumer.Consumer")
    def test_commit_calls_underlying_consumer(self, mock_consumer):
        """Verify that commit() passes messages to the underlying consumer when message exists."""
        consumer_instance = mock_consumer.return_value
        mock_processor_class = MagicMock()
        consumer = KafkaConsumer(mock_processor_class)

        msg = MagicMock()
        consumer.commit(msg)

        consumer_instance.commit.assert_called_once_with(
            message=msg, asynchronous=False
        )

    @patch("kafka_consumer.Consumer")
    def test_commit_ignores_none_message(self, mock_consumer):
        """Verify commit() does nothing when message=None."""
        consumer_instance = mock_consumer.return_value
        mock_processor_class = MagicMock()
        consumer = KafkaConsumer(mock_processor_class)

        consumer.commit(None)

        consumer_instance.commit.assert_not_called()


if __name__ == "__main__":
    unittest.main()
