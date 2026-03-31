"""
Unit tests for KafkaConsumer using unittest and mocks.

These tests verify constructor behaviour, commit behaviour,
and correct usage of underlying Consumer methods.
"""

import os
import unittest
from unittest.mock import MagicMock, patch

from esgf_core_utils.models.kafka.consumer import KafkaConsumer

MOCK_ENV = {
    "KAFKA_CONSUMER_TOPICS": "local",
    "KAFKA_CONSUMER_CONFIG__BOOTSTRAP_SERVERS": "boots",
    "KAFKA_CONSUMER_CONFIG__SASL_USERNAME": "hello",
    "KAFKA_CONSUMER_CONFIG__SASL_PASSWORD": "world",
    "KAFKA_CONSUMER_CONFIG__GROUP_ID": "foo",
}  # nosec CWE-259


@patch.dict(os.environ, MOCK_ENV, clear=True)
class TestKafkaConsumerUnit(unittest.TestCase):
    """Unit tests for KafkaConsumer."""

    @patch("esgf_core_utils.models.kafka.consumer.Consumer")
    def test_constructor_initialises_consumer(
        self,
        mock_consumer: MagicMock,
    ) -> None:
        """Ensure KafkaConsumer initialises the confluent_kafka.Consumer correctly."""
        mock_processor_class: MagicMock = MagicMock()
        consumer: KafkaConsumer = KafkaConsumer(mock_processor_class)

        mock_processor_class.assert_called_once()
        mock_consumer.assert_called_once_with({"bootstrap.servers": "boots"})

        self.assertIsNotNone(consumer.consumer)
        self.assertIsNotNone(consumer.message_processor)

    @patch("esgf_core_utils.models.kafka.consumer.Consumer")
    def test_commit_calls_underlying_consumer(self, mock_consumer: MagicMock) -> None:
        """Verify that commit() passes messages to the underlying consumer when message exists."""
        consumer_instance: MagicMock = mock_consumer.return_value
        mock_processor_class: MagicMock = MagicMock()

        consumer: KafkaConsumer = KafkaConsumer(mock_processor_class)

        msg: MagicMock = MagicMock()
        consumer.commit(msg)

        consumer_instance.commit.assert_called_once_with(
            message=msg,
            asynchronous=False,
        )

    @patch("esgf_core_utils.models.kafka.consumer.Consumer")
    def test_commit_ignores_none_message(self, mock_consumer: MagicMock) -> None:
        """Verify commit() does nothing when message=None."""
        consumer_instance: MagicMock = mock_consumer.return_value
        mock_processor_class: MagicMock = MagicMock()

        consumer: KafkaConsumer = KafkaConsumer(mock_processor_class)

        consumer.commit(None)

        consumer_instance.commit.assert_not_called()


if __name__ == "__main__":
    unittest.main()
