"""
Functional-style tests for KafkaConsumer using unittest.

These tests simulate behaviour of the start() loop, ensuring:
- subscription occurs
- ingest is called for valid messages
- commits occur
- exception handlers run appropriately
"""

import os
import unittest
from unittest.mock import MagicMock, patch

from confluent_kafka import KafkaException

from esgf_core_utils.models.kafka.consumer import KafkaConsumer

MOCK_ENV = {
    "KAFKA_CONSUMER_TOPICS": "local",
    "KAFKA_CONSUMER_CONFIG__BOOTSTRAP_SERVERS": "boots",
    "KAFKA_CONSUMER_CONFIG__SASL_USERNAME": "hello",
    "KAFKA_CONSUMER_CONFIG__SASL_PASSWORD": "world",
    "KAFKA_CONSUMER_CONFIG__GROUP_ID": "foo",
}  # nosec CWE-259


@patch.dict(os.environ, MOCK_ENV, clear=True)
class TestKafkaConsumerFunctional(unittest.TestCase):
    """Functional test suite for KafkaConsumer."""

    @patch("kafka_consumer.logging")
    @patch("kafka_consumer.consumer_settings")
    @patch("kafka_consumer.Consumer")
    def test_start_processes_messages(
        self,
        mock_consumer: MagicMock,
        mock_logging: MagicMock,
    ) -> None:
        """Simulate two poll cycles: one None message and one valid message."""
        consumer_instance: MagicMock = mock_consumer.return_value

        msg: MagicMock = MagicMock()
        consumer_instance.poll.side_effect = [None, msg, KeyboardInterrupt()]

        mock_processor_class: MagicMock = MagicMock()
        mock_processor_instance: MagicMock = mock_processor_class.return_value

        consumer: KafkaConsumer = KafkaConsumer(mock_processor_class)
        consumer.start()

        consumer_instance.subscribe.assert_called_once_with(["topicA"])
        mock_processor_instance.ingest.assert_called_once_with(msg)
        consumer_instance.commit.assert_called_once_with(
            message=msg,
            asynchronous=False,
        )
        consumer_instance.close.assert_called_once()

    @patch("kafka_consumer.logging")
    @patch("kafka_consumer.consumer_settings")
    @patch("kafka_consumer.Consumer")
    def test_start_handles_kafka_exception(
        self,
        mock_consumer: MagicMock,
        mock_logging: MagicMock,
    ) -> None:
        """Ensure KafkaException inside loop triggers error logging and proper shutdown."""
        consumer_instance: MagicMock = mock_consumer.return_value
        consumer_instance.poll.side_effect = KafkaException("boom")

        mock_processor_class: MagicMock = MagicMock()
        consumer: KafkaConsumer = KafkaConsumer(mock_processor_class)

        consumer.start()

        mock_logging.error.assert_called()
        consumer_instance.close.assert_called_once()

    @patch("kafka_consumer.logging")
    @patch("kafka_consumer.consumer_settings")
    @patch("kafka_consumer.Consumer")
    def test_start_keyboard_interrupt(
        self,
        mock_consumer: MagicMock,
        mock_logging: MagicMock,
    ) -> None:
        """Ensure KeyboardInterrupt is handled gracefully and closes the consumer."""
        consumer_instance: MagicMock = mock_consumer.return_value
        consumer_instance.poll.side_effect = KeyboardInterrupt()

        mock_processor_class: MagicMock = MagicMock()
        consumer: KafkaConsumer = KafkaConsumer(mock_processor_class)

        consumer.start()

        mock_logging.info.assert_called()
        consumer_instance.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
