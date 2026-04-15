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
    "KAFKA_CONSUMER_CONFIG__CLIENT_ID": "bar",
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
        mock_processor: MagicMock = MagicMock()
        consumer: KafkaConsumer = KafkaConsumer(mock_processor)

        mock_processor.assert_called_once()
        mock_consumer.assert_called_once()
        self.assertIs(consumer.consumer, mock_consumer)
        self.assertIs(consumer.message_processor, mock_processor)
        self.assertIsNotNone(consumer.settings)
