"""
Functional-style tests for KafkaConsumer.

These tests validate the configuration path by:
- setting environment variables that ConsumerSettings reads
- constructing KafkaConsumer normally (no mocking ConsumerSettings)
- mocking only the external confluent_kafka.Consumer

They remain fast/deterministic and do not require a Kafka broker.
"""

from __future__ import annotations

import os
import unittest
from typing import Any, Dict
from unittest.mock import MagicMock, patch

from esgf_core_utils.models.kafka.consumer import KafkaConsumer

MOCK_ENV: Dict[str, str] = {
    "KAFKA_CONSUMER_TOPICS": "local",
    "KAFKA_CONSUMER_CONFIG__BOOTSTRAP_SERVERS": "boots",
    "KAFKA_CONSUMER_CONFIG__SASL_USERNAME": "hello",
    "KAFKA_CONSUMER_CONFIG__SASL_PASSWORD": "world",
    "KAFKA_CONSUMER_CONFIG__GROUP_ID": "foo",
}  # nosec (test credentials)


@patch.dict(os.environ, MOCK_ENV, clear=True)
class TestKafkaConsumerFunctional(unittest.TestCase):
    """Functional-style tests using real ConsumerSettings loaded from environment variables."""

    @patch("esgf_core_utils.models.kafka.consumer.Consumer")
    def test_constructor_initialises_consumer_from_env_settings(
        self,
        mock_consumer_cls: MagicMock,
    ) -> None:
        """
        Constructor should build the confluent_kafka.Consumer using settings derived from env vars.

        We assert only key config fields to keep the test resilient to future additions
        to the config structure.
        """
        # Arrange
        consumer_instance: MagicMock = MagicMock()
        mock_consumer_cls.return_value = consumer_instance
        processor: MagicMock = MagicMock()

        # Act
        kafka_consumer: KafkaConsumer = KafkaConsumer(processor)

        # Assert
        mock_consumer_cls.assert_called_once()
        passed_config: Dict[str, Any] = mock_consumer_cls.call_args.args[0]

        # Key expectations from env -> settings -> config dump
        self.assertEqual(passed_config["bootstrap.servers"], "boots")
        self.assertEqual(passed_config["group.id"], "foo")

        # Sanity checks: object wiring
        self.assertIs(kafka_consumer.consumer, consumer_instance)
        self.assertIs(kafka_consumer.message_processor, processor)
        self.assertIsNotNone(kafka_consumer.settings)

    @patch("esgf_core_utils.models.kafka.consumer.time.sleep")
    @patch("esgf_core_utils.models.kafka.consumer.Consumer")
    def test_start_processes_message_and_commits(
        self,
        mock_consumer_cls: MagicMock,
        mock_sleep: MagicMock,
    ) -> None:
        """
        start() should ingest and commit a polled message, then close on KeyboardInterrupt.

        We terminate the otherwise-infinite loop by raising KeyboardInterrupt after one message.
        """
        # Arrange
        consumer_instance: MagicMock = MagicMock()
        mock_consumer_cls.return_value = consumer_instance

        msg: MagicMock = MagicMock()
        consumer_instance.poll.side_effect = [msg, KeyboardInterrupt()]

        processor: MagicMock = MagicMock()
        kafka_consumer: KafkaConsumer = KafkaConsumer(processor)

        # Act
        kafka_consumer.start()

        # Assert
        consumer_instance.subscribe.assert_called_once_with(
            kafka_consumer.settings.topics
        )
        processor.ingest.assert_called_once_with(msg)
        consumer_instance.commit.assert_called_once_with(
            message=msg, asynchronous=False
        )
        consumer_instance.close.assert_called_once()
        mock_sleep.assert_not_called()

    @patch("esgf_core_utils.models.kafka.consumer.time.sleep")
    @patch("esgf_core_utils.models.kafka.consumer.Consumer")
    def test_start_handles_none_message(
        self,
        mock_consumer_cls: MagicMock,
        mock_sleep: MagicMock,
    ) -> None:
        """
        If poll() yields None, start() should sleep briefly and continue without ingest/commit,
        and still close the consumer when the loop terminates.
        """
        # Arrange
        consumer_instance: MagicMock = MagicMock()
        mock_consumer_cls.return_value = consumer_instance

        consumer_instance.poll.side_effect = [None, KeyboardInterrupt()]

        processor: MagicMock = MagicMock()
        kafka_consumer: KafkaConsumer = KafkaConsumer(processor)

        # Act
        kafka_consumer.start()

        # Assert
        processor.ingest.assert_not_called()
        consumer_instance.commit.assert_not_called()
        mock_sleep.assert_called_once_with(0.1)
        consumer_instance.close.assert_called_once()
