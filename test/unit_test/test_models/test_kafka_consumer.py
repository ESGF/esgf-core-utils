"""
Unit tests for KafkaConsumer.

These tests isolate KafkaConsumer from its configuration source by mocking
ConsumerSettings and the underlying confluent_kafka.Consumer. This allows us to
verify:
- Constructor wiring (settings -> config dump -> Consumer instantiated)
- start() control flow: subscribe, poll loop, ingest, commit, and close
- Handling of None messages (sleep + continue)
- Graceful exit on KeyboardInterrupt and KafkaException

All tests are deterministic and do not require a running Kafka broker.
"""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from esgf_core_utils.models.kafka.consumer import KafkaConsumer


class TestKafkaConsumerUnit(unittest.TestCase):
    """Unit tests for KafkaConsumer (dependencies isolated via mocks)."""

    @patch("esgf_core_utils.models.kafka.consumer.Consumer")
    @patch("esgf_core_utils.models.kafka.consumer.ConsumerSettings")
    def test_constructor_initialises_consumer(
        self,
        mock_settings_cls: MagicMock,
        mock_consumer_cls: MagicMock,
    ) -> None:
        """
        The constructor should:
        - instantiate ConsumerSettings
        - call settings.config.model_dump(by_alias=True, exclude_none=True)
        - construct confluent_kafka.Consumer with that dict
        - retain the provided message_processor
        """
        # Arrange: settings.config.model_dump returns deterministic dict
        settings: MagicMock = MagicMock()
        settings.config.model_dump.return_value = {
            "bootstrap.servers": "boots",
            "group.id": "foo",
        }
        settings.topics = ["local"]
        settings.timeout = 1.0
        mock_settings_cls.return_value = settings

        consumer_instance: MagicMock = MagicMock()
        mock_consumer_cls.return_value = consumer_instance

        processor: MagicMock = MagicMock()

        # Act
        kafka_consumer: KafkaConsumer = KafkaConsumer(processor)

        # Assert
        mock_settings_cls.assert_called_once()
        settings.config.model_dump.assert_called_once_with(
            by_alias=True, exclude_none=True
        )
        mock_consumer_cls.assert_called_once_with(
            {"bootstrap.servers": "boots", "group.id": "foo"}
        )

        self.assertIs(kafka_consumer.settings, settings)
        self.assertIs(kafka_consumer.message_processor, processor)
        self.assertIs(kafka_consumer.consumer, consumer_instance)

    @patch("esgf_core_utils.models.kafka.consumer.time.sleep")
    @patch("esgf_core_utils.models.kafka.consumer.Consumer")
    @patch("esgf_core_utils.models.kafka.consumer.ConsumerSettings")
    def test_start_processes_single_message_and_commits_then_closes_on_keyboardinterrupt(
        self,
        mock_settings_cls: MagicMock,
        mock_consumer_cls: MagicMock,
        mock_sleep: MagicMock,
    ) -> None:
        """
        start() should:
        - subscribe to settings.topics
        - poll messages in a loop
        - ingest each non-None message
        - commit synchronously for each ingested message
        - close the consumer when interrupted (KeyboardInterrupt)
        """
        # Arrange
        settings: MagicMock = MagicMock()
        settings.topics = ["t1"]
        settings.timeout = 0.5
        settings.config.model_dump.return_value = {"bootstrap.servers": "boots"}
        mock_settings_cls.return_value = settings

        consumer_instance: MagicMock = MagicMock()
        mock_consumer_cls.return_value = consumer_instance

        msg: MagicMock = MagicMock()
        consumer_instance.poll.side_effect = [msg, KeyboardInterrupt()]

        processor: MagicMock = MagicMock()

        kafka_consumer: KafkaConsumer = KafkaConsumer(processor)

        # Act
        kafka_consumer.start()

        # Assert
        consumer_instance.subscribe.assert_called_once_with(["t1"])
        processor.ingest.assert_called_once_with(msg)
        consumer_instance.commit.assert_called_once_with(
            message=msg, asynchronous=False
        )
        consumer_instance.close.assert_called_once()

        mock_sleep.assert_not_called()

    @patch("esgf_core_utils.models.kafka.consumer.time.sleep")
    @patch("esgf_core_utils.models.kafka.consumer.Consumer")
    @patch("esgf_core_utils.models.kafka.consumer.ConsumerSettings")
    def test_start_skips_none_messages_and_sleeps(
        self,
        mock_settings_cls: MagicMock,
        mock_consumer_cls: MagicMock,
        mock_sleep: MagicMock,
    ) -> None:
        """
        When poll() returns None, start() should:
        - sleep briefly (0.1s) and continue
        - not call ingest() or commit()
        - still close the consumer when the loop ends (KeyboardInterrupt here)
        """
        # Arrange
        settings: MagicMock = MagicMock()
        settings.topics = ["t1"]
        settings.timeout = 0.5
        settings.config.model_dump.return_value = {"bootstrap.servers": "boots"}
        mock_settings_cls.return_value = settings

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

    @patch("esgf_core_utils.models.kafka.consumer.Consumer")
    @patch("esgf_core_utils.models.kafka.consumer.ConsumerSettings")
    def test_start_closes_consumer_on_kafkaexception(
        self,
        mock_settings_cls: MagicMock,
        mock_consumer_cls: MagicMock,
    ) -> None:
        """
        If poll() raises KafkaException, start() should:
        - handle the exception
        - not ingest or commit any messages
        - always close the consumer in the finally block
        """
        # Arrange
        settings: MagicMock = MagicMock()
        settings.topics = ["t1"]
        settings.timeout = 0.5
        settings.config.model_dump.return_value = {"bootstrap.servers": "boots"}
        mock_settings_cls.return_value = settings

        consumer_instance: MagicMock = MagicMock()
        mock_consumer_cls.return_value = consumer_instance

        class FakeKafkaException(Exception):
            """Local stand-in for confluent_kafka.KafkaException in this unit test."""

        # Patch the KafkaException symbol in the module under test so we can raise it
        with patch(
            "esgf_core_utils.models.kafka.consumer.KafkaException", FakeKafkaException
        ):
            consumer_instance.poll.side_effect = FakeKafkaException("boom")

            processor: MagicMock = MagicMock()
            kafka_consumer: KafkaConsumer = KafkaConsumer(processor)

            # Act
            kafka_consumer.start()

            # Assert
            consumer_instance.close.assert_called_once()
            processor.ingest.assert_not_called()
            consumer_instance.commit.assert_not_called()
