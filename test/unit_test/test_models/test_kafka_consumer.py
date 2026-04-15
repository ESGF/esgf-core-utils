# test_kafka_consumer_unit.py
import unittest
from unittest.mock import MagicMock, patch

# Adjust this import to match your package layout
from esgf_core_utils.models.kafka.consumer import KafkaConsumer


class TestKafkaConsumerUnit(unittest.TestCase):
    """Unit tests for KafkaConsumer (isolated from settings/env)."""

    @patch("esgf_core_utils.models.kafka.consumer.Consumer")
    @patch("esgf_core_utils.models.kafka.consumer.ConsumerSettings")
    def test_constructor_initialises_consumer(self, mock_settings_cls, mock_consumer_cls):
        settings = MagicMock()
        settings.config.model_dump.return_value = {
            "bootstrap.servers": "boots",
            "group.id": "foo",
        }
        settings.topics = ["local"]
        settings.timeout = 1.0
        mock_settings_cls.return_value = settings

        consumer_instance = MagicMock()
        mock_consumer_cls.return_value = consumer_instance

        processor = MagicMock()

        kafka_consumer = KafkaConsumer(processor)

        mock_settings_cls.assert_called_once()
        settings.config.model_dump.assert_called_once_with(by_alias=True, exclude_none=True)
        mock_consumer_cls.assert_called_once_with({
            "bootstrap.servers": "boots",
            "group.id": "foo",
        })

        self.assertIs(kafka_consumer.settings, settings)
        self.assertIs(kafka_consumer.message_processor, processor)
        self.assertIs(kafka_consumer.consumer, consumer_instance)

    @patch("esgf_core_utils.models.kafka.consumer.time.sleep")
    @patch("esgf_core_utils.models.kafka.consumer.Consumer")
    @patch("esgf_core_utils.models.kafka.consumer.ConsumerSettings")
    def test_start_processes_single_message_and_commits_then_closes_on_keyboardinterrupt(
        self, mock_settings_cls, mock_consumer_cls, mock_sleep
    ):
        settings = MagicMock()
        settings.topics = ["t1"]
        settings.timeout = 0.5
        settings.config.model_dump.return_value = {"bootstrap.servers": "boots"}
        mock_settings_cls.return_value = settings

        consumer_instance = MagicMock()
        mock_consumer_cls.return_value = consumer_instance

        msg = MagicMock()
        consumer_instance.poll.side_effect = [msg, KeyboardInterrupt()]

        processor = MagicMock()

        kafka_consumer = KafkaConsumer(processor)

        kafka_consumer.start()

        consumer_instance.subscribe.assert_called_once_with(["t1"])
        processor.ingest.assert_called_once_with(msg)
        consumer_instance.commit.assert_called_once_with(message=msg, asynchronous=False)
        consumer_instance.close.assert_called_once()

        mock_sleep.assert_not_called()

    @patch("esgf_core_utils.models.kafka.consumer.time.sleep")
    @patch("esgf_core_utils.models.kafka.consumer.Consumer")
    @patch("esgf_core_utils.models.kafka.consumer.ConsumerSettings")
    def test_start_skips_none_messages_and_sleeps(self, mock_settings_cls, mock_consumer_cls, mock_sleep):
        settings = MagicMock()
        settings.topics = ["t1"]
        settings.timeout = 0.5
        settings.config.model_dump.return_value = {"bootstrap.servers": "boots"}
        mock_settings_cls.return_value = settings

        consumer_instance = MagicMock()
        mock_consumer_cls.return_value = consumer_instance

        consumer_instance.poll.side_effect = [None, KeyboardInterrupt()]

        processor = MagicMock()
        kafka_consumer = KafkaConsumer(processor)

        kafka_consumer.start()

        processor.ingest.assert_not_called()
        consumer_instance.commit.assert_not_called()
        mock_sleep.assert_called_once_with(0.1)
        consumer_instance.close.assert_called_once()

    @patch("esgf_core_utils.models.kafka.consumer.Consumer")
    @patch("esgf_core_utils.models.kafka.consumer.ConsumerSettings")
    def test_start_closes_consumer_on_kafkaexception(self, mock_settings_cls, mock_consumer_cls):
        settings = MagicMock()
        settings.topics = ["t1"]
        settings.timeout = 0.5
        settings.config.model_dump.return_value = {"bootstrap.servers": "boots"}
        mock_settings_cls.return_value = settings

        consumer_instance = MagicMock()
        mock_consumer_cls.return_value = consumer_instance

        class FakeKafkaException(Exception):
            pass

        with patch("esgf_core_utils.models.kafka.consumer.KafkaException", FakeKafkaException):
            consumer_instance.poll.side_effect = FakeKafkaException("boom")

            processor = MagicMock()
            kafka_consumer = KafkaConsumer(processor)

            kafka_consumer.start()

            consumer_instance.close.assert_called_once()
            processor.ingest.assert_not_called()
            consumer_instance.commit.assert_not_called()
