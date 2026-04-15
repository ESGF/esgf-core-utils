# test_kafka_consumer_functional.py
import os
from unittest.mock import MagicMock, patch

# Adjust this import to match your package layout
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
    """Functional-style tests for KafkaConsumer using real ConsumerSettings via env vars."""

    @patch("esgf_core_utils.models.kafka.consumer.Consumer")
    def test_constructor_initialises_consumer_from_env_settings(self, mock_consumer_cls):
        # Arrange
        consumer_instance = MagicMock()
        mock_consumer_cls.return_value = consumer_instance
        processor = MagicMock()

        # Act
        kafka_consumer = KafkaConsumer(processor)

        # Assert
        mock_consumer_cls.assert_called_once()
        passed_config = mock_consumer_cls.call_args.args[0]

        # Check key fields we expect from env → settings → config dump
        self.assertEqual(passed_config["bootstrap.servers"], "boots")
        self.assertEqual(passed_config["group.id"], "foo")

        # Sanity checks: object wiring
        self.assertIs(kafka_consumer.consumer, consumer_instance)
        self.assertIs(kafka_consumer.message_processor, processor)
        self.assertIsNotNone(kafka_consumer.settings)

    @patch("esgf_core_utils.models.kafka.consumer.time.sleep")
    @patch("esgf_core_utils.models.kafka.consumer.Consumer")
    def test_start_processes_message_and_commits(self, mock_consumer_cls, mock_sleep):
        # Arrange
        consumer_instance = MagicMock()
        mock_consumer_cls.return_value = consumer_instance

        msg = MagicMock()
        # one message then stop loop
        consumer_instance.poll.side_effect = [msg, KeyboardInterrupt()]

        processor = MagicMock()
        kafka_consumer = KafkaConsumer(processor)

        # Act
        kafka_consumer.start()

        # Assert
        consumer_instance.subscribe.assert_called_once_with(kafka_consumer.settings.topics)
        processor.ingest.assert_called_once_with(msg)
        consumer_instance.commit.assert_called_once_with(message=msg, asynchronous=False)
        consumer_instance.close.assert_called_once()
        mock_sleep.assert_not_called()

    @patch("esgf_core_utils.models.kafka.consumer.time.sleep")
    @patch("esgf_core_utils.models.kafka.consumer.Consumer")
    def test_start_handles_none_message(self, mock_consumer_cls, mock_sleep):
        # Arrange
        consumer_instance = MagicMock()
        mock_consumer_cls.return_value = consumer_instance

        consumer_instance.poll.side_effect = [None, KeyboardInterrupt()]

        processor = MagicMock()
        kafka_consumer = KafkaConsumer(processor)

        # Act
        kafka_consumer.start()

        # Assert
        processor.ingest.assert_not_called()
        consumer_instance.commit.assert_not_called()
        mock_sleep.assert_called_once_with(0.1)
        consumer_instance.close.assert_called_once()
        consumer_instance.close.assert_called_once()
