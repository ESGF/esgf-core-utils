"""
Test that KafkaProducer converts message-too-large errors into a 413 exception
"""

import unittest
from unittest.mock import MagicMock, patch

from confluent_kafka import KafkaError, KafkaException

from esgf_core_utils.models.exceptions import PayloadTooLargeException
from esgf_core_utils.models.kafka.producer import DummyProducer, KafkaProducer

ENV = {
    "KAFKA_PRODUCER_SUCCESS_TOPIC": "success",
    "KAFKA_PRODUCER_CONFIG__BOOTSTRAP_SERVERS": "boots",
    "KAFKA_PRODUCER_CONFIG__SASL_USERNAME": "hello",
    "KAFKA_PRODUCER_CONFIG__SASL_PASSWORD": "world",  # nosec CWE-259
}


class TestKafkaProducerTooLarge(unittest.TestCase):
    """Test KafkaProducer.produce handling of oversized messages"""

    def _producer(self, mock_cls: MagicMock) -> tuple[KafkaProducer, MagicMock]:
        with patch.dict("os.environ", ENV):
            return KafkaProducer(), mock_cls.return_value

    @patch("esgf_core_utils.models.kafka.producer.Producer")
    def test_local_size_error(self, mock_cls: MagicMock) -> None:
        """Client-side MSG_SIZE_TOO_LARGE should raise a 413 exception"""
        producer, inner = self._producer(mock_cls)
        inner.produce.side_effect = KafkaException(
            KafkaError(KafkaError.MSG_SIZE_TOO_LARGE)
        )
        with self.assertRaises(PayloadTooLargeException) as ctx:
            producer.produce("topic", "key", "value")
        self.assertEqual(ctx.exception.status_code, 413)

    @patch("esgf_core_utils.models.kafka.producer.Producer")
    def test_broker_size_error(self, mock_cls: MagicMock) -> None:
        """Broker-side MSG_SIZE_TOO_LARGE in the delivery report should raise 413"""
        producer, inner = self._producer(mock_cls)

        def fake_produce(**kwargs: object) -> None:
            kwargs["callback"](KafkaError(KafkaError.MSG_SIZE_TOO_LARGE), MagicMock())  # type: ignore[operator]

        inner.produce.side_effect = fake_produce
        with self.assertRaises(PayloadTooLargeException):
            producer.produce("topic", "key", "value")

    @patch("esgf_core_utils.models.kafka.producer.Producer")
    def test_other_kafka_exception_reraised(self, mock_cls: MagicMock) -> None:
        """Other Kafka exceptions should not be converted"""
        producer, inner = self._producer(mock_cls)
        inner.produce.side_effect = KafkaException(KafkaError(KafkaError._TRANSPORT))
        with self.assertRaises(KafkaException):
            producer.produce("topic", "key", "value")


class TestKafkaProducerProduce(unittest.TestCase):
    """Test the normal (non-error) behaviour of the producers"""

    @patch("esgf_core_utils.models.kafka.producer.Producer")
    def test_produce_success(self, mock_cls: MagicMock) -> None:
        """Successful delivery reports should be returned"""
        with patch.dict("os.environ", ENV):
            producer = KafkaProducer()
        inner = mock_cls.return_value
        msg = MagicMock()
        inner.produce.side_effect = lambda **kwargs: kwargs["callback"](None, msg)

        reports = producer.produce("topic", "key", "value")

        self.assertEqual(reports, [(None, msg)])
        inner.flush.assert_called_once()

    @patch("esgf_core_utils.models.kafka.producer.Producer")
    def test_success_and_error_topics(self, mock_cls: MagicMock) -> None:
        """success() and error() should publish to the configured topics"""
        with patch.dict("os.environ", {**ENV, "KAFKA_PRODUCER_ERROR_TOPIC": "error"}):
            producer = KafkaProducer()
        inner = mock_cls.return_value

        producer.success("key", "value")
        producer.error("key", "value")

        topics = [call.kwargs["topic"] for call in inner.produce.call_args_list]
        self.assertEqual(topics, ["success", "error"])

    @patch("esgf_core_utils.models.kafka.producer.Producer")
    def test_other_delivery_error_returned(self, mock_cls: MagicMock) -> None:
        """Delivery errors other than message size should be returned, not raised"""
        with patch.dict("os.environ", ENV):
            producer = KafkaProducer()
        err = KafkaError(KafkaError._TRANSPORT)
        msg = MagicMock()
        mock_cls.return_value.produce.side_effect = lambda **kwargs: kwargs["callback"](
            err, msg
        )

        self.assertEqual(producer.produce("topic", "key", "value"), [(err, msg)])

    def test_dummy_producer(self) -> None:
        """DummyProducer should just log the message"""
        with self.assertLogs(
            "esgf_core_utils.models.kafka.producer", level="INFO"
        ) as logs:
            DummyProducer().produce("topic", "key", "value")
        self.assertIn("value", logs.output[0])
