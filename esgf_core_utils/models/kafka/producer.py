import logging
from abc import ABC, abstractmethod

import attr
from confluent_kafka import KafkaError, Message, Producer

from esgf_core_utils.settings.kafka import producer_settings

# Setup logger
logger = logging.getLogger(__name__)


@attr.s
class BaseProducer(ABC):
    @abstractmethod
    def produce(self, topic: str, key: str | bytes, value: str | bytes):
        """Publish message

        Args:
            topic (str): topic to post message to
            key (str | bytes): message key
            value (str | bytes): message
        """


class StdoutProducer(BaseProducer):
    def produce(self, topic: str, key: str | bytes, value: str | bytes):
        logger.info(f"message: {value}")


class KafkaProducer(BaseProducer):
    def __init__(self):
        self.producer = Producer(
            producer_settings.config.model_dump(by_alias=True, exclude_none=True)
        )
        logger.info("KafkaProducer initialized")

    def produce(
        self, topic: str, key: str | bytes, value: str | bytes
    ) -> list[tuple[KafkaError, Message]]:
        delivery_reports = []

        def delivery_report(err: KafkaError, msg: Message) -> None:
            if err is not None:
                logger.error(f"Delivery failed for message {msg.key()}: {err}")
            else:
                logger.info(
                    f"Message {msg.key()} successfully delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
                )
            delivery_reports.append((err, msg))

        self.producer.produce(
            topic=topic, key=key, value=value, callback=delivery_report
        )
        self.producer.flush()
        return delivery_reports

    def error(
        self, key: str | bytes, value: str | bytes
    ) -> list[tuple[KafkaError, Message]]:
        """Post an message to the error event stream

        Args:
            key (str | bytes): message key
            value (str | bytes): message

        Returns:
            list[tuple[KafkaError, Message]]: delivery reports
        """
        self.produce(topic=producer_settings.error_topic, key=key, value=value)

    def success(
        self, key: str | bytes, value: str | bytes
    ) -> list[tuple[KafkaError, Message]]:
        """Post an message to the success event stream

        Args:
            key (str | bytes): message key
            value (str | bytes): message

        Returns:
            list[tuple[KafkaError, Message]]: delivery reports
        """
        self.produce(topic=producer_settings.success_topic, key=key, value=value)
