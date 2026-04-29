import logging
from abc import ABC, abstractmethod
from typing import Any, AnyStr

import attr
from confluent_kafka import KafkaError, Message, Producer

from esgf_core_utils.settings.kafka.producer import ProducerSettings

# Setup logger
logger = logging.getLogger(__name__)


@attr.s
class BaseProducer(ABC):
    """
    Base Producer
    """

    @abstractmethod
    def produce(self, topic: str, key: AnyStr, value: AnyStr) -> Any:
        """Publish message

        Args:
            topic (str): topic to post message to
            key (AnyStr): message key
            value (AnyStr): message
        """


class DummyProducer(BaseProducer):
    """
    Dummy Producer
    """

    def produce(self, topic: str, key: AnyStr, value: AnyStr) -> None:
        logger.info("message: %s", repr(value))


class KafkaProducer(BaseProducer):
    """
    Kafka Producer
    """

    def __init__(self) -> None:
        self.settings = ProducerSettings()
        self.producer = Producer(
            self.settings.config.model_dump(by_alias=True, exclude_none=True)
        )
        logger.info("KafkaProducer initialised")

    def produce(
        self, topic: str, key: AnyStr, value: AnyStr
    ) -> list[tuple[KafkaError | None, Message]]:
        delivery_reports = []

        def delivery_report(err: KafkaError | None, msg: Message) -> None:
            if err is not None:
                logger.error("Delivery failed for message %s: %s", repr(msg.key()), err)
            else:
                logger.info(
                    "Message %s successfully delivered to %s [%s] at offset %s",
                    repr(msg.key()),
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                )
            delivery_reports.append((err, msg))

        self.producer.produce(
            topic=topic, key=key, value=value, callback=delivery_report
        )
        self.producer.flush()
        return delivery_reports

    def error(
        self, key: AnyStr, value: AnyStr
    ) -> list[tuple[KafkaError | None, Message]]:
        """Post an message to the error event stream

        Args:
            key (AnyStr): message key
            value (AnyStr): message

        Returns:
            list[tuple[KafkaError, Message]]: delivery reports
        """
        return self.produce(topic=self.settings.error_topic, key=key, value=value)

    def success(
        self, key: AnyStr, value: AnyStr
    ) -> list[tuple[KafkaError | None, Message]]:
        """Post an message to the success event stream

        Args:
            key (AnyStr): message key
            value (AnyStr): message

        Returns:
            list[tuple[KafkaError, Message]]: delivery reports
        """
        return self.produce(topic=self.settings.success_topic, key=key, value=value)
