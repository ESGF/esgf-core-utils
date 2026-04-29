import abc

from confluent_kafka import Message as KafkaMessage


class MessageProcessor(metaclass=abc.ABCMeta):
    """
    Kafka Message Processor
    """

    @abc.abstractmethod
    def ingest(self, message: KafkaMessage) -> None:
        """Process kafka message

        Args:
            message (KafkaMessage): Kafka message
        """
