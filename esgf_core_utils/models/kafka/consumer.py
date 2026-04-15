import logging
import time

from confluent_kafka import Consumer, KafkaException

from esgf_core_utils.models.kafka.message_processor import MessageProcessor
from esgf_core_utils.settings.kafka.consumer import ConsumerSettings


class KafkaConsumer:
    """
    Kafka consumer
    """
    def __init__(self, message_processor: MessageProcessor):
        self.settings = ConsumerSettings()
        self.message_processor = message_processor
        self.consumer = Consumer(
            self.settings.config.model_dump(by_alias=True, exclude_none=True)
        )

    def start(self) -> None:
        """Start consuming messages"""
        self.consumer.subscribe(self.settings.topics)

        try:
            logging.info(
                "Kafka consumer started. Subscribed to topics: %s",
                self.settings.topics,
            )

            while True:
                message = self.consumer.poll(timeout=self.settings.timeout)
                logging.info(
                    "Kafka consuming message: %s",
                    message,
                )
                if message is None:
                    time.sleep(0.1)
                    continue

                self.message_processor.ingest(message)

                self.consumer.commit(message=message, asynchronous=False)

        except KeyboardInterrupt:
            logging.info("Kafka consumer interrupted. Exiting")

        except KafkaException as e:
            logging.error("Kafka exception: %s", e)

        finally:
            logging.info("Closing Kafka consumer")

            self.consumer.close()
