import logging

from confluent_kafka import Consumer, KafkaException
from stac_fastapi.extensions.core.transaction import BaseTransactionsClient

from esgf_core_utils.settings.kafka import consumer_settings


class KafkaConsumer:
    def __init__(self, message_processor: BaseTransactionsClient):
        self.message_processor = message_processor()
        self.consumer = Consumer(
            consumer_settings.config.model_dump(by_alias=True, exclude_none=True)
        )

    def commit(self, message):
        if message:
            self.consumer.commit(message=message, asynchronous=False)

    def start(self):
        self.consumer.subscribe(consumer_settings.topics)

        try:
            logging.info(
                "Kafka consumer started. Subscribed to topics: %s",
                consumer_settings.topics,
            )

            while True:
                message = self.consumer.poll(timeout=consumer_settings.timeout)
                logging.info(
                    "Kafka consuming message: %s",
                    message,
                )
                if message is None:
                    continue

                if message.error():
                    logging.error(
                        "Message error at offset %s: %s.",
                        message.offset(),
                        message.error(),
                    )
                    logging.error(
                        "Message data %s.",
                        message,
                    )
                self.message_processor.ingest(message)

                self.consumer.commit(message=message, asynchronous=False)

        except KeyboardInterrupt:
            logging.info("Kafka consumer interrupted. Exiting")

        except KafkaException as e:
            logging.error("Kafka exception: %s", e)

        finally:
            logging.info("Closing Kafka consumer")

            self.consumer.close()
