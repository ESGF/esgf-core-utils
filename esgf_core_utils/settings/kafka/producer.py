from pydantic import ConfigDict
from pydantic_settings import BaseSettings

from esgf_core_utils.models.kafka.config import KafkaConfig


class ProducerSettings(BaseSettings):
    """
    Event Stream Settings
    """

    model_config = ConfigDict(
        validate_by_name=True,
        env_prefix="KAFKA_PRODUCER_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    config: KafkaConfig
    error_topic: str
    success_topic: str
