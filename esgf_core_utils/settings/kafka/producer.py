from pydantic_settings import BaseSettings, SettingsConfigDict

from esgf_core_utils.models.kafka.config import KafkaProducerConfig


class ProducerSettings(BaseSettings):
    """
    Event Stream Settings
    """

    model_config = SettingsConfigDict(
        validate_by_name=True,
        validate_by_alias=True,
        env_prefix="KAFKA_PRODUCER_",
        env_nested_delimiter="__",
        env_file=".env",
        extra="ignore",
    )

    config: KafkaProducerConfig
    error_topic: str = ""
    success_topic: str
