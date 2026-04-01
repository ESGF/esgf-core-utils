from pydantic_settings import BaseSettings, SettingsConfigDict

from esgf_core_utils.models.kafka.config import KafkaConfig


class ProducerSettings(BaseSettings):
    """
    Event Stream Settings
    """

    model_config = SettingsConfigDict(
        validate_by_name=True,
        validate_by_alias=True,
        env_prefix="KAFKA_PRODUCER_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    config: KafkaConfig
    error_topic: str | None = None
    success_topic: str
