from typing import Annotated, Any, Self

from pydantic import ConfigDict, TypeAdapter, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode

from esgf_core_utils.models.kafka.config import KafkaConsumerConfig


class ConsumerSettings(BaseSettings):
    """
    Event Stream Settings
    """

    model_config = ConfigDict(
        validate_by_name=True,
        env_prefix="KAFKA_CONSUMER_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    config: KafkaConsumerConfig
    topics: Annotated[list[str], NoDecode]
    timeout: float = 5.0

    debug: bool = False

    @model_validator(mode="after")
    def check_debug(self) -> Self:
        """
        Check if debug is set if so update kafka config.
        """
        if self.debug:
            self.config.debug = self.config.debug or "all"
            self.config.log_level = self.config.log_level or 7

        return self

    @field_validator("topics", mode="before")
    @classmethod
    def split_topics(cls, v: Any) -> list[str]:
        """
        Accept comma-separated string or list.
        """
        if isinstance(v, str):
            v = [t.strip() for t in v.split(",")]

        return TypeAdapter(list[str]).validate_python(v)
