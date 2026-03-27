from typing import Annotated, Self

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode

from esgf_core_utils.models.kafka.config import KafkaConsumerConfig


class ConsumerSettings(BaseSettings):
    """
    Event Stream Settings
    """

    class Config:
        validate_by_name = True
        env_prefix = "KAFKA_CONSUMER_"
        env_nested_delimiter = "__"
        extra = "ignore"

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
            if self.config.debug is None:
                setattr(self.config, "debug", "all")

            if self.config.level is None:
                setattr(self.config, "level", 7)

        return self

    @field_validator("topics", mode="before")
    @classmethod
    def split_topics(cls, v) -> list[str]:
        """
        Accept comma-separated string or list.
        """
        if isinstance(v, str):
            return [t.strip() for t in v.split(",") if t.strip()]

        if isinstance(v, (list, tuple)):
            return list(v)

        raise TypeError("topics must be a string or list")
