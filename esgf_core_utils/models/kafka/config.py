import socket

from pydantic import BaseModel, ConfigDict, Field


class KafkaConfig(BaseModel):
    """
    Kafka Config
    """

    model_config = ConfigDict(
        validate_by_name=True,
        validate_by_alias=True,
        extra="ignore",
    )

    bootstrap_servers: str = Field(alias="bootstrap.servers")
    sasl_mechanism: str = Field(default="PLAIN", alias="sasl.mechanism")
    sasl_username: str = Field(alias="sasl.username")
    sasl_password: str = Field(alias="sasl.password")
    security_protocol: str = Field(default="SASL_SSL", alias="security.protocol")
    client_id: str = Field(default=socket.gethostname(), alias="client.id")


class KafkaConsumerConfig(KafkaConfig):
    """
    Kafka Consumer Config
    """

    auto_offset_reset: str = Field(default="earliest", alias="auto.offset.reset")
    enable_auto_commit: bool = Field(default=False, alias="enable.auto.commit")
    group_id: str = Field(alias="group.id")
    session_timeout_ms: int = Field(default=45000, alias="session.timeout.ms")
    debug: str | None = None
    log_level: int | None = None
