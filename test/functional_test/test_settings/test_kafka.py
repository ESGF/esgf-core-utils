"""
Functional tests for the ConsumerSettings class.

These tests validate full model behaviour including:
- Environment variable loading
- Combined validator interaction
- Default timeout behaviour
"""

import os
import unittest
from unittest.mock import patch

from esgf_core_utils.models.kafka.config import KafkaConsumerConfig
from esgf_core_utils.settings.kafka.consumer import ConsumerSettings


class TestConsumerSettingsFunctional(unittest.TestCase):
    """Functional tests for Pydantic BaseSettings interaction."""

    @patch.dict(os.environ, {}, clear=True)
    def test_instantiation_with_explicit_values(self) -> None:
        """Test creating ConsumerSettings with direct constructor values."""
        cfg = KafkaConsumerConfig(
            bootstrap_servers="boots",
            sasl_username="hello",
            sasl_password="world",
            group_id="foo",
        )  # nosec CWE-259

        settings = ConsumerSettings(
            config=cfg,
            topics=["t1", "t2"],
        )

        self.assertEqual(settings.topics, ["t1", "t2"])
        self.assertEqual(settings.timeout, 5.0)
        self.assertFalse(settings.debug)

    @patch.dict(
        os.environ,
        {
            "KAFKA_CONSUMER_TOPICS": "local",
            "KAFKA_CONSUMER_CONFIG__BOOTSTRAP_SERVERS": "boots",
            "KAFKA_CONSUMER_CONFIG__SASL_USERNAME": "hello",
            "KAFKA_CONSUMER_CONFIG__SASL_PASSWORD": "world",
            "KAFKA_CONSUMER_CONFIG__GROUP_ID": "foo",
            "KAFKA_CONSUMER_DEBUG": "true",
            "KAFKA_CONSUMER_GROUP_ID": "foo",
        },  # nosec CWE-259
        clear=True,
    )
    def test_environment_loading(self) -> None:
        """Ensure BaseSettings loads environment variables using prefixes and nested delimiter."""
        settings = ConsumerSettings()

        self.assertEqual(settings.config.debug, "all")
        self.assertEqual(settings.config.log_level, 7)
        self.assertEqual(settings.topics, ["local"])
