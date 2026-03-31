"""
Test the functions in kafka.config module
"""

import unittest

from esgf_core_utils.models.kafka.config import KafkaConsumerConfig
from esgf_core_utils.settings.kafka.consumer import ConsumerSettings
from esgf_core_utils.settings.kafka.producer import ProducerSettings


class TestProducerSettings(unittest.TestCase):
    """Test the functionality of the kafka ProducerSettings class"""

    def test_init(self) -> None:
        """Settings should be creatable with no additional import or env files"""

        settings = ProducerSettings.model_validate(
            {
                "success_topic": "success",
                "error_topic": "error",
                "config": {
                    "bootstrap_servers": "boots",
                    "sasl_protocol": "SASL_PLAINTEXT",
                    "sasl_mechanism": "PLAIN",
                    "sasl_username": "hello",
                    "sasl_password": "world",
                },  # nosec CWE-259
            }
        )

        # This appears to be meaningless, but it is actually to prevent automatic code
        # formatters from removing the line (the tsts doesn't strictly need to assert anything, it just needs
        # not to raise an Exception
        self.assertIsInstance(settings, ProducerSettings)


class TestConsumerSettings(unittest.TestCase):
    """Test the functionality of the kafka ConsumerSettings class"""

    def test_init(self) -> None:
        """Settings should be creatable with no additional import or env files"""

        settings = ConsumerSettings.model_validate(
            {
                "topics": ["local"],
                "config": {
                    "bootstrap_servers": "boots",
                    "sasl_protocol": "SASL_PLAINTEXT",
                    "sasl_mechanism": "PLAIN",
                    "sasl_username": "hello",
                    "sasl_password": "world",
                    "group_id": "foo",
                },  # nosec CWE-259
            }
        )

        # This appears to be meaningless, but it is actually to prevent automatic code
        # formatters from removing the line (the tsts doesn't strictly need to assert anything, it just needs
        # not to raise an Exception
        self.assertIsInstance(settings, ConsumerSettings)

    def test_split_topics_from_string(self) -> None:
        """Ensure comma-separated topic strings are split into a list."""
        topics = ConsumerSettings.split_topics("a,b,c")
        self.assertEqual(topics, ["a", "b", "c"])

    def test_split_topics_from_list(self) -> None:
        """Ensure lists of topics pass through unchanged."""
        topics = ConsumerSettings.split_topics(["x", "y"])
        self.assertEqual(topics, ["x", "y"])

    def test_split_topics_strips_whitespace(self) -> None:
        """Whitespace around topics should be removed."""
        topics = ConsumerSettings.split_topics(" a ,  b , c ")
        self.assertEqual(topics, ["a", "b", "c"])

    def test_check_debug_updates_config(self) -> None:
        """
        check_debug should update config.debug and config.log_level
        when debug=True.
        """
        cfg = KafkaConsumerConfig(
            bootstrap_servers="boots",
            sasl_username="hello",
            sasl_password="world",
            debug=None,
            log_level=None,
            group_id="foo",
        )  # nosec CWE-259
        settings = ConsumerSettings(
            config=cfg,
            topics=["topic"],
            debug=True,
        )

        self.assertEqual(settings.config.debug, "all")
        self.assertEqual(settings.config.log_level, 7)

    def test_check_debug_no_update_when_debug_false(self) -> None:
        """When debug=False, validator should not modify debug/log_level."""
        cfg = KafkaConsumerConfig(
            bootstrap_servers="boots",
            sasl_username="hello",
            sasl_password="world",
            debug=None,
            log_level=None,
            group_id="foo",
        )  # nosec CWE-259
        settings = ConsumerSettings(
            config=cfg,
            topics=["topic"],
            debug=False,
        )

        self.assertIsNone(settings.config.debug)
        self.assertIsNone(settings.config.log_level)
