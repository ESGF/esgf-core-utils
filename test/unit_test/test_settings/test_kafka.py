"""
Test the functions in kafka.config module
"""

import unittest

from esgf_core_utils.settings.kafka.consumer import ConsumerSettings
from esgf_core_utils.settings.kafka.producer import ProducerSettings


class TestProducerSettings(unittest.TestCase):
    """Test the functionality of the kafka ProducerSettings class"""

    def test_init(self) -> None:
        """Settings should be creatable with no additional import or env files"""

        settings = ProducerSettings(
            config={
                "bootstrap_servers": "boots",
                "sasl_protocol": "SASL_PLAINTEXT",
                "sasl_mechanism": "PLAIN",
                "sasl_username": "hello",
                "sasl_password": "world",
            },  # nosec CWE-259
            error_topic="error",
            success_topic="success",
        )

        # This appears to be meaningless, but it is actually to prevent automatic code
        # formatters from removing the line (the tsts doesn't strictly need to assert anything, it just needs
        # not to raise an Exception
        self.assertIsInstance(settings, ProducerSettings)


class TestConsumerSettings(unittest.TestCase):
    """Test the functionality of the kafka ConsumerSettings class"""

    def test_init(self) -> None:
        """Settings should be creatable with no additional import or env files"""

        settings = ConsumerSettings(
            config={
                "bootstrap_servers": "boots",
                "sasl_protocol": "SASL_PLAINTEXT",
                "sasl_mechanism": "PLAIN",
                "sasl_username": "hello",
                "sasl_password": "world",
                "group_id": "foo",
            },  # nosec CWE-259
            topics="local",
        )

        # This appears to be meaningless, but it is actually to prevent automatic code
        # formatters from removing the line (the tsts doesn't strictly need to assert anything, it just needs
        # not to raise an Exception
        self.assertIsInstance(settings, ConsumerSettings)
