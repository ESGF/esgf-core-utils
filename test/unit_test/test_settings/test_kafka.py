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
                },
            }
        )

        # This appears to be meaningless, but it is actually to prevent automatic code
        # formatters from removing the line (the tsts doesn't strictly need to assert anything, it just needs
        # not to raise an Exception
        self.assertIsInstance(settings, ConsumerSettings)
