"""
Test the functions in kafka.config module
"""

import unittest

from esgf_core_utils.config.kafka import KafkaConfig


class TestSettings(unittest.TestCase):
    """Test the functionality of the Settings class"""

    def test_init(self) -> None:
        """Settings should be creatable with no additional import or env files"""

        settings = KafkaConfig()

        # This appears to be meaningless, but it is actually to prevent automatic code
        # formatters from removing the line (the tsts doesn't strictly need to assert anything, it just needs
        # not to raise an Exception
        self.assertIsInstance(settings, KafkaConfig)
