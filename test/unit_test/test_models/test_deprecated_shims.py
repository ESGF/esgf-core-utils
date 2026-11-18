"""
Tests for the deprecated ``esgf_core_utils.models`` aliases of ``esgf_core_models``.
"""

import importlib
import sys
import unittest

import esgf_core_models.aggregation_assets
import esgf_core_models.auth
import esgf_core_models.exceptions
import esgf_core_models.kafka.config
import esgf_core_models.kafka.events

SHIMS = {
    "esgf_core_utils.models.aggregation_assets": esgf_core_models.aggregation_assets,
    "esgf_core_utils.models.auth": esgf_core_models.auth,
    "esgf_core_utils.models.exceptions": esgf_core_models.exceptions,
    "esgf_core_utils.models.kafka.config": esgf_core_models.kafka.config,
    "esgf_core_utils.models.kafka.events": esgf_core_models.kafka.events,
}


class TestDeprecatedShims(unittest.TestCase):
    def test_import_warns_and_reexports(self) -> None:
        for old, new in SHIMS.items():
            with self.subTest(module=old):
                sys.modules.pop(old, None)
                with self.assertWarns(DeprecationWarning) as ctx:
                    module = importlib.import_module(old)
                self.assertIn(old, str(ctx.warning))
                self.assertIn(new.__name__, str(ctx.warning))
                public = [n for n in vars(new) if not n.startswith("_")]
                for name in public:
                    self.assertIs(getattr(module, name), getattr(new, name))
