"""
Deprecated alias of :py:mod:`esgf_core_models.aggregation_assets`.

.. deprecated:: 1.4.0
    Import from :py:mod:`esgf_core_models.aggregation_assets` instead. This module will be removed in a
    future release.
"""

import warnings

from esgf_core_models.aggregation_assets import *  # noqa: F403

warnings.warn(
    "esgf_core_utils.models.aggregation_assets is deprecated and will be removed in a future release, "
    "use esgf_core_models.aggregation_assets instead.",
    DeprecationWarning,
    stacklevel=2,
)
