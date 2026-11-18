"""
Deprecated alias of :py:mod:`esgf_core_models.exceptions`.

.. deprecated:: 1.4.0
    Import from :py:mod:`esgf_core_models.exceptions` instead. This module will be removed in a
    future release.
"""

import warnings

from esgf_core_models.exceptions import *  # noqa: F403

warnings.warn(
    "esgf_core_utils.models.exceptions is deprecated and will be removed in a future release, "
    "use esgf_core_models.exceptions instead.",
    DeprecationWarning,
    stacklevel=2,
)
