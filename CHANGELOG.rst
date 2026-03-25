Changelog
=========

All notable changes to this package are recorded in this document.

This format is based on `Keep a ChangeLog <https://keepachangelog.com/>`_ and this project
adheres to `Semantic Versioning <https://semver.org>`_.

[Unreleased]
------------

Added
^^^^^

Changed
^^^^^^^

Removed
^^^^^^^

Fixed
^^^^^

`1.0.5 <https://github.com/ESGF/esgf-core-utils/releases/tag/1.0.6>`_  - 2026-03-25
-----------------------------------------------------------------------------------------

Added
^^^^^

Changed
^^^^^^^

- Rename project.

Removed
^^^^^^^


`1.0.5 <https://github.com/ESGF/esgf-playground-utils/releases/tag/1.0.5>`_  - 2025-07-11
-----------------------------------------------------------------------------------------

Added
^^^^^

Changed
^^^^^^^

Removed
^^^^^^^
- Removed custom Item model.

Fixed
^^^^^

`1.0.4 <https://github.com/ESGF/esgf-playground-utils/releases/tag/1.0.4>`_  - 2025-06-24
-----------------------------------------------------------------------------------------

Added
^^^^^

Changed
^^^^^^^

Removed
^^^^^^^

Fixed
^^^^^
- Fixed stac-pydantic requirement.

`1.0.3 <https://github.com/ESGF/esgf-playground-utils/releases/tag/1.0.3>`_  - 2025-06-20
-----------------------------------------------------------------------------------------

Added
^^^^^

Changed
^^^^^^^
- Updated to stac-fastapi v6.0.0.

Removed
^^^^^^^

Fixed
^^^^^

`1.0.2 <https://github.com/ESGF/esgf-playground-utils/releases/tag/1.0.2>`_  - 2025-06-18
-----------------------------------------------------------------------------------------

Added
^^^^^

Changed
^^^^^^^
- Updated PatchPayload to use PatialItem and PatchOperation.

Removed
^^^^^^^

Fixed
^^^^^

`1.0.1 <https://github.com/ESGF/esgf-playground-utils/releases/tag/1.0.1>`_  - 2025-03-05
-----------------------------------------------------------------------------------------

Added
^^^^^

Changed
^^^^^^^
- Updated ESGFItemProperties citation_url made optional.

Removed
^^^^^^^

Fixed
^^^^^

`1.0.0 <https://github.com/ESGF/esgf-playground-utils/releases/tag/1.0.0>`_  - 2025-02-14
-----------------------------------------------------------------------------------------

Added
^^^^^

Changed
^^^^^^^
- Updated ESGFItemProperties to more accurate typing.
- Updated Kafka Models in particular authorisation section.

Removed
^^^^^^^

Fixed
^^^^^

`0.3.8 <https://github.com/ESGF/esgf-playground-utils/releases/tag/0.3.8>`_  - 2024-11-19
-----------------------------------------------------------------------------------------

Added
^^^^^
- Fix to KafakEvent model to allow ExtendedMEtadata model to be used in the event

Changed
^^^^^^^

Removed
^^^^^^^

Fixed
^^^^^

`0.3.7 <https://github.com/ESGF/esgf-playground-utils/releases/tag/0.3.7>`_  - 2024-11-19
-----------------------------------------------------------------------------------------

Added
^^^^^
- A new model which is an extension of the Metadata Model for Publish Events.

Changed
^^^^^^^

Removed
^^^^^^^

Fixed
^^^^^

`0.3.6 <https://github.com/ESGF/esgf-playground-utils/releases/tag/0.3.6>`_  - 2024-11-18
-----------------------------------------------------------------------------------------

Added
^^^^^

Changed
^^^^^^^

- Changed AuthData model (:py:class:`esgf_core_utils.models.kafka.AuthData`) to match event schema

Removed
^^^^^^^

Fixed
^^^^^

`0.3.4 <https://github.com/ESGF/esgf-playground-utils/releases/tag/0.3.4>`_  - 2024-11-06
-----------------------------------------------------------------------------------------

Added
^^^^^
- Added AuthData model (:py:class:`esgf_core_utils.models.kafka.AuthData`) for adding 
  more detailed auth data to a publish event

Changed
^^^^^^^

- Updated virtualenv (20.26.3 -> 20.27.1)

Removed
^^^^^^^

Fixed
^^^^^


`0.3.3 <https://github.com/ESGF/esgf-playground-utils/releases/tag/0.3.3>`_  - 2024-09-04
-----------------------------------------------------------------------------------------

Added
^^^^^
- Added new kafka PartialUpdatePayload to Data class

Changed
^^^^^^^

- Updated poetry (1.82 -> 1.83)

Removed
^^^^^^^

Fixed
^^^^^


`0.3.2 <https://github.com/ESGF/esgf-playground-utils/releases/tag/0.3.2>`_  - 2024-09-04
-----------------------------------------------------------------------------------------

Added
^^^^^

- Payload model (:py:class:`esgf_core_utils.models.kafka.PartialUpdatePayload`) for "soft delete` of an
  :py:class:`stac_pydantic.item.Item`.

Changed
^^^^^^^

- Updated certifi (2024.7.4 -> 2024.8.30)
- Updated idna (3.7 -> 3.8)
- Updated pbr (6.0.0 -> 6.1.0)
- Updated geojson-pydantic (1.1.0 -> 1.1.1)
- Updated rich (13.7.1 -> 13.8.0)
- Updated stevedore (5.2.0 -> 5.3.0)
- Updated mypy (1.11.1 -> 1.11.2)
- Updated sphinx-autoapi (3.2.1 -> 3.3.1)
- Updated stac-pydantic (3.1.1 -> 3.1.2)

Removed
^^^^^^^

Fixed
^^^^^

`0.3.1 <https://github.com/ESGF/esgf-playground-utils/releases/tag/0.3.1>`_ - 2024-08-13
----------------------------------------------------------------------------------------

Added
^^^^^

- Community documentation
- Pre-commit hooks for quality
- Documentation published to GitHub Pages

Changed
^^^^^^^

- :py:attr:`esgf_core_utils.models.kafka.UpdatePayload.item_id` added to enable canonical id of resource
  to be changed.

Removed
^^^^^^^

Fixed
^^^^^

`0.3.0 <https://github.com/ESGF/esgf-playground-utils/releases/tag/0.3.0>`_ - 2024-07-22
------------------------------------------------------------------------------------------

Initial working release.

Added
^^^^^

Changed
^^^^^^^

Removed
^^^^^^^

Fixed
^^^^^

