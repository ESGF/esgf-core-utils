"""Unit tests for validation helpers."""

import unittest
from unittest.mock import Mock, patch

from jsonschema.validators import Draft202012Validator
from stac_fastapi.extensions.transaction.request import PartialItem

from esgf_core_utils.models.exceptions import (
    ExtensionBelowMinimumException,
    STACValidationException,
)
from esgf_core_utils.models.validation import (
    evaluate_patch,
    extract_version,
    get_null_keys,
    validate_bbox,
    validate_extension_version,
)
from esgf_core_utils.models.validation.patch_validators import (
    PATCH_SCHEMAS,
    PATCH_VALIDATORS,
)


class TestExtractVersion(unittest.TestCase):
    """Tests for extract_version."""

    @patch("esgf_core_utils.models.validation.settings.version_regex")
    def test_extract_version(self, mock_regex: Mock) -> None:
        """Ensure versions are extracted."""

        match = Mock()
        match.group.return_value = "1.0.0"

        mock_regex.search.return_value = match

        self.assertEqual(
            extract_version("example"),
            "1.0.0",
        )

    @patch("esgf_core_utils.models.validation.settings.version_regex")
    def test_extract_version_failure(self, mock_regex: Mock) -> None:
        """Ensure parse failures raise."""

        mock_regex.search.return_value = None

        with self.assertRaises(ValueError):
            extract_version("invalid")


class TestValidateExtensionVersion(unittest.TestCase):
    """Tests for validate_extension_version."""

    @patch("esgf_core_utils.models.validation.extract_version")
    def test_validate_extension_version_success(
        self,
        mock_extract: Mock,
    ) -> None:
        """Ensure newer versions are accepted."""

        mock_extract.side_effect = [
            "1.0.0",
            "1.0.1",
        ]

        validate_extension_version(
            "minimum",
            "extension",
        )

    @patch("esgf_core_utils.models.validation.extract_version")
    def test_validate_extension_version_failure(
        self,
        mock_extract: Mock,
    ) -> None:
        """Ensure older versions raise."""

        mock_extract.side_effect = [
            "2.0.0",
            "1.0.0",
        ]

        with self.assertRaises(ExtensionBelowMinimumException):
            validate_extension_version(
                "minimum",
                "extension",
            )


class TestValidateBBox(unittest.TestCase):
    """Tests for validate_bbox."""

    def test_validate_bbox_success(self) -> None:
        """Ensure valid bounding boxes pass."""

        validate_bbox(
            (-10.0, -10.0, 10.0, 10.0),
        )

    def test_validate_bbox_none(self) -> None:
        """Ensure missing bbox raises."""

        with self.assertRaises(STACValidationException):
            validate_bbox(None)

    def test_validate_bbox_invalid(self) -> None:
        """Ensure invalid bbox raises."""

        with self.assertRaises(STACValidationException):
            validate_bbox(
                (-200.0, -10.0, 10.0, 10.0),
            )


class TestGetNullKeys(unittest.TestCase):
    """Tests for get_null_keys."""

    def test_get_null_keys(self) -> None:
        """Ensure null keys are removed."""

        item = PartialItem.model_validate(
            {
                "properties": {
                    "a": None,
                    "b": "value",
                }
            }
        )

        updated_item, null_keys = get_null_keys(item)

        self.assertIn("a", null_keys)

        self.assertNotIn(
            "a",
            updated_item.model_dump()["properties"],
        )


class TestPatchValidators(unittest.TestCase):
    """Tests for patch validator definitions."""

    def test_patch_validators_created(self) -> None:
        """Ensure validators are created for all patch schemas."""

        self.assertEqual(
            set(PATCH_VALIDATORS.keys()),
            {schema_name.upper() for schema_name in PATCH_SCHEMAS},
        )

        for validators in PATCH_VALIDATORS.values():
            self.assertTrue(validators)

            for validator in validators:
                self.assertIsInstance(
                    validator,
                    Draft202012Validator,
                )


class TestEvaluatePatch(unittest.TestCase):
    """Tests for evaluate_patch."""

    @patch("esgf_core_utils.models.validation.PATCH_VALIDATORS")
    def test_evaluate_patch_match(
        self,
        mock_validators: Mock,
    ) -> None:
        """Ensure matching validator name returned."""

        validator = Mock()
        validator.is_valid.return_value = True

        mock_validators.items.return_value = [
            (
                "DELETE",
                [validator],
            )
        ]

        self.assertEqual(
            evaluate_patch([]),
            "DELETE",
        )

    @patch("esgf_core_utils.models.validation.PATCH_VALIDATORS")
    def test_evaluate_patch_default(
        self,
        mock_validators: Mock,
    ) -> None:
        """Ensure UPDATE is default."""

        validator = Mock()
        validator.is_valid.return_value = False

        mock_validators.items.return_value = [
            (
                "DELETE",
                [validator],
            )
        ]

        self.assertEqual(
            evaluate_patch([]),
            "UPDATE",
        )
