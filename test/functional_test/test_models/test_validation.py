"""Functional tests for validation."""

import unittest
from unittest.mock import Mock, patch

from jsonschema import ValidationError
from stac_fastapi.extensions.transaction.request import (
    PartialItem,
    PatchAddReplaceTest,
)

from esgf_core_utils.models.exceptions import (
    ExpectedExtensionsMissingException,
    OperationNotPermittedException,
    STACValidationException,
    UnexpectedExtensionException,
)
from esgf_core_utils.models.validation import (
    evaluate_patch,
    get_null_keys,
    operation_to_partial_item,
    validate_extensions,
    validate_geometry,
    validate_patch,
    validate_post,
)


class TestOperationToPartialItem(unittest.TestCase):
    """Functional tests for operation_to_partial_item."""

    def test_add_operation(self) -> None:
        """Ensure add operations are converted."""

        item = operation_to_partial_item(
            "cmip6",
            [
                PatchAddReplaceTest(
                    op="add",
                    path="/id",
                    value="item1",
                )
            ],
        )

        self.assertEqual(
            item.id,
            "item1",
        )

    def test_move_operation_not_permitted(
        self,
    ) -> None:
        """Ensure move operations raise."""

        operation = Mock()
        operation.op = "move"
        operation.path = "/id"

        with self.assertRaises(OperationNotPermittedException):
            operation_to_partial_item(
                "cmip6",
                [operation],
            )

    def test_copy_operation_not_permitted(
        self,
    ) -> None:
        """Ensure copy operations raise."""
        operation = Mock()
        operation.op = "copy"
        operation.path = "/id"

        with self.assertRaises(OperationNotPermittedException):
            operation_to_partial_item(
                "cmip6",
                [operation],
            )

    def test_add_numeric_list_path(
        self,
    ) -> None:
        """Ensure numeric path segments become list updates."""
        item = operation_to_partial_item(
            "cmip6",
            [
                PatchAddReplaceTest(
                    op="add",
                    path="/properties/keywords/1",
                    value="value",
                )
            ],
        )

        self.assertIsNotNone(item.properties)
        self.assertEqual(
            item.properties["keywords"],  # type: ignore[index]
            ["value"],
        )

    def test_add_append_list_path(
        self,
    ) -> None:
        """Ensure '-' path segments become list updates."""
        item = operation_to_partial_item(
            "cmip6",
            [
                PatchAddReplaceTest(
                    op="add",
                    path="/properties/keywords/-",
                    value="value",
                )
            ],
        )

        self.assertIsNotNone(item.properties)

        self.assertEqual(
            item.properties["keywords"],  # type: ignore[index]
            ["value"],
        )

    def test_add_numeric_list_path_extends_existing(
        self,
    ) -> None:
        """Ensure numeric list paths extend existing values."""
        item = operation_to_partial_item(
            "cmip6",
            [
                PatchAddReplaceTest(
                    op="add",
                    path="/properties/keywords",
                    value=["existing"],
                ),
                PatchAddReplaceTest(
                    op="add",
                    path="/properties/keywords/1",
                    value="new",
                ),
            ],
        )

        self.assertIsNotNone(item.properties)

        self.assertEqual(
            item.properties["keywords"],  # type: ignore[index]
            ["new", "existing"],
        )

    def test_remove_operation(self) -> None:
        """Ensure remove operations become add None."""

        item = operation_to_partial_item(
            "cmip6",
            [
                Mock(
                    op="remove",
                    path="/properties/title",
                )
            ],
        )

        self.assertIsNotNone(item.properties)

        self.assertIsNone(item.properties["title"])  # type: ignore[index]

    def test_add_list_value(self) -> None:
        """Ensure list values are handled."""

        item = operation_to_partial_item(
            "cmip6",
            [
                PatchAddReplaceTest(
                    op="add",
                    path="/properties/keywords",
                    value=["a", "b"],
                )
            ],
        )

        self.assertIsNotNone(item.properties)

        self.assertEqual(
            item.properties["keywords"],  # type: ignore[index]
            ["a", "b"],
        )

    @patch("esgf_core_utils.models.validation.get_extension_validator")
    @patch("esgf_core_utils.models.validation.get_null_keys")
    def test_validate_patch_required_removed_key(
        self,
        mock_null_keys: Mock,
        mock_validator: Mock,
    ) -> None:
        """Ensure removed required properties raise."""
        error = Mock()
        error.validator = "required"
        error.validator_value = ["removed"]

        validator = Mock()
        validator.iter_errors.return_value = [error]
        mock_validator.return_value = validator

        item = PartialItem.model_validate({})

        mock_null_keys.return_value = (
            item,
            {'["removed"]'},
        )

        with self.assertRaises(STACValidationException):
            validate_patch(
                "item1",
                item,
                ["ext"],
            )


class TestValidateExtensions(unittest.TestCase):
    """Functional tests for validate_extensions."""

    @patch("esgf_core_utils.models.validation.settings.default_extensions")
    @patch("esgf_core_utils.models.validation.validate_extension_version")
    def test_validate_extensions_success(
        self,
        mock_validate: Mock,
        mock_defaults: Mock,
    ) -> None:
        """Ensure expected extensions pass."""

        mock_defaults.get.return_value = {
            "x": {
                "default": "ext/v1.0.0/schema.json",
                "regex": [
                    r".*schema\.json",
                ],
            }
        }

        validate_extensions(
            "collection",
            [
                "ext/v1.0.0/schema.json",
            ],
        )

    @patch("esgf_core_utils.models.validation.settings.default_extensions")
    def test_validate_extensions_unexpected(
        self,
        mock_defaults: Mock,
    ) -> None:
        """Ensure unexpected extensions raise."""

        mock_defaults.get.return_value = {}

        with self.assertRaises(UnexpectedExtensionException):
            validate_extensions(
                "collection",
                ["unexpected"],
            )

    @patch("esgf_core_utils.models.validation.settings.default_extensions")
    def test_validate_extensions_missing_strict(
        self,
        mock_defaults: Mock,
    ) -> None:
        """Ensure missing extensions raise in strict mode."""

        mock_defaults.get.return_value = {
            "x": {
                "default": "required",
                "regex": [
                    r"required",
                ],
            }
        }

        with self.assertRaises(ExpectedExtensionsMissingException):
            validate_extensions(
                "collection",
                [],
                strict=True,
            )

    @patch("esgf_core_utils.models.validation.settings.default_extensions")
    @patch("esgf_core_utils.models.validation.validate_extension_version")
    def test_validate_extensions_second_match(
        self,
        mock_validate: Mock,
        mock_defaults: Mock,
    ) -> None:
        """Ensure later expected extensions are evaluated."""
        mock_defaults.get.return_value = {
            "a": {
                "default": "ext-a/v1.0.0/schema.json",
                "regex": [r"ext-a.*"],
            },
            "b*": {
                "default": "ext-b/*1.0.0/schema.json",
                "regex": [r"ext-b.*"],
            },
        }

        extensions = validate_extensions(
            "collection",
            [
                "ext-b/v1.0.0/schema.json",
            ],
        )

        self.assertIn(
            "ext-a/v1.0.0/schema.json",
            extensions,
        )

        mock_validate.assert_called_once()

    @patch("esgf_core_utils.models.validation.validate_extensions")
    def test_patch_stac_extensions_validation(
        self,
        mock_validate: Mock,
    ) -> None:
        """Ensure stac extension updates are validated."""

        operation_to_partial_item(
            "cmip6",
            [
                PatchAddReplaceTest(
                    op="add",
                    path="/stac_extensions",
                    value=["ext"],
                )
            ],
        )

        mock_validate.assert_called_once()

    def test_get_nested_null_keys(self) -> None:
        """Ensure nested null keys are removed."""

        item = PartialItem.model_validate(
            {
                "properties": {
                    "outer": {
                        "inner": None,
                    }
                }
            }
        )

        _, null_keys = get_null_keys(item)

        self.assertIn(
            "inner",
            null_keys,
        )

    def test_validate_geometry_invalid(self) -> None:
        """Ensure invalid geometry raises."""

        geometry = {
            "type": "Polygon",
            "coordinates": [
                [
                    [0, 0],
                    [1, 1],
                    [1, 0],
                    [0, 1],
                    [0, 0],
                ]
            ],
        }

        with self.assertRaises(STACValidationException):
            validate_geometry(geometry)

    @patch("esgf_core_utils.models.validation.PATCH_VALIDATORS")
    def test_evaluate_patch_first_match(
        self,
        mock_validators: Mock,
    ) -> None:
        """Ensure matching validator name returned."""

        validator = Mock()
        validator.is_valid.return_value = True

        mock_validators.items.return_value = [
            ("CREATE", validator),
        ]

        self.assertEqual(
            evaluate_patch({}),
            "CREATE",
        )

    @patch("esgf_core_utils.models.validation.get_extension_validator")
    def test_validate_patch_success(
        self,
        mock_validator: Mock,
    ) -> None:
        """Ensure valid patch passes."""

        validator = Mock()
        validator.iter_errors.return_value = []

        mock_validator.return_value = validator

        item = PartialItem.model_validate(
            {
                "geometry": {
                    "type": "Point",
                    "coordinates": [0.0, 0.0],
                },
                "bbox": [-1.0, -1.0, 1.0, 1.0],
            }
        )

        validate_patch(
            "item1",
            item,
            ["ext"],
        )

    @patch("esgf_core_utils.models.validation.get_null_keys")
    @patch("esgf_core_utils.models.validation.get_extension_validator")
    def test_validate_patch_required_error(
        self,
        mock_validator: Mock,
        mock_null_keys: Mock,
    ) -> None:
        """Ensure required field validation raises."""

        error = Mock()
        error.validator = "required"
        error.validator_value = ["a"]

        validator = Mock()
        validator.iter_errors.return_value = [error]
        mock_validator.return_value = validator

        item = PartialItem.model_validate({})

        mock_null_keys.return_value = (
            item,
            {'["a"]'},
        )

        with self.assertRaises(STACValidationException):
            validate_patch(
                "item1",
                item,
                ["ext"],
            )

    @patch("esgf_core_utils.models.validation.get_extension_validator")
    def test_validate_patch_validation_error(
        self,
        mock_validator: Mock,
    ) -> None:
        """Ensure validation errors raise."""

        error = Mock()
        error.validator = "type"

        validator = Mock()
        validator.iter_errors.return_value = [error]

        mock_validator.return_value = validator

        item = PartialItem.model_validate({})

        with self.assertRaises(STACValidationException):
            validate_patch(
                "item1",
                item,
                ["ext"],
            )

    @patch("esgf_core_utils.models.validation.get_extension_validator")
    def test_validate_patch_oneof_ignored(
        self,
        mock_validator: Mock,
    ) -> None:
        """Ensure oneOf errors are ignored."""

        error = Mock()
        error.validator = "oneOf"

        validator = Mock()
        validator.iter_errors.return_value = [error]

        mock_validator.return_value = validator

        item = PartialItem.model_validate({})

        validate_patch(
            "item1",
            item,
            ["ext"],
        )


class TestValidateGeometry(unittest.TestCase):
    """Functional tests for validate_geometry."""

    def test_validate_geometry_success(
        self,
    ) -> None:
        """Ensure valid geometry passes."""

        validate_geometry(
            {
                "type": "Point",
                "coordinates": [
                    0.0,
                    0.0,
                ],
            }
        )

    def test_validate_geometry_missing(
        self,
    ) -> None:
        """Ensure missing geometry raises."""

        with self.assertRaises(STACValidationException):
            validate_geometry(None)

    @patch("esgf_core_utils.models.validation.get_extension_validator")
    def test_validate_post_success(
        self,
        mock_validator: Mock,
    ) -> None:
        """Ensure valid post passes."""

        validator = Mock()
        validator.iter_errors.return_value = []

        mock_validator.return_value = validator

        item = Mock()
        item.geometry = {
            "type": "Point",
            "coordinates": [0.0, 0.0],
        }
        item.bbox = [-1.0, -1.0, 1.0, 1.0]
        item.model_dump_json.return_value = "{}"

        validate_post(
            "item1",
            item,
            ["ext"],
        )

    @patch("esgf_core_utils.models.validation.get_extension_validator")
    def test_validate_post_validation_error(
        self,
        mock_validator: Mock,
    ) -> None:
        """Ensure post validation errors raise."""

        error = ValidationError("Validation failed")

        validator = Mock()
        validator.iter_errors.return_value = [error]

        mock_validator.return_value = validator

        item = Mock()
        item.geometry = {
            "type": "Point",
            "coordinates": [0.0, 0.0],
        }
        item.bbox = [-1.0, -1.0, 1.0, 1.0]
        item.model_dump_json.return_value = "{}"

        with self.assertRaises(STACValidationException):
            validate_post(
                "item1",
                item,
                ["ext"],
            )

    @patch("esgf_core_utils.models.validation.logger")
    @patch("esgf_core_utils.models.validation.get_extension_validator")
    def test_validate_post_logs_error(
        self,
        mock_validator: Mock,
        mock_logger: Mock,
    ) -> None:
        """Ensure validation failures are logged."""

        error = ValidationError("Validation failed")

        validator = Mock()
        validator.iter_errors.return_value = [error]
        mock_validator.return_value = validator

        item = Mock()
        item.geometry = {
            "type": "Point",
            "coordinates": [0.0, 0.0],
        }
        item.bbox = [-1.0, -1.0, 1.0, 1.0]
        item.model_dump_json.return_value = "{}"

        with self.assertRaises(STACValidationException):
            validate_post(
                "item1",
                item,
                ["ext"],
            )

        mock_logger.error.assert_called_once_with(
            "STAC validation error: item1",
        )
