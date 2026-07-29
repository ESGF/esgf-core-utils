import json
import logging
import re
from typing import Any

import jsonschema
import requests
from jsonschema.protocols import Validator
from packaging.version import Version
from shapely.geometry import shape
from stac_fastapi.extensions.transaction.request import (
    PartialItem,
    PatchAddReplaceTest,
    PatchOperation,
)
from stac_pydantic.item import Item

from esgf_core_utils.models.exceptions import (
    ExpectedExtensionsMissingException,
    ExtensionBelowMinimumException,
    OperationNotPermittedException,
    STACValidationException,
    UnexpectedExtensionException,
)
from esgf_core_utils.models.validation.patch_validators import PATCH_VALIDATORS
from esgf_core_utils.settings.validation import settings

# Setup logger
logger = logging.getLogger("uvicorn.error")


type BBox2D = tuple[float | int, float | int, float | int, float | int]
type BBox3D = tuple[
    float | int,
    float | int,
    float | int,
    float | int,
    float | int,
    float | int,
]


def operation_to_partial_item(
    collection_id: str, operations: list[PatchOperation]
) -> PartialItem:
    """Convert operations to partial item

    Args:
        collection_id (str): ID of Item's Collection.
        operations (list[PatchOperation]): List of operations to be converted to PartialItem

    Raises:
        OperationNotPermittedException: Move & Copy operatations not permitted

    Returns:
        PartialItem: Partial item equivalent to operations
    """
    item: dict[str, Any] = {}
    nest: Any

    for operation in operations:

        match operation.op:

            case "remove":
                operations.append(
                    PatchAddReplaceTest(op="add", path=operation.path, value=None)
                )

            case "add" | "replace":
                if operation.path.lstrip("/") == "stac_extensions":
                    validate_extensions(
                        collection_id=collection_id,
                        item_extensions=operation.value,
                        strict=True,
                    )

                path_parts = operation.path.lstrip("/").split("/")

                if path_parts[-1] == "-" or path_parts[-1].lstrip("-").isdigit():
                    path_parts.pop()
                    nest = [operation.value]

                else:
                    nest = operation.value

                if isinstance(nest, list):
                    existing = item.copy()
                    for path_part in path_parts:
                        existing = existing.get(path_part, {})

                    if existing:
                        nest.extend(existing)

                for path_part in reversed(path_parts):
                    nest = {path_part: nest}

                item |= nest

            # Loop continuation branch is impossible because this path always raises.
            case "move" | "copy":  # pragma: no branch
                # May need to update this for alternat asset updates
                raise OperationNotPermittedException(op=operation.op)

    return PartialItem.model_validate(item)


def extract_version(url: str) -> str:
    """extract version from extension url.

    Args:
        url (str): extension url.

    Raises:
        ValueError: If version can not be extracted.

    Returns:
        str: extracted version.
    """
    match = settings.version_regex.search(url)

    if match is None:
        raise ValueError(f"Could not parse version from {url!r}")

    return match.group(1)


def validate_extension_version(minimum: str, extension: str) -> None:
    """Validate an extenions version is above the minimum.

    Args:
        default (str): default extension with minimum version.
        extension (str): extension to be validated.

    Raises:
        ExtensionBelowMinimumException: extension below minimum
    """

    minimum_version = extract_version(minimum)
    extension_version = extract_version(extension)

    if Version(extension_version) < Version(minimum_version):
        raise ExtensionBelowMinimumException(
            extension=extension, minimum_version=f"v{minimum_version}"
        )


def validate_extensions(
    collection_id: str, item_extensions: list[str], strict: bool = False
) -> list[str]:
    """Validate expected default extensions are present.

    Args:
        collection_id (str): ID of Item's Collection.
        item_extensions (list[str]): Given list of extensions.
        strict (bool): if True Exception is raised if expected extensions are missing.

    Raises:
        UnexpectedExtensionException: Unexpected extensions
        ExpectedExtensionsMissingException: Expected extension missing

    Returns:
        list[str]: list of extensions including defaults
    """

    expected_extensions = settings.default_extensions.get(collection_id, {}).copy()

    for item_extension in item_extensions:
        expected = False
        for (
            expected_extension_key,
            expected_extension,
        ) in expected_extensions.copy().items():
            if any(
                re.compile(regex).match(str(item_extension))
                for regex in expected_extension["regex"]
            ):
                expected_extensions.pop(expected_extension_key)
                expected = True

                validate_extension_version(
                    minimum=expected_extension["default"], extension=str(item_extension)
                )

        if not expected:
            raise UnexpectedExtensionException(extension=item_extension)

    missing_extensions = [
        expected_extension["default"]
        for expected_extension in expected_extensions.values()
    ]

    if strict & len(missing_extensions) > 0:
        raise ExpectedExtensionsMissingException(extensions=missing_extensions)

    item_extensions.extend(missing_extensions)

    return item_extensions


def get_null_keys(item: PartialItem) -> tuple[PartialItem, set[str]]:
    """Remove and list null value keys from PatialItem.

    Args:
        item (dict): Item dictionary to be updated

    Returns:
        tuple[dict, list[str]]: The PartialItem with nulls removed and list of null keys
    """

    def nested_null_keys(d: dict[str, Any]) -> tuple[dict[str, Any], set[str]]:
        null_keys = set()
        for k, v in d.copy().items():

            if v is None:
                del d[k]
                null_keys.add(k)

            if isinstance(v, dict):
                sub_dict, sub_null_keys = nested_null_keys(v)
                null_keys.update(sub_null_keys)
                d[k] = sub_dict

        return d, null_keys

    item_dict, null_keys = nested_null_keys(item.model_dump())
    item = PartialItem.model_validate(item_dict)

    return item, null_keys


def get_extension_validator(extension: str) -> Validator:
    """Get JSON schema validator for an extension.

    Args:
        extension (str): Extension URI

    Returns:
        Validator: Validator for extension
    """
    schema = requests.get(extension, timeout=5.0).json()
    # This block is cribbed (w/ change in error handling) from
    # jsonschema.validate
    cls = jsonschema.validators.validator_for(schema)
    cls.check_schema(schema)
    return cls(schema)


def validate_bbox(
    bbox: BBox2D | BBox3D | None,
) -> None:
    """Validate bounding box is WGS84

    Args:
        bbox (int | float): bounding box to be validated

    Raises:
        STACValidationException: _description_
    """
    if bbox is None:
        raise STACValidationException()

    minx, miny, maxx, maxy = bbox[:4]
    if not (
        -180.0 <= minx <= 180.0
        and -180.0 <= maxx <= 180.0
        and -90.0 <= miny <= 90.0
        and -90.0 <= maxy <= 90.0
    ):
        raise STACValidationException()


def validate_geometry(geometry: Any | None) -> None:
    """Validate GeoJSON geometry

    Args:
        geometry (dict): geometry to be validation.

    Raises:
        STACValidationException: Validation error
    """
    if not geometry:
        raise STACValidationException()

    geometry_shape = shape(geometry)

    if not geometry_shape.is_valid:
        raise STACValidationException()

    # Check geometry is WGS84
    validate_bbox(geometry_shape.bounds)


def get_patch_validator(extension: str) -> Validator:
    """Get JSON schema validator for an extension.

    Args:
        extension (str): Extension URI

    Returns:
        Validator: Validator for extension
    """
    schema = requests.get(extension, timeout=5.0).json()
    # This block is cribbed (w/ change in error handling) from
    # jsonschema.validate
    cls = jsonschema.validators.validator_for(schema)
    cls.check_schema(schema)
    return cls(schema)


def evaluate_patch(
    data: Any,
) -> str:
    """Evaluate Patch payload

    Args:
        data (Any): Patch payload to validate

    Returns:
        role (str): role required for patch type
    """

    for role, validator in PATCH_VALIDATORS.items():
        if validator.is_valid(data):
            return role

    return "UPDATE"


def validate_patch(
    item_id: str,
    item: PartialItem,
    extensions: list[str],
) -> None:
    """Validate a PartialItem patch request

    Args:
        item_id (str): ID of the item to validate
        item (PartialItem): Partial Item to be validated to validate
        extensions (list[str]): List of STAC extensions to be validated against

    Raises:
        STACValidationException: Validation error
        UnexpectedExtensionException: Unexpect exception with validation
    """
    if item.geometry:
        validate_geometry(item.geometry)

    if item.bbox:
        validate_bbox(item.bbox)

    item, null_keys = get_null_keys(item)

    for extension in extensions:
        extension_validator = get_extension_validator(str(extension))

        required_keys = set()
        raise_errors = []
        for error in extension_validator.iter_errors(
            json.loads(item.model_dump_json())
        ):

            if error.validator in ["oneOf"]:
                continue

            elif error.validator == "required":
                required_keys.add(json.dumps(error.validator_value))

            else:
                raise_errors.append(error)

        for null_key_error in required_keys & null_keys:
            raise_errors.append(
                f"Variable {null_key_error} is required and cannot be removed"
            )

        if raise_errors:
            logger.error("STAC validation error: %s", item_id)

            raise STACValidationException()


def validate_post(
    item_id: str,
    item: Item,
    extensions: list[str],
) -> None:
    """Validate a Item post request

    Args:
        item_id (str): ID of the item to validate
        item (Item): Partial Item to be validated to validate
        extensions (list[str]): List of STAC extensions to be validated against

    Raises:
        STACValidationException: Validation error
    """
    validate_geometry(item.geometry)
    validate_bbox(item.bbox)

    for extension in extensions:
        extension_validator = get_extension_validator(str(extension))

        raise_errors = []
        for error in extension_validator.iter_errors(
            json.loads(item.model_dump_json())
        ):
            raise_errors.append(error)

        if raise_errors:
            logger.error("STAC validation error: %s", item_id)

            raise STACValidationException()
