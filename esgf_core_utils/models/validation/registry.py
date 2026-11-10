"""
Retrieval and caching of the JSON schemas used to validate STAC extensions.
"""

import json
from functools import partial

import jsonschema
import requests
from jsonschema.protocols import Validator
from referencing import Registry, Resource
from referencing.exceptions import Unretrievable
from referencing.jsonschema import DRAFT202012
from referencing.retrieval import to_cached_resource

from esgf_core_utils.models.exceptions import ExtensionValidationException


@to_cached_resource(
    from_contents=partial(Resource.from_contents, default_specification=DRAFT202012)
)
def retrieve_schema(uri: str) -> str:
    """Download a JSON schema.

    The result is cached per URI, so each schema (an extension schema or a schema
    it references) is downloaded at most once per process.

    Args:
        uri (str): Schema URI

    Returns:
        str: Serialized JSON schema
    """
    response = requests.get(uri, timeout=5.0)
    response.raise_for_status()
    return response.text


SCHEMA_REGISTRY: Registry = Registry(retrieve=retrieve_schema)  # type: ignore[call-arg]
"""Registry used to resolve extension schemas and the schemas they reference."""


def get_extension_validator(extension: str) -> Validator:
    """Get JSON schema validator for an extension.

    The extension schema, and any schema it references, is retrieved through
    ``SCHEMA_REGISTRY`` and is only downloaded the first time it is needed.

    Args:
        extension (str): Extension URI

    Raises:
        ExtensionValidationException: The schema could not be retrieved or decoded

    Returns:
        Validator: Validator for extension
    """
    try:
        schema = SCHEMA_REGISTRY.get_or_retrieve(extension).value.contents
    except Unretrievable as exc:
        cause = exc.__cause__
        if isinstance(cause, requests.exceptions.HTTPError):
            raise ExtensionValidationException(
                extension=extension,
                detail=f"Error {cause.response.status_code} while getting the extension schema {cause.request.url or extension}",
            ) from cause
        if isinstance(cause, requests.exceptions.RequestException):
            raise ExtensionValidationException(
                extension=extension,
                detail=(
                    f"An error occurred while getting the extension schema {cause.request.url if cause.request is not None else extension}"
                ),
            ) from cause
        if isinstance(cause, json.JSONDecodeError):
            raise ExtensionValidationException(
                extension=extension,
                detail=f"Failed to decode the extension schema {extension}: {cause.msg}. Error occured at line: {cause.lineno}, column: {cause.colno}",
            ) from cause
        raise
    # This block is cribbed (w/ change in error handling) from
    # jsonschema.validate
    cls = jsonschema.validators.validator_for(schema)
    cls.check_schema(schema)
    return cls(schema, registry=SCHEMA_REGISTRY)


def get_patch_validator(extension: str) -> Validator:
    """Get JSON schema validator for an extension.

    Args:
        extension (str): Extension URI

    Returns:
        Validator: Validator for extension
    """
    return get_extension_validator(extension)
