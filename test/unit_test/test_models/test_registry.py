"""Unit tests for the JSON schema registry."""

import json
import unittest
from unittest.mock import Mock, patch

from referencing.exceptions import Unretrievable

from esgf_core_utils.models.exceptions import ExtensionValidationException
from esgf_core_utils.models.validation.registry import (
    get_extension_validator,
    get_patch_validator,
    retrieve_schema,
)


class TestSchemaValidators(unittest.TestCase):
    """Tests for schema validator helpers."""

    def setUp(self) -> None:
        """Schemas are cached per URI, so start every test with an empty cache."""
        retrieve_schema.cache_clear()  # type: ignore[attr-defined]

    @patch(
        "esgf_core_utils.models.validation.registry.jsonschema.validators.validator_for"
    )
    @patch("esgf_core_utils.models.validation.registry.requests.get")
    def test_get_extension_validator(
        self,
        mock_get: Mock,
        mock_validator_for: Mock,
    ) -> None:
        """Ensure extension validators are created."""
        schema = {"type": "object"}

        response = Mock()
        response.text = json.dumps(schema)
        mock_get.return_value = response

        validator_cls = Mock()
        validator = Mock()

        validator_cls.return_value = validator
        mock_validator_for.return_value = validator_cls

        result = get_extension_validator("http://example/schema.json")

        self.assertEqual(
            result,
            validator,
        )
        validator_cls.check_schema.assert_called_once_with(
            schema,
        )

    @patch(
        "esgf_core_utils.models.validation.registry.jsonschema.validators.validator_for"
    )
    @patch("esgf_core_utils.models.validation.registry.requests.get")
    def test_get_patch_validator(
        self,
        mock_get: Mock,
        mock_validator_for: Mock,
    ) -> None:
        """Ensure patch validators are created."""
        schema = {"type": "object"}

        response = Mock()
        response.text = json.dumps(schema)
        mock_get.return_value = response

        validator_cls = Mock()
        validator = Mock()

        validator_cls.return_value = validator
        mock_validator_for.return_value = validator_cls

        result = get_patch_validator("http://example/schema.json")

        self.assertEqual(
            result,
            validator,
        )
        validator_cls.check_schema.assert_called_once_with(
            schema,
        )

    @patch("esgf_core_utils.models.validation.registry.requests.get")
    def test_schemas_are_retrieved_only_once(self, mock_get: Mock) -> None:
        """An extension schema and the schema it references are downloaded once."""
        schemas = {
            "http://example/ext.json": {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "properties": {"name": {"$ref": "http://example/name.json"}},
            },
            "http://example/name.json": {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "string",
            },
        }

        def fake_get(uri: str, timeout: float) -> Mock:
            response = Mock()
            response.text = json.dumps(schemas[uri])
            return response

        mock_get.side_effect = fake_get

        for _ in range(3):
            validator = get_extension_validator("http://example/ext.json")
            self.assertTrue(validator.is_valid({"properties": {}, "name": "ok"}))
            self.assertFalse(validator.is_valid({"name": 1}))

        self.assertEqual(
            sorted(call.args[0] for call in mock_get.call_args_list),
            ["http://example/ext.json", "http://example/name.json"],
        )

    @patch("esgf_core_utils.models.validation.registry.requests.get")
    def test_get_extension_validator_unexpected_error(self, mock_get: Mock) -> None:
        """Ensure unexpected retrieval errors are not hidden."""
        mock_get.side_effect = RuntimeError("boom")

        with self.assertRaises(Unretrievable):
            get_extension_validator("http://example/schema.json")

    @patch("esgf_core_utils.models.validation.registry.requests.get")
    def test_get_extension_validator_http_error(
        self,
        mock_get: Mock,
    ) -> None:
        """Ensure HTTP errors raise."""
        import requests

        response = Mock()
        response.status_code = 404

        request = Mock()
        request.url = "http://example/schema.json"

        error = requests.exceptions.HTTPError(
            response=response,
            request=request,
        )

        mock_response = Mock()
        mock_response.raise_for_status.side_effect = error
        mock_get.return_value = mock_response

        with self.assertRaises(ExtensionValidationException):
            get_extension_validator("http://example/schema.json")

    @patch("esgf_core_utils.models.validation.registry.requests.get")
    def test_get_extension_validator_request_error(
        self,
        mock_get: Mock,
    ) -> None:
        """Ensure request errors raise."""
        import requests

        request = Mock()
        request.url = "http://example/schema.json"

        mock_get.side_effect = requests.exceptions.RequestException(
            request=request,
        )

        with self.assertRaises(ExtensionValidationException):
            get_extension_validator("http://example/schema.json")

    @patch("esgf_core_utils.models.validation.registry.requests.get")
    def test_get_extension_validator_json_decode_error(
        self,
        mock_get: Mock,
    ) -> None:
        """Ensure invalid JSON raises."""
        response = Mock()
        response.raise_for_status.return_value = None
        response.text = "not json"

        mock_get.return_value = response

        with self.assertRaises(ExtensionValidationException):
            get_extension_validator("http://example/schema.json")
