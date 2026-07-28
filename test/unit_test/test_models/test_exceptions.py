"""
Unit tests for the exception classes defined in exceptions.py.

These tests verify that each exception class correctly sets its internal
attributes during instantiation, without asserting runtime behaviour.
"""

import unittest

from esgf_core_utils.models.exceptions import (
    AuthorizationException,
    ExpectedExtensionsMissingException,
    ExtensionBelowMinimumException,
    InvalidTokenAudienceException,
    ItemAlreadyExistsException,
    ItemDoesNotExistException,
    MissingPermissionException,
    OperationNotPermittedException,
    RFC9457Exception,
    STACValidationException,
    UnexpectedExtensionException,
    UnknownException,
)


class DummyRFC9457Exception(RFC9457Exception):
    """Concrete RFC9457Exception for base-class testing."""

    status_code = 418
    type = "dummy-type"
    title = "dummy-title"

    @property
    def detail(self) -> str:
        """Return test detail."""
        return "dummy-detail"


class TestExceptionUnit(unittest.TestCase):
    """Unit test suite for validating exception initialisation."""

    def test_missing_permission_exception(self) -> None:
        """Ensure MissingPermissionException stores type, target, and role."""
        exc = MissingPermissionException("project", "target", "roleX")

        self.assertEqual(exc.type, "project")
        self.assertEqual(exc.target, "target")
        self.assertEqual(exc.role, "roleX")

    def test_operation_not_permitted_exception(self) -> None:
        """Validate OperationNotPermittedException fields and dynamic detail text."""
        exc = OperationNotPermittedException("PATCH")

        self.assertEqual(exc.status_code, 400)
        self.assertEqual(
            exc.type,
            "https://esgf.io/publication/errors/operation-not-permitted",
        )
        self.assertEqual(
            exc.title,
            "The operation you attempted is not permitted",
        )
        self.assertIn("PATCH", exc.detail)

    def test_unexpected_extension_exception(self) -> None:
        """Validate UnexpectedExtensionException fields and detail message."""
        exc = UnexpectedExtensionException("weird-ext")

        self.assertEqual(exc.status_code, 400)
        self.assertEqual(
            exc.type,
            "https://esgf.io/publication/errors/unexpected-extension",
        )
        self.assertEqual(
            exc.title,
            "There is an unexpected extension in your request",
        )
        self.assertIn("weird-ext", exc.detail)

    def test_expected_extensions_missing_exception(self) -> None:
        """Verify ExpectedExtensionsMissingException joins extension names correctly."""
        exc = ExpectedExtensionsMissingException(["extA", "extB"])

        self.assertEqual(exc.status_code, 400)
        self.assertEqual(
            exc.type,
            "https://esgf.io/publication/errors/expected-extension-missing",
        )
        self.assertEqual(
            exc.title,
            "A required extension is missing from your request",
        )
        self.assertIn("[extA,extB]", exc.detail.replace(" ", ""))

    def test_extension_below_minimum_exception(self) -> None:
        """Validate ExtensionBelowMinimumException fields and detail message."""
        exc = ExtensionBelowMinimumException("old-ext", "v1.0.0")

        self.assertEqual(exc.status_code, 400)
        self.assertEqual(
            exc.type,
            "https://esgf.io/publication/errors/extension-below-minimum",
        )
        self.assertEqual(
            exc.title,
            "There is an extension in your request below the minimum allowed version",
        )
        self.assertIn("old-ext", exc.detail)
        self.assertIn("v1.0.0", exc.detail)

    def test_invalid_token_audience_exception(self) -> None:
        """Validate InvalidTokenAudienceException fields and detail message."""
        exc = InvalidTokenAudienceException("token-aud", "exepected-aud")

        self.assertEqual(exc.status_code, 401)
        self.assertEqual(
            exc.type,
            "https://esgf.io/publication/errors/invalid-token-audience",
        )
        self.assertEqual(
            exc.title,
            "Invalid token audience",
        )
        self.assertIn("token-aud", exc.detail)
        self.assertIn("exepected-aud", exc.detail)

    def test_stac_validation_exception(self) -> None:
        """Verify STACValidationException default metadata."""
        exc = STACValidationException()

        self.assertEqual(exc.status_code, 400)
        self.assertEqual(
            exc.type,
            "https://esgf.io/publication/errors/stac-validation",
        )
        self.assertEqual(exc.title, "Your request is invalid")
        self.assertIn("invalid", exc.detail)

    def test_authorization_exception(self) -> None:
        """Ensure AuthorizationException includes instance and permission metadata."""
        exc = AuthorizationException(instance="instance-123")

        self.assertEqual(exc.status_code, 403)
        self.assertEqual(
            exc.type,
            "https://esgf.io/publication/errors/missing-permission",
        )
        self.assertEqual(exc.title, "You do not have permission")
        self.assertEqual(exc.instance, "instance-123")
        self.assertIn("required permission", exc.detail)

    def test_item_already_exists_exception(self) -> None:
        """Validate ItemAlreadyExistsException stores item, collection, and instance."""
        exc = ItemAlreadyExistsException("colA", "itemX", instance="inst99")

        self.assertEqual(exc.status_code, 409)
        self.assertEqual(
            exc.type,
            "https://esgf.io/publication/errors/item-already-exists",
        )
        self.assertEqual(
            exc.title,
            "The Item you attempted to publish already exists",
        )
        self.assertEqual(exc.item, "itemX")
        self.assertEqual(exc.instance, "inst99")
        self.assertIn("itemX", exc.detail)
        self.assertIn("colA", exc.detail)

    def test_item_does_not_exist_exception(self) -> None:
        """Validate ItemDoesNotExistException fields and instance metadata."""
        exc = ItemDoesNotExistException("colA", "itemX", instance="inst55")

        self.assertEqual(exc.status_code, 404)
        self.assertEqual(
            exc.type,
            "https://esgf.io/publication/errors/item-does-not-exist",
        )
        self.assertEqual(
            exc.title,
            "The Item you attempted to update does not exist",
        )
        self.assertEqual(exc.item, "itemX")
        self.assertEqual(exc.instance, "inst55")
        self.assertIn("itemX", exc.detail)

    def test_unknown_exception(self) -> None:
        """Verify UnknownException includes correct status, title, and instance."""
        exc = UnknownException(instance="instXYZ")

        self.assertEqual(exc.status_code, 500)
        self.assertEqual(
            exc.type,
            "https://esgf.io/publication/errors/unknown",
        )
        self.assertEqual(
            exc.title,
            "An unidentified server side error occurred",
        )
        self.assertEqual(exc.instance, "instXYZ")
        self.assertIn("help@esgf.io", exc.detail)

    def test_missing_permission_exception_str(self) -> None:
        """Ensure MissingPermissionException string formatting."""
        exc = MissingPermissionException(
            "project",
            "cmip6",
            "CREATE",
        )

        self.assertEqual(
            str(exc),
            "Missing permission 'project' for 'cmip6'.",
        )

    def test_rfc9457_status_property(self) -> None:
        """Ensure status property returns status_code."""
        exc = DummyRFC9457Exception()

        self.assertEqual(exc.status, 418)

    def test_rfc9457_str_returns_detail(self) -> None:
        """Ensure string conversion returns detail."""
        exc = DummyRFC9457Exception()

        self.assertEqual(str(exc), "dummy-detail")

    def test_rfc945response(self) -> None:
        """Ensure RFC9457 response payload is generated."""
        exc = DummyRFC9457Exception(
            instance="test-instance",
        )

        self.assertEqual(
            exc.rfc945response(),
            {
                "type": "dummy-type",
                "title": "dummy-title",
                "status": 418,
                "detail": "dummy-detail",
                "instance": "test-instance",
            },
        )

    def test_rfc9457_base_detail_not_implemented(self) -> None:
        """Ensure base detail property raises NotImplementedError."""

        class BaseTestException(RFC9457Exception):
            status_code = 500
            type = "test"
            title = "test"

        with self.assertRaises(NotImplementedError):
            BaseTestException().detail
