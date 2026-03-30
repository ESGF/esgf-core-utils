"""
Functional tests for verifying behaviour of the exception classes when they
are raised and caught at runtime.

These tests ensure that exception instances propagate correctly and contain
the expected metadata inside the context of real raises.
"""

import unittest

from esgf_core_utils.models.exceptions import (
    AuthorizationException,
    ExpectedExtensionsMissingException,
    ItemAlreadyExistsException,
    ItemDoesNotExistException,
    MissingPermissionException,
    OperationNotPermittedException,
    STACValidationException,
    UnexpectedExtensionException,
    UnknownException,
)


class TestExceptionFunctional(unittest.TestCase):
    """Functional test suite for validating exception raising behaviour."""

    def test_raise_missing_permission_exception(self) -> None:
        """Assert MissingPermissionException is raised and attributes preserved."""
        with self.assertRaises(MissingPermissionException) as ctx:
            raise MissingPermissionException("project", "dataset", "admin")

        exc = ctx.exception
        self.assertEqual(exc.type, "project")
        self.assertEqual(exc.target, "dataset")
        self.assertEqual(exc.role, "admin")

    def test_raise_operation_not_permitted_exception(self) -> None:
        """Confirm OperationNotPermittedException raises and detail includes op."""
        with self.assertRaises(OperationNotPermittedException) as ctx:
            raise OperationNotPermittedException("PATCH")

        exc = ctx.exception
        self.assertEqual(exc.status_code, 400)
        self.assertIn("PATCH", exc.detail)

    def test_raise_unexpected_extension_exception(self) -> None:
        """Ensure UnexpectedExtensionException raises normally."""
        with self.assertRaises(UnexpectedExtensionException):
            raise UnexpectedExtensionException("bad-ext")

    def test_raise_expected_extensions_missing_exception(self) -> None:
        """Verify ExpectedExtensionsMissingException formats extension list."""
        with self.assertRaises(ExpectedExtensionsMissingException) as ctx:
            raise ExpectedExtensionsMissingException(["a", "b"])

        self.assertIn("[a,b]", ctx.exception.detail.replace(" ", ""))

    def test_raise_stac_validation_exception(self) -> None:
        """Ensure STACValidationException raises without extra parameters."""
        with self.assertRaises(STACValidationException):
            raise STACValidationException()

    def test_raise_authorization_exception(self) -> None:
        """Confirm AuthorizationException stores the instance field correctly."""
        with self.assertRaises(AuthorizationException) as ctx:
            raise AuthorizationException("instA")

        self.assertEqual(ctx.exception.instance, "instA")

    def test_raise_item_already_exists_exception(self) -> None:
        """Verify ItemAlreadyExistsException sets item and instance when raised."""
        with self.assertRaises(ItemAlreadyExistsException) as ctx:
            raise ItemAlreadyExistsException("colA", "itemX", "inst1")

        exc = ctx.exception
        self.assertEqual(exc.item, "itemX")
        self.assertEqual(exc.instance, "inst1")

    def test_raise_item_does_not_exist_exception(self) -> None:
        """Test that ItemDoesNotExistException captures missing item metadata."""
        with self.assertRaises(ItemDoesNotExistException) as ctx:
            raise ItemDoesNotExistException("colZ", "itemY", "instB")

        exc = ctx.exception
        self.assertEqual(exc.item, "itemY")
        self.assertEqual(exc.instance, "instB")

    def test_raise_unknown_exception(self) -> None:
        """Ensure UnknownException raises and contains instance and detail fields."""
        with self.assertRaises(UnknownException) as ctx:
            raise UnknownException("foo")

        exc = ctx.exception
        self.assertEqual(exc.status_code, 500)
        self.assertEqual(exc.instance, "foo")


if __name__ == "__main__":
    unittest.main()
