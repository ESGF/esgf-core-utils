"""Functional tests for Authorizer."""

import unittest
from unittest.mock import Mock

from esgf_core_utils.models.auth import (
    Authorizer,
    Node,
    Project,
)
from esgf_core_utils.models.exceptions import AuthorizationException
from esgf_core_utils.models.kafka.events import RequesterData


class TestAuthorizer(unittest.TestCase):
    """Functional tests for Authorizer."""

    def setUp(self) -> None:
        """Create common test fixtures."""
        self.regex = r"(?P<type>project|node):" r"(?P<id>[^:]+):" r"(?P<role>[A-Z]+)"

        self.requester_data = RequesterData.model_construct(
            client_id="test_client", iss="test_iss", sub="test_sub"
        )

    def test_add_project_entitlement(self) -> None:
        """Ensure project entitlements are parsed."""
        authorizer = Authorizer(
            requester_data=self.requester_data,
            regex=self.regex,
        )

        authorizer.add(
            [
                "project:cmip6:CREATE",
            ]
        )

        self.assertIn(
            "cmip6",
            authorizer.projects.projects,
        )

    def test_add_node_entitlement(self) -> None:
        """Ensure node entitlements are parsed."""
        authorizer = Authorizer(
            requester_data=self.requester_data,
            regex=self.regex,
        )

        authorizer.add(
            [
                "node:example.com:CREATE",
            ]
        )

        self.assertIn(
            "example.com",
            authorizer.nodes.nodes,
        )

    def test_add_ignores_unmatched_entitlement(self) -> None:
        """Ensure invalid entitlements are ignored."""
        authorizer = Authorizer(
            requester_data=self.requester_data,
            regex=self.regex,
        )

        authorizer.add(["invalid"])

        self.assertEqual(
            authorizer.projects.projects,
            {},
        )
        self.assertEqual(
            authorizer.nodes.nodes,
            {},
        )

    def test_add_skips_validation_error(self) -> None:
        """Ensure invalid roles are skipped."""
        authorizer = Authorizer(
            requester_data=self.requester_data,
            regex=self.regex,
        )

        authorizer.add(
            [
                "project:cmip6:INVALID_ROLE",
            ]
        )

        self.assertEqual(
            authorizer.projects.projects,
            {},
        )

    def test_authorize_success(self) -> None:
        """Ensure authorization succeeds with valid permissions."""
        authorizer = Authorizer(
            requester_data=self.requester_data,
            regex=self.regex,
        )

        authorizer.projects.add(
            Project(
                id="cmip6",
                roles={"CREATE"},
            )
        )

        authorizer.nodes.add(
            Node(
                id="example.com",
                roles={"CREATE"},
            )
        )

        item = Mock()
        item.assets = {
            "asset": {
                "href": "ignored",
                "alternate:name": "example.com",
            }
        }

        authorizer.authorize(
            collection_id="cmip6",
            item=item,
            role="CREATE",
            request_id="request-id",
            event_id="event-id",
        )

    def test_authorize_raises_authorization_exception(self) -> None:
        """Ensure permission failures are wrapped correctly."""
        authorizer = Authorizer(
            requester_data=self.requester_data,
            regex=self.regex,
        )

        item = Mock()
        item.assets = {}

        with self.assertRaises(AuthorizationException):
            authorizer.authorize(
                collection_id="cmip6",
                item=item,
                role="CREATE",
                request_id="request-id",
                event_id="event-id",
            )
