"""Unit tests for auth models."""

import unittest

from stac_pydantic.shared import Asset

from esgf_core_utils.models.auth import (
    Nodes,
    Permission,
    Projects,
)
from esgf_core_utils.models.exceptions import MissingPermissionException


class TestNodes(unittest.TestCase):
    """Tests for Nodes."""

    def test_add_new_node(self) -> None:
        """Ensure a new node can be added."""
        nodes = Nodes()

        nodes.add(Permission(id="node1", roles={"CREATE"}))

        self.assertEqual(nodes.permissions["node1"].roles, {"CREATE"})

    def test_add_dict(self) -> None:
        """Ensure dictionary node definitions are accepted."""
        nodes = Nodes()

        nodes.add(
            {
                "id": "node1",
                "roles": {"CREATE"},
            }
        )

        self.assertEqual(nodes.permissions["node1"].roles, {"CREATE"})

    def test_add_merges_roles(self) -> None:
        """Ensure duplicate nodes merge roles."""
        nodes = Nodes()

        nodes.add(Permission(id="node1", roles={"CREATE"}))
        nodes.add(Permission(id="node1", roles={"UPDATE"}))

        self.assertEqual(
            nodes.permissions["node1"].roles,
            {"CREATE", "UPDATE"},
        )

    def test_authorize_href_success(self) -> None:
        """Ensure valid node authorization succeeds."""
        nodes = Nodes()
        nodes.add(Permission(id="example.com", roles={"CREATE"}))

        nodes.authorize_href(
            "https://example.com/file.nc",
            "CREATE",
        )

    def test_authorize_href_missing_node(self) -> None:
        """Ensure missing nodes raise an exception."""
        nodes = Nodes()

        with self.assertRaises(MissingPermissionException):
            nodes.authorize_href(
                "https://example.com/file.nc",
                "CREATE",
            )

    def test_authorize_href_missing_role(self) -> None:
        """Ensure missing roles raise an exception."""
        nodes = Nodes()
        nodes.add(Permission(id="example.com", roles={"UPDATE"}))

        with self.assertRaises(MissingPermissionException):
            nodes.authorize_href(
                "https://example.com/file.nc",
                "CREATE",
            )

    def test_authorize_dict_asset(self) -> None:
        """Ensure dictionary assets are authorized."""
        nodes = Nodes()
        nodes.add(Permission(id="example.com", roles={"CREATE"}))

        assets = {
            "asset": {
                "href": "dummy",
                "alternate:name": "example.com",
            }
        }

        nodes.authorize(assets, "CREATE")

    def test_authorize_model_dump_path(self) -> None:
        """Ensure model assets use model_dump()."""
        nodes = Nodes()
        nodes.add(Permission(id="example.com", roles={"CREATE"}))

        assets = {
            "asset": Asset.model_validate(
                {
                    "href": "dummy",
                    "alternate:name": "example.com",
                }
            )
        }

        nodes.authorize(assets, "CREATE")

    def test_authorize_recursive_alternate(self) -> None:
        """Ensure recursive alternates are authorized."""
        nodes = Nodes()
        nodes.add(Permission(id="example.com", roles={"CREATE"}))

        assets = {
            "outer": {
                "alternate": {
                    "inner": {
                        "href": "dummy",
                        "alternate:name": "example.com",
                    }
                }
            }
        }

        nodes.authorize(assets, "CREATE")


class TestProjects(unittest.TestCase):
    """Tests for Projects."""

    def test_add_new_project(self) -> None:
        """Ensure a project can be added."""
        projects = Projects()

        projects.add(
            Permission(
                id="cmip6",
                roles={"CREATE"},
            )
        )

        self.assertEqual(
            projects.permissions["cmip6"].roles,
            {"CREATE"},
        )

    def test_add_project_dict(self) -> None:
        """Ensure dictionary project definitions are accepted."""
        projects = Projects()

        projects.add(
            {
                "id": "cmip6",
                "roles": {"CREATE"},
            }
        )

        self.assertEqual(
            projects.permissions["cmip6"].roles,
            {"CREATE"},
        )

    def test_add_merges_roles(self) -> None:
        """Ensure duplicate projects merge roles."""
        projects = Projects()

        projects.add(Permission(id="cmip6", roles={"CREATE"}))
        projects.add(Permission(id="cmip6", roles={"UPDATE"}))

        self.assertEqual(
            projects.permissions["cmip6"].roles,
            {"CREATE", "UPDATE"},
        )

    def test_authorize_success(self) -> None:
        """Ensure valid project authorization succeeds."""
        projects = Projects()
        projects.add(Permission(id="cmip6", roles={"CREATE"}))

        projects.authorize("cmip6", "CREATE")

    def test_authorize_missing_project(self) -> None:
        """Ensure missing projects raise exceptions."""
        with self.assertRaises(MissingPermissionException):
            Projects().authorize(
                "cmip6",
                "CREATE",
            )

    def test_authorize_missing_role(self) -> None:
        """Ensure missing role assignments raise exceptions."""
        projects = Projects()
        projects.add(Permission(id="cmip6", roles={"UPDATE"}))

        with self.assertRaises(MissingPermissionException):
            projects.authorize(
                "cmip6",
                "CREATE",
            )
