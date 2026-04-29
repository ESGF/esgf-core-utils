"""
Models relating to Authorisation for the ESGF Next Gen Core Architecture.
"""

import logging
import re
from typing import Any, Literal
from urllib.parse import urlparse

from pydantic import BaseModel
from pydantic_core import ValidationError
from stac_fastapi.extensions.core.transaction.request import PartialItem
from stac_pydantic.item import Item

from esgf_core_utils.models.exceptions import (
    AuthorizationException,
    MissingPermissionException,
)
from esgf_core_utils.models.kafka.events import RequesterData

logger = logging.getLogger("EGI_AUTH")

Role = Literal[
    "CREATE",
    "UPDATE",
    "DELETE",
    "REPLICATE",
    "REVOKE",
]


class Node(BaseModel):
    """
    Model describing Node auth info of a ESGF publisher.
    """

    id: str
    roles: set[Role]


class Project(BaseModel):
    """
    Model describing Project auth info of a ESGF publisher.
    """

    id: str
    roles: set[Role]


class Nodes(BaseModel):
    """
    Model describing Project auth info of a ESGF publisher.
    """

    nodes: dict[str, Node] = {}

    def add(self, node: Node | dict[str, Any]) -> None:
        """
        Add a new project or update roles if project already exists.

        Args:
            node (Node | dict): node to be added
        """
        if isinstance(node, dict):
            node = Node(**node)

        if existing_node := self.nodes.get(node.id):
            existing_node.roles.update(node.roles)

        else:
            self.nodes[node.id] = node

    def authorize_href(self, asset_href: str, role: Role) -> None:
        asset_url = urlparse(asset_href)
        node_permission = self.nodes.get(asset_url.hostname or "", None)

        if not node_permission:
            raise MissingPermissionException(
                permission_type="node",
                target=asset_href,
            )

        if role not in node_permission.roles:
            raise MissingPermissionException(
                permission_type="node",
                role=role,
                target=asset_href,
            )

    def authorize(self, assets: dict[str, Any], role: Role) -> None:
        """Check for appropriate authorisation.

        Args:
            assets (dict): item to be authorised
            role (Role): required role for auhroisation

        Raises:
            MissingPermissionException: Raised if either node or role permission is missing
        """

        for asset in assets.values():
            asset = asset.model_dump() if not isinstance(asset, dict) else asset

            if "href" in asset:
                self.authorize_href(f"https://{asset.get("alternate:name")}", role)

            if alternates := asset.get("alternate"):
                self.authorize(alternates, role)


class Projects(BaseModel):
    """
    Model describing Project auth info of a ESGF publisher.
    """

    projects: dict[str, Project] = {}

    def add(self, project: Project | dict[str, Any]) -> None:
        """
        Add a new project or update roles if project already exists.

        Args:
            project (Project | dict): project to be added
        """
        if isinstance(project, dict):
            project = Project(**project)

        if existing_project := self.projects.get(project.id):
            existing_project.roles.update(project.roles)

        else:
            self.projects[project.id] = project

    def authorize(self, project: str, role: Role) -> None:
        """Check for appropriate authorisation.

        Args:
            item (Item): item to be authorised
            role (Role): required role for auhroisation

        Raises:
            MissingPermissionException: Raised if either node or role permission is missing
        """
        project_permission = self.projects.get(project, None)

        if not project_permission:
            raise MissingPermissionException(
                permission_type="project",
                target=project,
            )

        if role not in project_permission.roles:
            raise MissingPermissionException(
                permission_type="project",
                role=role,
                target=project,
            )


class EGIAuth(BaseModel):
    """
    Model describing Authentication information of a ESGF publisher.
    """

    requester_data: RequesterData
    nodes: Nodes = Nodes()
    projects: Projects = Projects()
    regex: str

    def authorize(
        self,
        collection_id: str,
        item: Item | PartialItem,
        role: Role,
        request_id: str,
        event_id: str,
    ) -> None:
        """Check for appropriate authorisation.

        Args:
            collection_id: collection id of request
            item (Item): item to be authorised
            role (Role): required role for auhroisation

        Raises:
            AuthorizationException: Raised if either node or role permission is missing
        """
        try:
            self.projects.authorize(collection_id, role)
            self.nodes.authorize(item.assets or {}, role)

        except MissingPermissionException as exc:
            raise AuthorizationException(instance=f"{request_id}:{event_id}") from exc

    def add(self, entitlements: list[str]) -> None:
        """add entitlements to Authorizer.

        Args:
            entitlements (list[str]): list of entitlements to be added
        """
        for entitlement in entitlements:
            match = re.search(self.regex, entitlement)

            if match is None:
                continue

            try:
                if match.group("type") == "project":
                    self.projects.add(
                        Project(
                            id=match.group("id"),
                            roles=[match.group("role")],
                        )
                    )

                elif match.group("type") == "node":
                    self.nodes.add(
                        Node(
                            id=match.group("id"),
                            roles=[match.group("role")],
                        )
                    )

            except ValidationError:
                logger.info("Entitlement skipped: %s", entitlement)
