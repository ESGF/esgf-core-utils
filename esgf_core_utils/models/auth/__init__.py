"""
Models relating to Authorisation for the ESGF Next Gen Core Architecture.
"""

import logging
import re
from typing import Any, Literal
from urllib.parse import urlparse

from pydantic import BaseModel
from pydantic_core import ValidationError
from stac_fastapi.extensions.transaction.request import PartialItem
from stac_pydantic.item import Item

from esgf_core_utils.models.exceptions import (
    AuthorizationException,
    MissingPermissionException,
)
from esgf_core_utils.models.kafka.events import RequesterData

logger = logging.getLogger("uvicorn.error")

Role = Literal[
    "CITATION",
    "CREATE",
    "DELETE",
    "ERRATA",
    "UPDATE",
    "REPLICATE",
    "RETRACT",
    "REVOKE",
]


class Permission(BaseModel):
    """
    Model describing Permission auth info of a ESGF publisher.
    """

    id: str
    roles: set[Role]


class PermissionStore(BaseModel):
    """
    Model describing Permission store of a ESGF publisher.
    """

    permissions: dict[str, Permission] = {}

    def add(self, permission: Permission | dict[str, Any]) -> None:
        """
        Add a new project or update roles if project already exists.

        Args:
            project (Permission | dict): project to be added
        """
        if isinstance(permission, dict):
            permission = Permission(**permission)

        if existing_permission := self.permissions.get(permission.id):
            existing_permission.roles.update(permission.roles)

        else:
            self.permissions[permission.id] = permission


class Nodes(PermissionStore):
    """
    Model describing Nodes auth info of a ESGF publisher.
    """

    def authorize_href(self, asset_href: str, role: Role) -> None:
        """Authorize an assets href

        Args:
            asset_href (str): href of asset to authorized
            role (Role): role to be checked

        Raises:
            MissingPermissionException: Permissions is missing
        """
        asset_url = urlparse(asset_href)
        permission = self.permissions.get(
            asset_url.hostname or ""
        ) or self.permissions.get("*")

        if not permission:
            raise MissingPermissionException(
                type="node",
                target=asset_href,
            )

        if role not in permission.roles:
            raise MissingPermissionException(
                type="node",
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


class Projects(PermissionStore):
    """
    Model describing Project auth info of a ESGF publisher.
    """

    def authorize(self, project: str, role: Role) -> None:
        """Check for appropriate authorisation.

        Args:
            item (Item): item to be authorised
            role (Role): required role for auhroisation

        Raises:
            MissingPermissionException: Raised if either node or role permission is missing
        """
        permission = self.permissions.get(project) or self.permissions.get("*")

        if not permission:
            raise MissingPermissionException(
                type="project",
                target=project,
            )

        if role not in permission.roles:
            raise MissingPermissionException(
                type="project",
                role=role,
                target=project,
            )


class Authorizer(BaseModel):
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
                logger.info("Entitlement skipped: %s : match not found", entitlement)
                continue

            try:
                if match.group("type") == "project":
                    self.projects.add(
                        Permission(
                            id=match.group("id"),
                            roles=[match.group("role")],
                        )
                    )

                elif match.group("type") == "node":
                    self.nodes.add(
                        Permission(
                            id=match.group("id"),
                            roles=[match.group("role")],
                        )
                    )

            except ValidationError:
                logger.info("Entitlement skipped: %s : validation failed", entitlement)
