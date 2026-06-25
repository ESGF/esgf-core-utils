"""
Exception used by ESGF packages.
"""

from dataclasses import dataclass
from typing import ClassVar, TypedDict


class RFC9457Response(TypedDict):
    """
    RFC 9457 Response.
    """

    type: str
    title: str
    status: int
    detail: str
    instance: str


@dataclass(slots=True)
class MissingPermissionException(Exception):
    """
    Raised when a user does not possess a required permission.
    """

    permission_type: str
    target: str
    role: str

    def __str__(self) -> str:
        """Return a human-readable error message."""
        return f"Missing permission '{self.permission_type}' " f"for '{self.target}'."


@dataclass(slots=True)
class RFC9457Exception(Exception):
    """
    Base class for RFC 9457 Exceptions.
    """

    instance: str

    status_code: ClassVar[int]
    type: ClassVar[str]
    title: ClassVar[str]

    @property
    def detail(self) -> str:
        """
        Details of exception.
        """
        raise NotImplementedError

    @property
    def status(self) -> int:
        """
        HTTP status code.
        """
        return self.status_code

    def __str__(self) -> str:
        """
        Exception message.
        """
        return self.detail

    def rfc945response(self) -> RFC9457Response:
        """
        RFC9457 Response.
        """
        return {
            "type": self.type,
            "title": self.title,
            "status": self.status_code,
            "detail": self.detail,
            "instance": self.instance,
        }


@dataclass(slots=True)
class OperationNotPermittedException(RFC9457Exception):
    """
    Raised when a requested patch operation is not permitted.
    """

    op: str

    status_code: ClassVar[int] = 400
    type: ClassVar[str] = "https://esgf.io/publication/errors/operation-not-permitted"
    title: ClassVar[str] = "The operation you attempted is not permitted"

    @property
    def detail(self) -> str:
        return (
            f"You attempted to perform an `{self.op}` operation "
            "which is not permitted -- please ensure your patch "
            "operation conforms to "
            "https://esgf.io/publication/api/v1/patch and try again."
        )


@dataclass(slots=True)
class UnexpectedExtensionException(RFC9457Exception):
    """
    Raised when an unexpected extension is supplied.
    """

    extension: str

    status_code: ClassVar[int] = 400
    type: ClassVar[str] = "https://esgf.io/publication/errors/unexpected-extension"
    title: ClassVar[str] = "There is an unexpected extension in your request"

    @property
    def detail(self) -> str:
        """Return the problem description."""
        return (
            f"Your request includes an unexpected extension: "
            f"`{self.extension}` -- please remove this extension "
            "and try again."
        )


@dataclass(slots=True)
class ExpectedExtensionsMissingException(RFC9457Exception):
    """
    Raised when one or more required extensions are missing.
    """

    extensions: list[str]

    status_code: ClassVar[int] = 400
    type: ClassVar[str] = (
        "https://esgf.io/publication/errors/expected-extension-missing"
    )
    title: ClassVar[str] = "A required extension is missing from your request"

    @property
    def detail(self) -> str:
        return (
            "Your request is missing required extensions: "
            f"`[{','.join(self.extensions)}]` "
            "-- please add this extension and try again."
        )


@dataclass(slots=True)
class ExtensionBelowMinimumException(RFC9457Exception):
    """
    Raised when an extension version is below the minimum allowed version.
    """

    extension: str
    minimum_version: str

    status_code: ClassVar[int] = 400
    type: ClassVar[str] = "https://esgf.io/publication/errors/extension-below-minimum"
    title: ClassVar[str] = (
        "There is an extension in your request below the minimum " "allowed version"
    )

    @property
    def detail(self) -> str:
        return (
            f"Your request includes an extension: "
            f"`{self.extension}` below the minimum allowed version "
            f"`{self.minimum_version}` -- please update the extension "
            "version and try again."
        )


@dataclass(slots=True)
class InvalidTokenAudienceException(RFC9457Exception):
    """
    Raised when an OAuth token was issued for the wrong audience.
    """

    token_audience: str
    expected_audience: str

    status_code: ClassVar[int] = 401
    type: ClassVar[str] = "https://esgf.io/publication/errors/invalid-token-audience"
    title: ClassVar[str] = "Invalid token audience"

    @property
    def detail(self) -> str:
        return (
            f"The access token was issued for audience: "
            f"`{self.token_audience}` but this resource "
            f"expects audience: {self.expected_audience}."
        )


@dataclass(slots=True)
class STACValidationException(RFC9457Exception):
    """
    Raised when a STAC document fails validation.
    """

    status_code: ClassVar[int] = 400
    type: ClassVar[str] = "https://esgf.io/publication/errors/stac-validation"
    title: ClassVar[str] = "Your request is invalid"

    @property
    def detail(self) -> str:
        return (
            "Your request is invalid -- please ensure your "
            "request is valid and try again."
        )


@dataclass(slots=True)
class AuthorizationException(RFC9457Exception):
    """
    Raised when the caller is not authorised to perform an operation.
    """

    status_code: ClassVar[int] = 403
    type: ClassVar[str] = "https://esgf.io/publication/errors/missing-permission"
    title: ClassVar[str] = "You do not have permission"

    @property
    def detail(self) -> str:
        return (
            "You do not have the required permission to perform "
            "that operation -- please check with your auth provider "
            "and try again."
        )


@dataclass(slots=True)
class ItemAlreadyExistsException(RFC9457Exception):
    """
    Raised when attempting to create an item that already exists.
    """

    collection: str
    item: str

    status_code: ClassVar[int] = 409
    type: ClassVar[str] = "https://esgf.io/publication/errors/item-already-exists"
    title: ClassVar[str] = "The Item you attempted to publish already exists"

    @property
    def detail(self) -> str:
        return (
            f"You attempted to publish a new STAC Item with id "
            f"`{self.item}`, but an item with that id already exists "
            f"at /collections/{self.collection}/items/{self.item}."
        )


@dataclass(slots=True)
class ItemDoesNotExistException(RFC9457Exception):
    """
    Raised when attempting to update an item that does not exist.
    """

    collection: str
    item: str

    status_code: ClassVar[int] = 404
    type: ClassVar[str] = "https://esgf.io/publication/errors/item-does-not-exist"
    title: ClassVar[str] = "The Item you attempted to update does not exist"

    @property
    def detail(self) -> str:
        return (
            f"You attempted to update a STAC Item with id "
            f"`{self.item}`, but an item with that id does not exist "
            f"at /collections/{self.collection}/items/{self.item}."
        )


@dataclass(slots=True)
class UnknownException(RFC9457Exception):
    """
    Raised when an unexpected internal server error occurs.
    """

    status_code: ClassVar[int] = 500
    type: ClassVar[str] = "https://esgf.io/publication/errors/unknown"
    title: ClassVar[str] = "An unidentified server side error occurred"

    @property
    def detail(self) -> str:
        return (
            "Please report this error to help@esgf.io so that "
            "we can identify and correct the problem."
        )
