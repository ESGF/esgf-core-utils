class MissingPermissionException(Exception):
    """
    Missing Permission Exception
    """

    def __init__(self, permission_type: str, target: str, role: str = "") -> None:
        self.type = permission_type
        self.role = role
        self.target = target


class OperationNotPermittedException(Exception):
    """
    Operation not permitted Exception
    """

    def __init__(self, op: str) -> None:
        self.status_code = 400
        self.type = "https://esgf.io/publication/errors/operation-not-permitted"
        self.title = "The operation you attempted is not permitted"
        self.detail = (
            f"You attempted to perform an `{op}` operation which is not permitted "
            "-- please ensure your patch operation conform to "
            "https://esgf.io/publication/api/v1/patch and try again."
        )


class UnexpectedExtensionException(Exception):
    """
    Unexpected extenison Exception
    """

    def __init__(self, extension: str) -> None:
        self.status_code = 400
        self.type = "https://esgf.io/publication/errors/unexpected-extension"
        self.title = "There is an unexpected extension in your request"
        self.detail = (
            f"Your request includes an unexpected extension: `{extension}` "
            "-- please remove this extenison and try again."
        )


class ExpectedExtensionsMissingException(Exception):
    """
    Expected extension missing Exception
    """

    def __init__(self, extensions: list[str]) -> None:
        self.status_code = 400
        self.type = "https://esgf.io/publication/errors/expected-extension-missing"
        self.title = "A required extension is missing from your request"
        self.detail = (
            f"Your request is missing required extensions: `[{','.join(extensions)}]` "
            "-- please add this extenison and try again."
        )


class RFC9457Exception(Exception):
    """
    RFC 9457 Exception
    """

    status_code: int
    type: str
    title: str
    detail: str
    instance: str


class STACValidationException(RFC9457Exception):
    """
    STAC validation Exception
    """

    def __init__(self) -> None:
        self.status_code = 400
        self.type = "https://esgf.io/publication/errors/stac-validation"
        self.title = "Your request in invalid"
        self.detail = (
            "Your request is invalid -- please ensure your request is valid "
            "and try again."
        )


class AuthorizationException(RFC9457Exception):
    """
    Authorization Exception
    """

    def __init__(self, instance: str) -> None:
        self.status_code = 403
        self.type = "https://esgf.io/publication/errors/missing-permission"
        self.title = "You do not have permission"
        self.detail = (
            "You do not have the required permission to perform that operation "
            "-- please check with your auth provider and try again."
        )

        self.instance = instance


class ItemAlreadyExistsException(RFC9457Exception):
    """
    Item already exists Exception
    """

    def __init__(self, collection: str, item: str, instance: str) -> None:
        self.status_code = 409
        self.type = "https://esgf.io/publication/errors/item-already-exists"
        self.title = "The Item you attempted to publish already exists"
        self.detail = (
            f"You attempted to publish a new STAC Item with id `{item}`, but an "
            f"item with that id already exists at /collections/{collection}/items/{item} "
            "-- please ensure this is the Item id you intended to publish or retract the "
            "existing item via https://esgf.io/publication/api/v1/retract and try again."
        )

        self.item = item
        self.instance = instance


class ItemDoesNotExistException(RFC9457Exception):
    """
    Item does not exist Exception
    """

    def __init__(self, collection: str, item: str, instance: str) -> None:
        self.status_code = 404
        self.type = "https://esgf.io/publication/errors/item-does-not-exist"
        self.title = "The Item you attempted to update does not exist"
        self.detail = (
            f"You attempted to update a STAC Item with id `{item}`, but an item with "
            f"that id does not exist at /collections/{collection}/items/{item} -- please "
            "ensure the the Item you intended to update has been published and try again."
        )
        self.item = item
        self.instance = instance


class UnknownException(RFC9457Exception):
    """
    Unknown Exception
    """

    def __init__(self, instance: str) -> None:
        self.status_code = 500
        self.type = "https://esgf.io/publication/errors/unknown"
        self.title = "An unidentified server side error occurred"
        self.detail = (
            "Please report this error to help@esgf.io so that we can identify and "
            "correct the problem."
        )
        self.instance = instance
