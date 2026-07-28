from typing import Literal

from stac_fastapi.extensions.transaction.request import (
    PatchAddReplaceTest,
)

type BBox2D = tuple[float | int, float | int, float | int, float | int]
type BBox3D = tuple[
    float | int,
    float | int,
    float | int,
    float | int,
    float | int,
    float | int,
]


class PatchAddReplace(PatchAddReplaceTest):
    op: Literal["add", "replace"]
