from typing import Dict, List, Tuple, Optional, Literal, TypedDict, Any
from datetime import datetime
from pydantic import BaseModel


class ArtistsRaw(TypedDict):
    id: int
    created_at: str
    name: str
    updated_at: str
    is_deleted: bool
    group_name: Optional[str]
    is_banned: bool
    other_names: List[str]


class ArtistEntry(BaseModel):
    id: int
    created_at: datetime
    name: str
    updated_at: datetime
    group_name: Optional[str]
    is_deleted: bool
    is_banned: bool

    @staticmethod
    def from_raw(artists: ArtistsRaw) -> "ArtistEntry":
        return ArtistEntry(id=artists["id"],
                           created_at=datetime.fromisoformat(artists["created_at"]),
                           name=artists["name"],
                           updated_at=datetime.fromisoformat(artists["updated_at"]),
                           group_name=artists["group_name"],
                           is_deleted=artists["is_deleted"],
                           is_banned=artists["is_banned"])
