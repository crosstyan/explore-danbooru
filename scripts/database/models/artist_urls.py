from typing import Dict, List, Tuple, Optional, Literal, TypedDict, Any
from datetime import datetime
from pydantic import BaseModel


class ArtistUrlsRaw(TypedDict):
    id: int
    artist_id: int
    url: str
    is_active: bool
    created_at: str
    updated_at: str


class ArtistUrlEntry(BaseModel):
    id: int
    artist_id: int
    url: str
    is_active: bool = True
    created_at: Optional[datetime]
    updated_at: Optional[datetime]

    @staticmethod
    def from_raw(artist_urls: ArtistUrlsRaw) -> "ArtistUrlEntry":
        def _get_date(date: Optional[str]) -> Optional[datetime]:
            return datetime.fromisoformat(date) if date else None

        return ArtistUrlEntry(
            id=artist_urls.get("id"),
            artist_id=artist_urls.get("artist_id"),
            url=artist_urls.get("url"),
            is_active=artist_urls.get("is_active"),
            created_at=_get_date(artist_urls.get("created_at")),
            updated_at=_get_date(artist_urls.get("updated_at")),
        )
