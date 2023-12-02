from typing import TypedDict, List
from pydantic import BaseModel


class TagRaw(TypedDict):
    id: int
    name: str
    post_count: int
    category: int
    created_at: str
    updated_at: str
    is_deprecated: bool
    words: List[str]


class TagEntry(BaseModel):
    tag_id: int
    name: str
    category: int
    is_deprecated: bool


def tag_raw_to_entry(tag: TagRaw) -> TagEntry:
    return TagEntry(tag_id=tag["id"],
                    name=tag["name"],
                    category=tag["category"],
                    is_deprecated=tag["is_deprecated"])
