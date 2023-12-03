from typing import Optional, TypedDict
from pydantic import BaseModel


# Tag implication shares the same structure as tag alias
class TagAliasRaw(TypedDict):
    id: int
    antecedent_name: str
    reason: str
    creator_id: int
    consequent_name: str
    status: str
    forum_topic_id: Optional[int]
    # ISO 8601
    created_at: str
    # ISO 8601
    updated_at: str
    approver_id: Optional[int]
    forum_post_id: Optional[int]


class TagAliasEntry(BaseModel):
    id: int
    antecedent_name: str
    consequent_name: str

    @staticmethod
    def from_raw(tag_alias: TagAliasRaw) -> "TagAliasEntry":
        return TagAliasEntry(id=tag_alias["id"],
                             antecedent_name=tag_alias["antecedent_name"],
                             consequent_name=tag_alias["consequent_name"])
