from typing import Optional, TypedDict

class TagAlias(TypedDict):
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


# Tag implication shares the same structure as tag alias
TagImplication = TagAlias
