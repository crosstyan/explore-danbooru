from typing import Dict, List, Tuple, Optional, Literal, TypedDict, Any
from datetime import datetime
from pydantic import BaseModel


class Variants(TypedDict):
    type: str
    url: str
    width: int
    height: int
    file_ext: str


class MediaAsset(TypedDict):
    id: int
    created_at: str
    updated_at: str
    md5: str
    file_ext: str
    file_size: int
    image_width: int
    image_height: int
    duration: Optional[str]
    status: str
    file_key: str
    is_public: bool
    pixel_hash: str
    variants: List[Variants]


class PostRaw(TypedDict):
    id: int
    created_at: str
    uploader_id: int
    score: int
    source: str
    md5: str
    last_comment_bumped_at: str
    rating: str
    image_width: int
    image_height: int
    tag_string: str
    fav_count: int
    file_ext: str
    last_noted_at: Optional[str]
    parent_id: int
    has_children: bool
    approver_id: Optional[int]
    tag_count_general: int
    tag_count_artist: int
    tag_count_character: int
    tag_count_copyright: int
    file_size: int
    up_score: int
    down_score: int
    is_pending: bool
    is_flagged: bool
    is_deleted: bool
    tag_count: int
    updated_at: str
    is_banned: bool
    pixiv_id: Optional[int]
    last_commented_at: str
    has_active_children: bool
    bit_flags: int
    tag_count_meta: int
    has_large: bool
    has_visible_children: bool
    media_asset: MediaAsset
    tag_string_general: str
    tag_string_character: str
    tag_string_copyright: str
    tag_string_artist: str
    tag_string_meta: str
    file_url: str
    large_file_url: str
    preview_file_url: str


class PostEntry(BaseModel):
    post_id: int
    created_at: datetime
    uploaded_id: int
    score: int
    source: str
    md5: str
    last_commented_at: Optional[datetime] = None
    rating: str
    width: int
    height: int
    fav_count: int
    file_ext: str
    last_noted_at: Optional[datetime]
    parent_id: Optional[int] = None
    has_children: bool
    approver_id: Optional[int] = None
    file_size: int
    up_score: int
    down_score: int
    is_pending: bool
    is_flaged: bool
    is_deleted: bool
    updated_at: datetime
    is_banned: bool
    pixiv_id: Optional[int] = None


def post_raw_to_entry(raw: PostRaw) -> PostEntry:
    from dateutil.parser import parse
    temp = {
        'post_id': raw['id'],
        'created_at': parse(raw['created_at']),
        'uploaded_id': raw['uploader_id'],
        'score': raw['score'],
        'source': raw['source'],
        'md5': raw['md5'],
        'last_commented_at': parse(raw['last_comment_bumped_at']) if raw['last_comment_bumped_at'] else None,
        'rating': raw['rating'],
        'width': raw['image_width'],
        'height': raw['image_height'],
        'fav_count': raw['fav_count'],
        'file_ext': raw['file_ext'],
        'last_noted_at': parse(raw['last_noted_at']) if raw['last_noted_at'] else None,
        'parent_id': raw['parent_id'],
        'has_children': raw['has_children'],
        'approver_id': raw['approver_id'],
        'file_size': raw['file_size'],
        'up_score': raw['up_score'],
        'down_score': raw['down_score'],
        'is_pending': raw['is_pending'],
        'is_flaged': raw['is_flagged'],
        'is_deleted': raw['is_deleted'],
        'updated_at': parse(raw['updated_at']),
        'is_banned': raw['is_banned'],
        'pixiv_id': raw['pixiv_id']
    }
    entry = PostEntry.model_validate(temp)
    return entry
