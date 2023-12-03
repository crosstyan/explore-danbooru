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
    md5: Optional[str]
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
    file_size: Optional[int]
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
    id: int
    created_at: datetime
    uploaded_id: Optional[int] = None
    score: Optional[int] = None
    source: Optional[str] = None
    md5: Optional[str] = None
    last_commented_at: Optional[datetime] = None
    rating: Optional[str] = None
    width: Optional[int] = None
    height: Optional[int] = None
    fav_count: Optional[int] = None
    file_ext: Optional[str] = None
    last_noted_at: Optional[datetime] = None
    parent_id: Optional[int] = None
    has_children: bool = False
    approver_id: Optional[int] = None
    file_size: int = 0
    up_score: int = 0
    down_score: int = 0
    is_pending: bool = False
    is_flagged: bool = False
    is_deleted: bool = False
    updated_at: Optional[datetime] = None
    is_banned: bool = False
    pixiv_id: Optional[int] = None

    @staticmethod
    def from_raw(raw: PostRaw) -> "PostEntry":
        from dateutil.parser import parse

        def parse_datetime(date_string):
            return parse(date_string) if date_string else None

        temp = {
            'id': raw.get('id'),
            'created_at': parse_datetime(raw.get('created_at')),
            'uploaded_id': raw.get('uploader_id'),
            'score': raw.get('score'),
            'source': raw.get('source'),
            'md5': raw.get('md5'),
            'last_commented_at': parse_datetime(raw.get('last_comment_bumped_at')),
            'rating': raw.get('rating'),
            'width': raw.get('image_width'),
            'height': raw.get('image_height'),
            'fav_count': raw.get('fav_count'),
            'file_ext': raw.get('file_ext'),
            'last_noted_at': parse_datetime(raw.get('last_noted_at')),
            'parent_id': raw.get('parent_id'),
            'has_children': raw.get('has_children'),
            'approver_id': raw.get('approver_id'),
            'file_size': raw.get('file_size'),
            'up_score': raw.get('up_score'),
            'down_score': raw.get('down_score'),
            'is_pending': raw.get('is_pending'),
            'is_flagged': raw.get('is_flagged'),
            'is_deleted': raw.get('is_deleted'),
            'updated_at': parse_datetime(raw.get('updated_at')),
            'is_banned': raw.get('is_banned'),
            'pixiv_id': raw.get('pixiv_id')
        }

        entry = PostEntry.model_validate(temp)
        return entry


class PostMediaVariantEntry(BaseModel):
    post_id: int
    type: Optional[str]
    url: Optional[str]
    width: Optional[int]
    height: Optional[int]

    @staticmethod
    def from_raw(raw: PostRaw) -> "list[PostMediaVariantEntry]":
        # variants = raw['media_asset']['variants']
        variants = raw.get('media_asset', {}).get('variants', [])
        entries = []
        for variant in variants:
            entry = PostMediaVariantEntry(
                post_id=raw['id'],
                type=variant.get('type'),
                url=variant.get('url'),
                width=variant.get('width'),
                height=variant.get('height')
            )
            entries.append(entry)
        return entries


class PostFileEntry(BaseModel):
    post_id: int
    file_url: Optional[str]
    large_file_url: Optional[str]
    preview_file_url: Optional[str]

    @staticmethod
    def from_raw(raw: PostRaw) -> "PostFileEntry":
        entry = PostFileEntry(
            post_id=raw['id'],
            file_url=raw.get('file_url'),
            large_file_url=raw.get('large_file_url'),
            preview_file_url=raw.get('preview_file_url')
        )
        return entry
