CREATE SCHEMA IF NOT EXISTS booru;

-- DROP TABLE IF EXISTS booru.posts;
CREATE TABLE booru.posts
(
    post_id           INT PRIMARY KEY,
    created_at        timestamptz,
    uploaded_id       INT,
    score             INT,
    source            TEXT,
    md5               TEXT,
    last_commented_at timestamptz,
    rating            TEXT,
    width             INT,
    height            INT,
    fav_count         INT,
    file_ext          TEXT,
    last_noted_at     timestamptz,
    parent_id         INT,
    has_children      BOOLEAN,
    approver_id       INT,
    file_size         INT,
    up_score          INT,
    down_score        INT,
    is_pending        BOOLEAN,
    is_flagged        BOOLEAN,
    is_deleted        BOOLEAN,
    updated_at        timestamptz,
    is_banned         BOOLEAN,
    pixiv_id          INT
);

-- DROP TABLE IF EXISTS booru.tags;
CREATE TABLE booru.tags
(
    tag_id   INT PRIMARY KEY,
    name     TEXT,
    category INT,
    is_deprecated BOOLEAN
);

-- tags <> posts (junction table)
CREATE TABLE booru.posts_tags
(
    post_id INT,
    tag_id  INT,
    PRIMARY KEY (post_id, tag_id)
);

-- indexes to improve the performance of queries involving those columns
CREATE INDEX idx_tags_ids ON booru.tags (tag_id);
CREATE INDEX idx_tags ON booru.tags (name, tag_id);
CREATE INDEX idx_posts_ids ON booru.posts (post_id);
CREATE INDEX idx_posts_tags ON booru.posts_tags (post_id, tag_id);
