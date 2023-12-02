CREATE TABLE posts
(
    post_id           INT PRIMARY KEY,
    created_at        TEXT,
    uploaded_id       INT,
    score             INT,
    source            TEXT,
    md5               TEXT,
    last_commented_at TEXT,
    rating            TEXT,
    image_width       INT,
    image_height      INT,
    is_note_locked    INT,
    file_ext          TEXT,
    last_noted_at     TEXT,
    is_rating_locked  INT,
    parent_id         INT,
    has_children      INT,
    approver_id       INT,
    file_size         INT,
    is_status_locked  INT,
    up_score          INT,
    down_score        INT,
    is_pending        INT,
    is_flagged        INT,
    is_deleted        INT,
    updated_at        TEXT,
    is_banned         INT,
    pixiv_id          INT,
    hidden            INT DEFAULT 0
);

CREATE TABLE tags
(
    tag_id   INT PRIMARY KEY,
    name     TEXT,
    category INT
);

-- tags <> posts (junction table)
CREATE TABLE posts_tags
(
    post_id INT,
    tag_id  INT,
    PRIMARY KEY (post_id, tag_id)
);

-- indexes to improve the performance of queries involving those columns
CREATE INDEX idx_tags_ids ON tags (tag_id);
CREATE INDEX idx_tags ON tags (name, tag_id);
CREATE INDEX idx_posts_ids ON posts (post_id);
CREATE INDEX idx_posts_tags ON posts_tags (post_id, tag_id);
