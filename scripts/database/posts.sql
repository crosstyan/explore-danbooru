CREATE SCHEMA IF NOT EXISTS booru;

-- DROP TABLE IF EXISTS booru.posts;
CREATE TABLE booru.posts
(
    id                INT PRIMARY KEY,
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

CREATE TABLE booru.posts_media_variants
(
    id      SERIAL PRIMARY KEY,
    post_id INT NOT NULL,
    type    TEXT,
    width   INT,
    height  INT,
    url     TEXT
);

CREATE TABLE booru.posts_file_urls
(
    id               SERIAL PRIMARY KEY,
    post_id          INT NOT NULL,
    file_url         TEXT,
    large_file_url   TEXT,
    preview_file_url TEXT
);

-- DROP TABLE IF EXISTS booru.tags;
CREATE TABLE booru.tags
(
    id            INT PRIMARY KEY,
    name          TEXT NOT NULL,
    category      INT,
    is_deprecated BOOLEAN
);

CREATE TABLE booru.tags_aliases
(
    id              INT PRIMARY KEY,
    predicate_name  TEXT NOT NULL,
    consequent_name TEXT NOT NULL
);

CREATE TABLE booru.tags_implications
(
    id              INT PRIMARY KEY,
    predicate_name  TEXT NOT NULL,
    consequent_name TEXT NOT NULL
);

CREATE TABLE booru.artists
(
    id         INT PRIMARY KEY,
    name       TEXT NOT NULL,
    group_name TEXT,
    created_at timestamptz,
    updated_at timestamptz,
    is_banned  BOOLEAN,
    is_deleted BOOLEAN
);

CREATE TABLE booru.artists_aliases
(
    id        SERIAL PRIMARY KEY,
    alias     TEXT NOT NULL,
    artist_id INT  NOT NULL
);

-- TODO: create a view for implications and aliases

-- tags <> posts (junction table)
CREATE TABLE booru.posts_tags
(
    post_id INT,
    tag_id  INT,
    PRIMARY KEY (post_id, tag_id)
);


-- indexes to improve the performance of queries involving those columns
CREATE INDEX idx_tags_ids ON booru.tags (id);
CREATE INDEX idx_tags ON booru.tags (name, id);
CREATE INDEX idx_posts_ids ON booru.posts (post_id);
CREATE INDEX idx_posts_tags ON booru.posts_tags (post_id, tag_id);
