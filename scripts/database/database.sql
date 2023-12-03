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
    post_id INT NOT NULL REFERENCES booru.posts (id),
    type    TEXT,
    width   INT,
    height  INT,
    url     TEXT
);

CREATE TABLE booru.posts_file_urls
(
    id               SERIAL PRIMARY KEY,
    post_id          INT NOT NULL REFERENCES booru.posts (id),
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

COMMENT ON TABLE booru.tags IS
    'https://danbooru.donmai.us/wiki_pages/api:tags';
-- no 2
COMMENT ON COLUMN booru.tags.category IS
    'The category of the tag. 0 = General, 1 = Artist, 3 = Copyright, 4 = Character, 5 = Meta';


CREATE TABLE booru.tags_aliases
(
    id              INT PRIMARY KEY,
    antecedent_name TEXT NOT NULL,
    consequent_name TEXT NOT NULL
);

COMMENT ON COLUMN booru.tags_aliases.antecedent_name
    IS 'The name of the tag that is a synonym of another tag, which should be in `booru.tags.name`';
COMMENT ON COLUMN booru.tags_aliases.consequent_name
    IS 'The name of the tag that is the target of a synonym, which should be in `booru.tags.name`';

CREATE TABLE booru.tags_implications
(
    id              INT PRIMARY KEY,
    antecedent_name TEXT NOT NULL,
    consequent_name TEXT NOT NULL
);

COMMENT ON COLUMN booru.tags_implications.antecedent_name
    IS 'The name of the tag that implies another tag, which should be in `booru.tags.name`';
COMMENT ON COLUMN booru.tags_implications.consequent_name
    IS 'The name of the tag that is implied by another tag, which should be in `booru.tags.name`';

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

COMMENT ON COLUMN booru.artists.name IS 'The name of the artist, which should be in `booru.tags.name`';

CREATE TABLE booru.artists_urls
(
    id         INT PRIMARY KEY,
    artist_id  INT NOT NULL REFERENCES booru.artists (id),
    url        TEXT,
    created_at timestamptz,
    updated_at timestamptz,
    is_active  BOOLEAN
);


CREATE TABLE booru.artists_aliases
(
    id        SERIAL PRIMARY KEY,
    artist_id INT  NOT NULL REFERENCES booru.artists (id),
    alias     TEXT NOT NULL,
    CONSTRAINT unique_artist_alias UNIQUE (artist_id, alias)
);


-- TODO: create a view for implications and aliases
-- TODO: create a view for tag counts of posts

-- tags <> posts (junction table)
CREATE TABLE booru.posts_tags_assoc
(
    post_id INT REFERENCES booru.posts (id),
    tag_id  INT REFERENCES booru.tags (id),
    PRIMARY KEY (post_id, tag_id)
);

CREATE VIEW booru.artist_tag_details AS
SELECT t.id                                AS tag_id,
       a.id                                AS artist_id,
       a.name                              AS artist_name,
       a.group_name                        AS artist_group_name,
       t.is_deprecated                     AS tag_is_deprecated,
       a.is_banned                         AS artist_is_banned,
       a.is_deleted                        AS artist_is_deleted,
       array_agg(DISTINCT al.alias)
       FILTER (WHERE al.alias IS NOT NULL) AS artist_aliases,
       array_agg(DISTINCT au.url)
       FILTER (WHERE au.url IS NOT NULL)   AS artist_urls
FROM booru.tags t
         JOIN
     booru.artists a ON t.name = a.name AND t.category = 1
         LEFT JOIN
     booru.artists_aliases al ON a.id = al.artist_id
         LEFT JOIN
     booru.artists_urls au ON a.id = au.artist_id
GROUP BY t.id, a.id;

CREATE VIEW booru.posts_tag_view AS
SELECT p.id,
       p.created_at,
       p.score,
       p.rating,
       array_agg(DISTINCT t.name)
       FILTER (WHERE t.category = 0) AS general,
       array_agg(DISTINCT t.name)
       FILTER (WHERE t.category = 1) AS artist,
       array_agg(DISTINCT t.name)
       FILTER (WHERE t.category = 3) AS copyright,
       array_agg(DISTINCT t.name)
       FILTER (WHERE t.category = 4) AS character,
       array_agg(DISTINCT t.name)
       FILTER (WHERE t.category = 5) AS meta
FROM booru.posts p
         JOIN
     booru.posts_tags_assoc pta ON p.id = pta.post_id
         JOIN
     booru.tags t ON pta.tag_id = t.id
GROUP BY p.id, p.created_at, p.score, p.rating;

-- indexes to improve the performance of queries involving those columns
CREATE INDEX idx_tags_ids ON booru.tags (id);
CREATE INDEX idx_tags_names ON booru.tags (name, id);
CREATE INDEX idx_posts_ids ON booru.posts (id);
CREATE INDEX idx_posts_tags ON booru.posts_tags_assoc (post_id, tag_id);
