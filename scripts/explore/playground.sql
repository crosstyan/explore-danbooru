CREATE OR REPLACE VIEW booru.artists_with_n_posts AS
SELECT t.id      as tag_id,
       artist_id as artist_id,
       t.name    as tag_name,
       post_count
FROM booru.artists
         INNER JOIN booru.artist_tags_assoc ata on artists.id = ata.artist_id
         INNER JOIN booru.tags t on ata.tag_id = t.id
         INNER JOIN booru.tag_post_counts tpc on t.id = tpc.tag_id
WHERE tpc.post_count > 100
GROUP BY t.id, artist_id, t.name, post_count
ORDER BY post_count DESC;

-- SELECT count(*)
-- FROM booru.artists_with_n_posts;

SELECT t.name as tag_name,
       post_count
FROM booru.artists
         INNER JOIN booru.artist_tags_assoc ata on artists.id = ata.artist_id
         INNER JOIN booru.tags t on ata.tag_id = t.id
         INNER JOIN booru.tag_post_counts tpc on t.id = tpc.tag_id
GROUP BY t.id, artist_id, t.name, post_count
ORDER BY post_count DESC;

CREATE OR REPLACE VIEW booru.view_modern_posts AS
SELECT distinct p.id            AS post_id,
                p.created_at,
                p.score,
                p.fav_count,
                array_agg(t.id) as tag_ids
FROM booru.posts p
         INNER JOIN booru.posts_tags_assoc pta on p.id = pta.post_id
         INNER JOIN booru.tags t on pta.tag_id = t.id
WHERE p.created_at > '2016-01-01'
GROUP BY p.id;

CREATE MATERIALIZED VIEW booru.view_modern_posts_illustration_only AS
SELECT *
FROM booru.view_modern_posts
WHERE NOT (tag_ids && ARRAY(SELECT id FROM booru.tags WHERE name = 'video'))
  AND NOT (tag_ids && ARRAY(SELECT id FROM booru.tags WHERE name = 'sound'))
  AND NOT (tag_ids && ARRAY(SELECT id FROM booru.tags WHERE name = 'animated'))
  AND NOT (tag_ids && ARRAY(SELECT id FROM booru.tags WHERE name LIKE '%comic' AND category = 0))
  AND NOT (tag_ids && ARRAY(SELECT id FROM booru.tags WHERE name LIKE '%4koma' AND category = 0));

CREATE OR REPLACE VIEW booru.view_posts_count_illustration_only AS
SELECT t.id               AS tag_id,
       COUNT(pta.post_id) AS post_count
FROM booru.tags t
         INNER JOIN booru.posts_tags_assoc pta ON t.id = pta.tag_id
         INNER JOIN booru.view_modern_posts_illustration_only p on pta.post_id = p.post_id
GROUP BY t.id;

-- a function return the tag id by artist name
-- which should be a unique single value
CREATE OR REPLACE FUNCTION booru.get_tag_id_by_artist_name(artist_name TEXT)
    RETURNS INT AS
$$
DECLARE
    ret_tag_id INT;
BEGIN
    SELECT ata.tag_id
    INTO ret_tag_id
    FROM booru.artists a
             JOIN booru.artist_tags_assoc ata ON a.id = ata.artist_id
    WHERE a.name = artist_name
    LIMIT 1;

    IF FOUND THEN
        RETURN ret_tag_id;
    ELSE
        RETURN NULL;
    END IF;
END;
$$
    LANGUAGE 'plpgsql';


CREATE OR REPLACE VIEW booru.view_artist_illustration_only_100 AS
SELECT t.id      as tag_id,
       artist_id as artist_id,
       t.name    as tag_name,
       post_count
FROM booru.artists
         INNER JOIN booru.artist_tags_assoc ata on artists.id = ata.artist_id
         INNER JOIN booru.tags t on ata.tag_id = t.id
         INNER JOIN booru.view_posts_count_illustration_only pc on t.id = pc.tag_id
WHERE pc.post_count > 100
GROUP BY t.id, artist_id, t.name, post_count;


CREATE OR REPLACE VIEW booru.view_modern_pussy_like_posts AS
SELECT distinct p.post_id AS post_id,
                p.created_at,
                p.score,
                p.fav_count
FROM booru.view_modern_posts_illustration_only p
         INNER JOIN booru.posts_tags_assoc pta on p.post_id = pta.post_id
         INNER JOIN booru.tags t on pta.tag_id = t.id
WHERE t.name LIKE '%pussy%'
   OR t.name = 'cameltoe'
    AND t.category = 0;

CREATE OR REPLACE VIEW booru.view_modern_pantyhose_like_posts AS
SELECT distinct p.post_id AS post_id,
                p.created_at,
                p.score,
                p.fav_count
FROM booru.view_modern_posts_illustration_only p
         INNER JOIN booru.posts_tags_assoc pta on p.post_id = pta.post_id
         INNER JOIN booru.tags t on pta.tag_id = t.id
WHERE t.name LIKE '%pantyhose%'
  AND t.category = 0;

CREATE OR REPLACE VIEW booru.view_modern_pantyhose_like_posts_filtered AS
SELECT distinct p.post_id,
                p.created_at,
                p.score,
                p.fav_count,
                artists_n.artist_id,
                artists_n.tag_id,
                artists_n.tag_name
FROM booru.view_artist_illustration_only_100 artists_n
         INNER JOIN booru.posts_tags_assoc pta on artists_n.tag_id = pta.tag_id
         INNER JOIN booru.view_modern_pantyhose_like_posts p on pta.post_id = p.post_id;

CREATE OR REPLACE VIEW booru.view_modern_pussy_like_posts_filtered AS
SELECT distinct p.post_id,
                p.created_at,
                p.score,
                p.fav_count,
                artists_n.artist_id,
                artists_n.tag_id,
                artists_n.tag_name
FROM booru.view_artist_illustration_only_100 artists_n
         INNER JOIN booru.posts_tags_assoc pta on artists_n.tag_id = pta.tag_id
         INNER JOIN booru.view_modern_pussy_like_posts p on pta.post_id = p.post_id;

SELECT artist_id, tag_id, tag_name, avg(score) as avg_score, avg(fav_count) as avg_fav_count
FROM booru.view_modern_pussy_like_posts_filtered
GROUP BY artist_id, tag_id, tag_name
ORDER BY avg(score) DESC
LIMIT 100;

SELECT artist_id, tag_id, tag_name, avg(score) as avg_score, avg(fav_count) as avg_fav_count
FROM booru.view_modern_pantyhose_like_posts_filtered
GROUP BY artist_id, tag_id, tag_name
ORDER BY avg(score) DESC
LIMIT 100;
