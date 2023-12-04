CREATE OR REPLACE VIEW booru.artists_with_n_posts AS
SELECT t.id      as tag_id,
       artist_id as artist_id,
       t.name    as tag_name,
       post_count
FROM booru.artists
         INNER JOIN booru.artist_tags_assoc ata on artists.id = ata.artist_id
         INNER JOIN booru.tags t on ata.tag_id = t.id
         INNER JOIN booru.tag_post_counts tpc on t.id = tpc.tag_id
WHERE tpc.post_count > 200
GROUP BY t.id, artist_id, t.name, post_count
ORDER BY post_count DESC;

SELECT count(*)
FROM booru.artists_with_n_posts;


SELECT t.name as tag_name,
       post_count
FROM booru.artists
         INNER JOIN booru.artist_tags_assoc ata on artists.id = ata.artist_id
         INNER JOIN booru.tags t on ata.tag_id = t.id
         INNER JOIN booru.tag_post_counts tpc on t.id = tpc.tag_id
GROUP BY t.id, artist_id, t.name, post_count
ORDER BY post_count DESC;

CREATE VIEW booru.view_modern_post AS
SELECT *
FROM booru.posts
WHERE created_at > '2019-01-01';

CREATE MATERIALIZED VIEW booru.view_artist_tag_no_comic AS
SELECT t.id               AS tag_id,
       COUNT(pta.post_id) AS post_count
FROM booru.tags t
         INNER JOIN booru.posts_tags_assoc pta ON t.id = pta.tag_id
WHERE t.name NOT LIKE '%comic'
  AND t.name NOT LIKE '%4koma'
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

-- hiten_(hitenkei)
WITH hiten AS (SELECT booru.get_tag_id_by_artist_name('hiten_(hitenkei)') AS id)
SELECT p.id as post_id, p.created_at
FROM booru.posts_tags_assoc pta
         JOIN booru.posts p ON pta.post_id = p.id
         JOIN hiten ON pta.tag_id = hiten.id;
