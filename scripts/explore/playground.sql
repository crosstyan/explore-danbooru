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
GROUP BY t.id, artist_id, t.name, post_count;

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
SELECT *
FROM booru.posts p
WHERE p.created_at > '2015-01-01';

CREATE OR REPLACE VIEW booru.view_posts_tags AS
SELECT distinct p.id,
                array_agg(t.id) as tag_ids
FROM booru.posts p
         INNER JOIN booru.posts_tags_assoc pta on p.id = pta.post_id
         INNER JOIN booru.tags t on pta.tag_id = t.id
group by p.id;

CREATE OR REPLACE VIEW booru.view_posts_illustration_only AS
SELECT distinct p.id,
                p.created_at,
                p.score,
                p.fav_count,
                pt.tag_ids as tag_ids
FROM booru.view_posts_tags pt
         INNER JOIN booru.posts p on pt.id = p.id
WHERE NOT (tag_ids && ARRAY(SELECT id FROM booru.tags WHERE name = 'video'))
  AND NOT (tag_ids && ARRAY(SELECT id FROM booru.tags WHERE name = 'sound'))
  AND NOT (tag_ids && ARRAY(SELECT id FROM booru.tags WHERE name = 'animated'))
  AND NOT (tag_ids && ARRAY(SELECT id FROM booru.tags WHERE name LIKE '%comic' AND category = 0))
  AND NOT (tag_ids && ARRAY(SELECT id FROM booru.tags WHERE name LIKE '%4koma' AND category = 0));

CREATE MATERIALIZED VIEW booru.view_modern_posts_illustration_only AS
SELECT distinct p.id      as post_id,
                p.created_at,
                p.score,
                p.fav_count,
                p.tag_ids as tag_ids
FROM booru.view_modern_posts vmp
         INNER JOIN booru.view_posts_illustration_only p on p.id = vmp.id;

CREATE MATERIALIZED VIEW booru.view_modern_posts_illustration_only_extra AS
SELECT p.id as post_id,
       p.created_at,
       p.score,
       p.fav_count,
       p.pixiv_id,
       vmpi.tag_ids,
       vpar.width,
       vpar.height,
       vpar.aspect_ratio,
       vpar.aspect_ratio_bucket,
       pfu.file_url,
       pfu.preview_file_url
FROM booru.view_modern_posts_illustration_only vmpi
         INNER JOIN booru.posts p on p.id = vmpi.post_id
         INNER JOIN booru.view_post_aspect_ratio vpar on p.id = vpar.id
         INNER JOIN booru.posts_file_urls pfu on p.id = pfu.post_id;

CREATE MATERIALIZED VIEW booru.view_posts_count_illustration_only AS
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

-- best artists for pussy
WITH target_posts AS (SELECT distinct p.post_id AS post_id,
                                      p.created_at,
                                      p.score,
                                      p.fav_count,
                                      p.tag_ids
                      FROM booru.view_modern_posts_illustration_only p
                      WHERE ((p.tag_ids && ARRAY(SELECT id FROM booru.tags WHERE name LIKE '%pussy%' AND category = 0))
                          OR (p.tag_ids && ARRAY(SELECT id FROM booru.tags WHERE name = 'cameltoe')))),
     target_filtered_posts AS (SELECT distinct p.post_id,
                                               p.created_at,
                                               p.score,
                                               p.fav_count,
                                               p.tag_ids,
                                               artists_n.artist_id,
                                               artists_n.tag_id,
                                               artists_n.tag_name
                               FROM booru.view_artist_illustration_only_100 artists_n
                                        INNER JOIN booru.posts_tags_assoc pta on artists_n.tag_id = pta.tag_id
                                        INNER JOIN target_posts p on pta.post_id = p.post_id),
     agg_artists AS (SELECT distinct pf.artist_id,
                                     pf.tag_id,
                                     pf.tag_name,
                                     avg(score)     as avg_score,
                                     avg(fav_count) as avg_fav_count,
                                     count(*)       as target_post_count,
                                     pc.post_count  as total_post_count
                     FROM target_filtered_posts pf
                              INNER JOIN booru.view_posts_count_illustration_only pc on pf.tag_id = pc.tag_id
                     GROUP BY pf.artist_id, pf.tag_id, pf.tag_name, pc.post_count),
     ratioed AS (SELECT *, target_post_count::float / total_post_count::float as target_ratio
                 FROM agg_artists),
     limited AS (SELECT *
                 FROM ratioed
                 WHERE target_ratio > 0.1
                 ORDER BY avg_score DESC
                 LIMIT 500)
SELECT *
FROM limited;

-- best artists for uncensored pussy
WITH target_posts AS (SELECT distinct p.post_id AS post_id,
                                      p.created_at,
                                      p.score,
                                      p.fav_count,
                                      p.tag_ids
                      FROM booru.view_modern_posts_illustration_only p
                      WHERE (p.tag_ids && ARRAY(SELECT id FROM booru.tags WHERE name = 'uncensored'))
                        AND ((p.tag_ids && ARRAY(SELECT id FROM booru.tags WHERE name LIKE '%pussy%' AND category = 0))
                          OR (p.tag_ids && ARRAY(SELECT id FROM booru.tags WHERE name = 'cameltoe')))),
     target_filtered_posts AS (SELECT distinct p.post_id,
                                               p.created_at,
                                               p.score,
                                               p.fav_count,
                                               p.tag_ids,
                                               artists_n.artist_id,
                                               artists_n.tag_id,
                                               artists_n.tag_name
                               FROM booru.view_artist_illustration_only_100 artists_n
                                        INNER JOIN booru.posts_tags_assoc pta on artists_n.tag_id = pta.tag_id
                                        INNER JOIN target_posts p on pta.post_id = p.post_id),
     agg_artists AS (SELECT distinct pf.artist_id,
                                     pf.tag_id,
                                     pf.tag_name,
                                     count(*)       as target_post_count,
                                     pc.post_count  as total_post_count,
                                     avg(score)     as avg_score,
                                     avg(fav_count) as avg_fav_count
                     FROM target_filtered_posts pf
                              INNER JOIN booru.view_posts_count_illustration_only pc on pf.tag_id = pc.tag_id
                     GROUP BY pf.artist_id, pf.tag_id, pf.tag_name, pc.post_count),
     ratioed AS (SELECT *, target_post_count::float / total_post_count::float as ratio
                 FROM agg_artists),
     limited AS (SELECT *
                 FROM ratioed
                 WHERE ratio > 0.1
                 ORDER BY ratio DESC
                 LIMIT 500)
SELECT *
FROM limited;


-- best artists for pantyhose
WITH target_posts AS (SELECT distinct p.post_id AS post_id,
                                      p.created_at,
                                      p.score,
                                      p.fav_count,
                                      p.tag_ids
                      FROM booru.view_modern_posts_illustration_only p
                      WHERE (p.tag_ids &&
                             ARRAY(SELECT id FROM booru.tags WHERE name LIKE '%pantyhose%' AND category = 0))),
     target_filtered_posts AS (SELECT distinct p.post_id,
                                               p.created_at,
                                               p.score,
                                               p.fav_count,
                                               p.tag_ids,
                                               artists_n.artist_id,
                                               artists_n.tag_id,
                                               artists_n.tag_name
                               FROM booru.view_artist_illustration_only_100 artists_n
                                        INNER JOIN booru.posts_tags_assoc pta on artists_n.tag_id = pta.tag_id
                                        INNER JOIN target_posts p on pta.post_id = p.post_id),
     agg_artists AS (SELECT distinct pf.artist_id,
                                     pf.tag_id,
                                     pf.tag_name,
                                     count(*)       as target_post_count,
                                     pc.post_count  as total_post_count,
                                     avg(score)     as avg_score,
                                     avg(fav_count) as avg_fav_count
                     FROM target_filtered_posts pf
                              INNER JOIN booru.view_posts_count_illustration_only pc on pf.tag_id = pc.tag_id
                     GROUP BY pf.artist_id, pf.tag_id, pf.tag_name, pc.post_count),
     ratioed AS (SELECT *, target_post_count::float / total_post_count::float as ratio
                 FROM agg_artists),
     limited AS (SELECT *
                 FROM ratioed
                 WHERE ratio > 0.1
                 ORDER BY ratio DESC
                 LIMIT 500)
SELECT *
FROM limited;

-- best artists for pantyhose related
WITH target_posts AS (SELECT distinct p.post_id AS post_id,
                                      p.created_at,
                                      p.score,
                                      p.fav_count,
                                      p.tag_ids
                      FROM booru.view_modern_posts_illustration_only p
                      WHERE (p.tag_ids &&
                             ARRAY(SELECT id FROM booru.tags WHERE name LIKE '%pantyhose' AND category = 0))
                         OR (p.tag_ids &&
                             ARRAY(SELECT id FROM booru.tags WHERE name LIKE '%thighhighs' AND category = 0))
                         OR (p.tag_ids &&
                             ARRAY(SELECT id FROM booru.tags WHERE name = 'fine_fabric_emphasis'))
                         OR (p.tag_ids &&
                             ARRAY(SELECT id FROM booru.tags WHERE name = 'crotch_seam'))
                         OR (p.tag_ids &&
                             ARRAY(SELECT id FROM booru.tags WHERE name = 'bodystocking'))),
     target_filtered_posts AS (SELECT distinct p.post_id,
                                               p.created_at,
                                               p.score,
                                               p.fav_count,
                                               p.tag_ids,
                                               artists_n.artist_id,
                                               artists_n.tag_id,
                                               artists_n.tag_name
                               FROM booru.view_artist_illustration_only_100 artists_n
                                        INNER JOIN booru.posts_tags_assoc pta on artists_n.tag_id = pta.tag_id
                                        INNER JOIN target_posts p on pta.post_id = p.post_id),
     agg_artists AS (SELECT distinct pf.artist_id,
                                     pf.tag_id,
                                     pf.tag_name,
                                     avg(score)     as avg_score,
                                     avg(fav_count) as avg_fav_count,
                                     count(*)       as target_post_count,
                                     pc.post_count  as total_post_count
                     FROM target_filtered_posts pf
                              INNER JOIN booru.view_posts_count_illustration_only pc on pf.tag_id = pc.tag_id
                     GROUP BY pf.artist_id, pf.tag_id, pf.tag_name, pc.post_count),
     ratioed AS (SELECT *, target_post_count::float / total_post_count::float as target_ratio
                 FROM agg_artists),
     limited AS (SELECT *
                 FROM ratioed
                 WHERE (target_ratio > 0.1
                     OR target_post_count > 100)
                   AND avg_score > 30
                 ORDER BY target_ratio DESC
                 LIMIT 2000)
SELECT *
FROM limited;


-- best artists for xp
WITH target_posts AS (SELECT distinct p.post_id AS post_id,
                                      p.created_at,
                                      p.score,
                                      p.fav_count,
                                      p.tag_ids
                      FROM booru.view_modern_posts_illustration_only p
                      WHERE ((p.tag_ids &&
                              ARRAY(SELECT id FROM booru.tags WHERE name LIKE '%masturbation' AND category = 0))
                          OR (p.tag_ids &&
                              ARRAY(SELECT id FROM booru.tags WHERE name = 'presenting'))
                          OR (p.tag_ids &&
                              ARRAY(SELECT id FROM booru.tags WHERE name = 'spread_pussy')))
                        AND NOT ((p.tag_ids &&
                                  ARRAY(SELECT id FROM booru.tags WHERE name LIKE '%penis' AND category = 0))
                          OR (p.tag_ids &&
                              ARRAY(SELECT id FROM booru.tags WHERE name = 'male_masturbation'))
                          OR (p.tag_ids &&
                              ARRAY(SELECT id FROM booru.tags WHERE name = '1boy')))),
     target_filtered_posts AS (SELECT distinct p.post_id,
                                               p.created_at,
                                               p.score,
                                               p.fav_count,
                                               p.tag_ids,
                                               artists_n.artist_id,
                                               artists_n.tag_id,
                                               artists_n.tag_name
                               FROM booru.view_artist_illustration_only_100 artists_n
                                        INNER JOIN booru.posts_tags_assoc pta on artists_n.tag_id = pta.tag_id
                                        INNER JOIN target_posts p on pta.post_id = p.post_id),
     agg_artists AS (SELECT distinct pf.artist_id,
                                     pf.tag_id,
                                     pf.tag_name,
                                     avg(score)     as avg_score,
                                     avg(fav_count) as avg_fav_count,
                                     count(*)       as target_post_count,
                                     pc.post_count  as total_post_count
                     FROM target_filtered_posts pf
                              INNER JOIN booru.view_posts_count_illustration_only pc on pf.tag_id = pc.tag_id
                     GROUP BY pf.artist_id, pf.tag_id, pf.tag_name, pc.post_count),
     ratioed AS (SELECT *, target_post_count::float / total_post_count::float as target_ratio
                 FROM agg_artists),
     limited AS (SELECT *
                 FROM ratioed
                 WHERE (target_ratio > 0.1
                     OR target_post_count > 100)
                   AND avg_score > 30
                 ORDER BY target_ratio DESC
                 LIMIT 2000)
SELECT *
FROM limited;

-- https://www.pgexplain.dev/
EXPLAIN (ANALYZE, BUFFERS, COSTS, TIMING, SUMMARY)
WITH artist AS (SELECT *
                FROM booru.artists_with_n_posts
                ORDER BY random()
                LIMIT 1),
     artist_posts_with_tags_id AS (SELECT ap.post_id,
                                          ap.score,
                                          ap.fav_count,
                                          ap.tag_ids,
                                          ap.created_at,
                                          ap.file_url,
                                          ap.preview_file_url
                                   FROM booru.view_modern_posts_illustration_only_extra ap
                                   WHERE ap.tag_ids && ARRAY(SELECT tag_id FROM artist)),

     -- translate tag id to tag name
     artist_posts_with_tags AS (SELECT ap.post_id,
                                       (SELECT array_agg(name)
                                        FROM booru.tags
                                        WHERE id = ANY (tag_ids)
                                          AND category = 1) as artist_tags,
                                       ap.score,
                                       ap.fav_count,
                                       (SELECT array_agg(name)
                                        FROM booru.tags
                                        WHERE id = ANY (tag_ids)
                                          AND category = 0) as general_tags,
                                       (SELECT array_agg(name)
                                        FROM booru.tags
                                        WHERE id = ANY (tag_ids)
                                          AND category = 3) as copyright_tags,
                                       (SELECT array_agg(name)
                                        FROM booru.tags
                                        WHERE id = ANY (tag_ids)
                                          AND category = 4) as characters_tags,
                                       ap.created_at,
                                       ap.file_url,
                                       ap.preview_file_url
                                FROM artist_posts_with_tags_id as ap)
SELECT *
FROM artist_posts_with_tags
LIMIT 50;

-- do the stats for all artists
CREATE MATERIALIZED VIEW booru.artists_stats AS
WITH spreaded AS (SELECT unnest(tag_ids) as tag_id, * FROM booru.view_modern_posts_illustration_only_extra)
SELECT a.tag_id,
       a.tag_name        as artist_name,
       min(p.created_at) as earliest_post_date,
       max(p.created_at) as latest_post_date,
       avg(p.score)      as avg_score,
       avg(p.fav_count)  as avg_fav_count,
       count(*)          as post_count
FROM spreaded as p
         LEFT JOIN booru.artists_with_n_posts as a ON p.tag_id = a.tag_id
GROUP BY a.tag_name, a.tag_id;


WITH char_tags AS (SELECT *
                   FROM booru.tags
                            INNER JOIN booru.tag_post_counts tpc on tags.id = tpc.tag_id
                   WHERE category = 4
                     AND tpc.post_count > 50),
     with_char_tags_posts AS (SELECT post_id, tag_ids, score, fav_count, tag_ids, ct.name as char_tag
                              FROM booru.view_modern_posts_illustration_only_extra p
                                       CROSS JOIN LATERAL UNNEST(p.tag_ids) AS unnested_tag_id
                                       INNER JOIN char_tags ct ON ct.id = unnested_tag_id
                              group by post_id, tag_ids, score, fav_count, ct.name),
     char_table AS (SELECT char_tag, count(*) as count, avg(score) as avg_score, avg(fav_count) as avg_fav_count
                    FROM with_char_tags_posts
                    GROUP BY char_tag)
SELECT *
FROM char_table as ct
WHERE ct.count > 50
ORDER BY ct.count DESC;

select count(*)
from booru.artists_with_n_posts;
