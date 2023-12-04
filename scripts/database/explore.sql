CREATE VIEW booru.artist_view AS
SELECT ata.tag_id                          AS tag_id,
       a.id                                AS artist_id,
       a.name                              AS artist_name,
       a.group_name                        AS artist_group_name,
       pc.post_count                       AS post_count,
       a.is_banned                         AS artist_is_banned,
       array_agg(DISTINCT al.alias)
       FILTER (WHERE al.alias IS NOT NULL) AS artist_aliases,
       array_agg(DISTINCT au.url)
       FILTER (WHERE au.url IS NOT NULL)   AS artist_urls
FROM booru.artist_tags_assoc ata
         JOIN
     booru.artists a ON ata.artist_id = a.id
         LEFT JOIN
     booru.artists_aliases al ON a.id = al.artist_id
         LEFT JOIN
     booru.artists_urls au ON a.id = au.artist_id
         LEFT JOIN
     booru.tag_post_counts pc ON ata.tag_id = pc.tag_id
GROUP BY ata.tag_id, a.id, pc.post_count;

CREATE VIEW booru.posts_tag_view AS
SELECT p.id                          AS post_id,
       p.created_at,
       p.score,
       p.rating,
       p.fav_count,
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
GROUP BY p.id, p.created_at, p.score, p.rating, p.fav_count;

CREATE OR REPLACE VIEW booru.view_post_aspect_ratio AS
SELECT p.id,
       p.width,
       p.height,
       p.width::float / p.height::float AS aspect_ratio,
       CASE
           WHEN width::float / height::float <= (9.0 / 21) THEN '(0, 9/21]'
           WHEN width::float / height::float > (9.0 / 21) AND width::float / height::float <= (9.0 / 16)
               THEN '(9/21, 9/16]'
           WHEN width::float / height::float > (9.0 / 16) AND width::float / height::float <= (3.0 / 4)
               THEN '(9/16, 3/4]'
           WHEN width::float / height::float > (3.0 / 4) AND width::float / height::float < 1.0 THEN '(3/4, 1)'
           WHEN width::float / height::float = 1.0 THEN '1'
           WHEN width::float / height::float > 1.0 AND width::float / height::float <= (4.0 / 3) THEN '(1, 4/3]'
           WHEN width::float / height::float > (4.0 / 3) AND width::float / height::float <= (16.0 / 9)
               THEN '(4/3, 16/9]'
           WHEN width::float / height::float > (16.0 / 9) AND width::float / height::float <= (21.0 / 9)
               THEN '(16/9, 21/9]'
           ELSE '(21/9, inf)'
           END                          AS aspect_ratio_bucket
FROM booru.posts p;

-- SELECT aspect_ratio_bucket, count(*)
-- FROM booru.view_post_aspect_ratio
-- GROUP BY aspect_ratio_bucket;

