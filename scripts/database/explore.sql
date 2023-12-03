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
SELECT p.id,
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
