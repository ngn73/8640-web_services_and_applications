-- Use Database
USE MEDIA_ANALYTICS_DB;

CREATE VIEW v_show_season_episode AS
SELECT 
	s.tmdb_id AS tmdb_id,
	s.name AS show_name,
	se.tmdb_season_id AS tmdb_season_id,
	se.season_number AS season_number,
	e.tmdb_episode_id AS tmdb_episode_id,
	e.episode_number AS episode_number,
	e.name AS episode_name,e.air_date AS air_date 
FROM tmdb_show s INNER JOIN tmdb_season se 
ON se.tmdb_show_id = s.tmdb_id 
INNER JOIN tmdb_episode e 
ON((e.tmdb_show_id = se.tmdb_show_id) and (e.season_number = se.season_number))


CREATE VIEW v_episode_cast AS
SELECT
    p.tmdb_person_id,
    p.person_name,
    c.character_name,
    ep.name,
    ep.episode_number,
    ep.season_number,
    ep.episode_number,
    s.name
FROM tmdb_person p
JOIN tmdb_episode_cast c
    ON p.tmdb_person_id = c.tmdb_person_id
JOIN tmdb_episode ep 
    ON ep.tmdb_episode_id = c.tmdb_episode_id
JOIN tmdb_show s 
    ON s.tmdb_id = ep.tmdb_show_id;