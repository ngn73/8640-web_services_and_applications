{# SQL scripts related to Trakt_status table #}

{# Use this script to SELECT a Trakt_status record in MySQL DB #}
{% if trakt_select_all %}
    SELECT trakt_status_id, tmdb_id, season, episode, last_watched_at
    FROM TRAKT_STATUS
    ORDER BY last_watched_at DESC
{% endif %}

{# Use this script to Update a Trakt_status record in MySQL DB #}
{% if tmdb_id and season and episode and last_watched_at and rating and rated_at %}
    UPDATE TRAKT_STATUS
    SET last_watched_at = {{last_watched_at}}, rating = {{rating}}, rated_at = {{rated_at}}
    WHERE tmdb_id = {{tmdb_id}} AND season = {{season}} AND episode = {{episode}};
{% endif %}

{# Use this script to INSERT a Trakt_status record in MySQL DB #}
{% if tmdb_id and season and episode and last_watched_at and rating and rated_at %}
    INSERT INTO TRAKT_STATUS
    (tmdb_id, season, episode, last_watched_at, rating, rated_at)
    VALUES ({{tmdb_id}}, {{season}}, {{episode}}, {{last_watched_at}}, {{rating}}, {{rated_at}});
{% endif %}
