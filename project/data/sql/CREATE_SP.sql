
USE media_analytics_db;


-- Initially Drop all the stored procedures.
DROP PROCEDURE IF EXISTS GetTraktAuth;
DROP PROCEDURE IF EXISTS UpdateTraktAuth;
DROP PROCEDURE IF EXISTS InsertTraktStatus;
DROP PROCEDURE IF EXISTS UpdateTraktStatus;
DROP PROCEDURE IF EXISTS GetTraktStatus;
DROP PROCEDURE IF EXISTS ClearTraktStatus;
DROP PROCEDURE IF EXISTS GetDistinctTMDBIds;
DROP PROCEDURE IF EXISTS GetAllShows;
DROP PROCEDURE IF EXISTS GetShowDetailsByTMDBId;
DROP PROCEDURE IF EXISTS GetSeasonDetailsByTMDBId;
DROP PROCEDURE IF EXISTS GetSeasonEpisodeDetailsByTMDBId;
DROP PROCEDURE IF EXISTS GetSeasonCastCrewByTMDBId;
DROP PROCEDURE IF EXISTS GetSeasonEpisodeDetailsByTMDBId;
DROP PROCEDURE IF EXISTS GetEpisodeCastByTMDBId;
DROP PROCEDURE IF EXISTS InsertTMDBShow;
DROP PROCEDURE IF EXISTS InsertTMDBSeason;
DROP PROCEDURE IF EXISTS InsertTMDBEpisode;
DROP PROCEDURE IF EXISTS InsertTMDBPerson;
DROP PROCEDURE IF EXISTS InsertTMDBEpisodeCast;
DROP PROCEDURE IF EXISTS InsertTMDBEpisodeCrew;
DROP PROCEDURE IF EXISTS ClearTMDBTables;
DROP PROCEDURE IF EXISTS InsertTMDBShowNetwork;
DROP PROCEDURE IF EXISTS InsertTMDBNetwork;
DROP PROCEDURE IF EXISTS Get_TMDB_Trakt_Delta;
DROP PROCEDURE IF EXISTS InsertTMDBShowArtwork;
DROP PROCEDURE IF EXISTS GetRndArtwork;
DROP PROCEDURE IF EXISTS GetRatedArtwork;


-- Change delimiter so MySQL doesn't stop at first ;
DELIMITER $$


-- =================================
-- Procedure : GetTraktAuth
-- Get Trakt Authentication Details
-- =================================
CREATE PROCEDURE GetTraktAuth()
BEGIN
    SELECT 
        auth_id,
        client_id,
        client_secret,
        redirect_uri,
        access_token,
        refresh_token,
        token_type,
        expires_in,
        created_at,
        refreshed_at
    FROM TRAKT_AUTH
    LIMIT 1;
END $$


-- =================================
-- Procedure : UpdateTraktAuth
-- Update Trakt Authentication Details
-- =================================
CREATE PROCEDURE UpdateTraktAuth(
    IN p_access_token CHAR(36),
    IN p_refresh_token TEXT,
    IN p_token_type VARCHAR(50),
    IN p_expires_in INT,
    IN p_created_at BIGINT
)
BEGIN
    UPDATE TRAKT_AUTH
    SET access_token = p_access_token,
        refresh_token = p_refresh_token,
        token_type = p_token_type,
        expires_in = p_expires_in,
        created_at = p_created_at,
        refreshed_at = NOW(),
        updated_at = NOW(),
        last_refresh_status = 'SUCCESS',
        last_refresh_error = NULL
    WHERE auth_id = 1;
END $$

-- =========================================
-- Procedure : InsertTraktStatus
-- Insert trakt status for a specific 
-- TMDB show, season, and episode
-- =========================================
CREATE PROCEDURE InsertTraktStatus(
    IN p_trakt_status_id CHAR(36),
    IN p_tmdb_id VARCHAR(100),
    IN p_season INT,
    IN p_episode INT,
    IN p_last_watched_at DATETIME,
    IN p_rating INT,
    IN p_rated_at DATETIME
)
BEGIN
    INSERT INTO TRAKT_STATUS (trakt_status_id, tmdb_id, season, episode, last_watched_at, rating, rated_at, updated_at)
    VALUES (p_trakt_status_id, p_tmdb_id, p_season, p_episode, p_last_watched_at, p_rating, p_rated_at, NOW());
END $$

-- =========================================
-- Procedure : UpdateTraktStatus
-- Update trakt status for a specific 
-- TMDB show, season, and episode
-- =========================================
CREATE PROCEDURE UpdateTraktStatus(
    IN p_tmdb_id VARCHAR(100),
    IN p_season INT,
    IN p_episode INT,
    IN p_last_watched_at DATETIME,
    IN p_rating INT,
    IN p_rated_at DATETIME
)
BEGIN
    UPDATE TRAKT_STATUS 
    SET last_watched_at = p_last_watched_at, rating = p_rating, rated_at = p_rated_at, updated_at = NOW()
    WHERE tmdb_id = p_tmdb_id AND season = p_season AND episode = p_episode;
END $$

-- =========================================
-- Procedure : GetTraktStatus
-- Get trakt status for a specific TMDB show, season, and episode
-- =========================================
CREATE PROCEDURE GetTraktStatus(
    IN p_tmdb_id VARCHAR(100),
    IN p_season INT,
    IN p_episode INT
)
BEGIN
    SELECT trakt_status_id, trakt_status_id, tmdb_id, season, episode, last_watched_at, rating, rated_at
    FROM TRAKT_STATUS
    WHERE tmdb_id = p_tmdb_id 
    AND season = p_season 
    AND episode = p_episode
    ORDER BY last_watched_at DESC;
END $$

-- =========================================
-- Procedure : ClearTraktStatus
-- Delete all data from "trakt_status" Table
-- =========================================
CREATE PROCEDURE ClearTraktStatus()
BEGIN
    DELETE FROM TRAKT_STATUS;
    SELECT COUNT(*) AS remaining_rows FROM TRAKT_STATUS;
END $$

-- =========================================
-- Procedure : GetDistinctTMDBIds
-- Get Distinct TMDB Ids under "trakt_status" Table
-- =========================================
CREATE PROCEDURE GetDistinctTMDBIds()
BEGIN
    SELECT DISTINCT tmdb_id
    FROM TRAKT_STATUS
    WHERE tmdb_id IS NOT NULL
    ORDER BY tmdb_id;
END $$

-- =====================================================================================
-- Procedure : GetAllShows
-- Get details for all shows in the database,
-- Show details, a (single) Poster for each show, and the latest watched season/episode
-- =====================================================================================
CREATE PROCEDURE GetAllShows()
BEGIN
SELECT
    TMDB.tmdb_id AS tmdb_id,
    TMDB.name AS name,
    TMDB.overview AS overview,
    TMDB.first_air_date AS first_air_date,
    TMDB.status AS status,
    TMDB.vote_average AS vote_average,
    TMDB.vote_count AS vote_count,
    TMDB.number_of_seasons AS number_of_seasons,
    TMDB.number_of_episodes AS number_of_episodes,
    TK.latest_season AS latest_season,
    TK.latest_episode AS latest_episode,
    (
        SELECT A.file_path
        FROM TMDB_SHOW_ARTWORK A
        WHERE A.artwork_type = 'poster'
        AND A.tmdb_show_id = TMDB.tmdb_id
        ORDER BY A.vote_average DESC
        LIMIT 1
    ) AS poster_path
FROM TMDB_SHOW TMDB
LEFT JOIN (
    SELECT
        tmdb_id,
        FLOOR(MAX(season * 10000 + episode) / 10000) AS latest_season,
        MOD(MAX(season * 10000 + episode), 10000) AS latest_episode
    FROM TRAKT_STATUS
    GROUP BY tmdb_id
) TK
    ON TMDB.tmdb_id = TK.tmdb_id;
END $$


-- ==========================================
-- Procedure : GetShowDetailsByTMDBId
-- Get Show Details for a specific TMDB Id
-- ==========================================
CREATE PROCEDURE GetShowDetailsByTMDBId(
    IN p_tmdb_id VARCHAR(100)
)
BEGIN
    SELECT 
        tmdb_id,
        name,
        overview,
        first_air_date,
        status,
        vote_average,
        vote_count,
        number_of_seasons,
        number_of_episodes
    FROM TMDB_SHOW
    WHERE tmdb_id = p_tmdb_id;
END $$


-- ==========================================================
-- Procedure : GetSeasonDetailsByTMDBId
-- Get Season Details for a specific TMDB Id
-- (use -1 for season_number to get all seasons for the show)
-- ==========================================================
CREATE PROCEDURE GetSeasonDetailsByTMDBId(
    IN p_tmdb_show_id VARCHAR(100),
    IN p_season_number INT
)
BEGIN
    SELECT 
        tmdb_season_id,
        tmdb_show_id,   
        season_number,
        name,
        overview,
        air_date,
        episode_count,
        poster_path
    FROM TMDB_SEASON
    WHERE 
    tmdb_show_id = p_tmdb_show_id
    AND (p_season_number = -1 OR season_number = p_season_number);
END $$

-- =================================================================================
-- Procedure : GetEpisodeDetailsByTMDBId
-- Get Episode Details for a specific TMDB Id   
-- (use -1 for episode_number to get all episodes for the season)
-- =================================================================================
CREATE PROCEDURE GetSeasonEpisodeDetailsByTMDBId(
    IN p_tmdb_show_id VARCHAR(100),
    IN p_season_number INT,
    IN p_episode_number INT
)
BEGIN
    SELECT 
        tmdb_episode_id,
        tmdb_show_id,
        season_number,
        episode_number,
        name,
        overview,
        air_date,
        runtime,
        vote_average,
        vote_count,
        still_path,
        last_watched_at,
        rating
    FROM TMDB_EPISODE E
    LEFT JOIN TRAKT_STATUS T
    ON E.tmdb_show_id = T.tmdb_id AND E.season_number = T.season AND E.episode_number = T.episode
    WHERE tmdb_show_id = p_tmdb_show_id
    AND season_number = p_season_number
    AND (p_episode_number = -1 OR episode_number = p_episode_number);
END $$

-- =========================================
-- Procedure : GetSeasonCastCrewByTMDBId
-- Get Season Cast and Crew Details for a specific TMDB Id
-- =========================================
CREATE PROCEDURE GetSeasonCastCrewByTMDBId(
    IN p_tmdb_show_id VARCHAR(100),
    IN p_season_number INT
)
BEGIN
    SELECT 
        person_id,
        name,
        character_name,
        profile_path
    FROM TMDB_SEASON_CAST_CREW
    WHERE tmdb_show_id = p_tmdb_show_id
    AND season_number = p_season_number;
END $$

-- ================================================
-- Procedure : GetEpisodeCastByTMDBId
-- Get Episode Cast Details for a specific TMDB Id
-- ================================================    
CREATE PROCEDURE GetEpisodeCastByTMDBId(
    IN p_tmdb_show_id VARCHAR(100),
    IN p_season_number INT,
    IN p_episode_number INT
)   
BEGIN
    SELECT 
        P.tmdb_person_id,
        P.person_name,
        P.biography,
        P.birthday,
        P.place_of_birth,
        C.character_name,
        C.cast_order,
        P.profile_path
    FROM TMDB_EPISODE_CAST C
    INNER JOIN TMDB_PERSON P 
        ON C.tmdb_person_id = P.tmdb_person_id
    INNER JOIN TMDB_EPISODE E
        ON C.tmdb_episode_id = E.tmdb_episode_id
    WHERE E.tmdb_show_id = p_tmdb_show_id
    AND E.season_number = p_season_number
    AND E.episode_number = p_episode_number
    ORDER BY C.cast_order;
END $$


-- =========================================
-- Procedure : InsertTMDBShow
-- INSERT TMDB Show Details
-- =========================================
CREATE PROCEDURE InsertTMDBShow(
    IN p_tmdb_id VARCHAR(100),
    IN p_name VARCHAR(255),
    IN p_overview TEXT,
    IN p_first_air_date DATE,
    IN p_status VARCHAR(100),
    IN p_vote_average DECIMAL(3, 1),
    IN p_vote_count INT,
    IN p_number_of_seasons INT,
    IN p_number_of_episodes INT
)
BEGIN
    INSERT INTO TMDB_SHOW (
        tmdb_id, name, overview, first_air_date, status, vote_average, 
        vote_count, number_of_seasons, number_of_episodes)
    VALUES 
    (p_tmdb_id, p_name, p_overview, p_first_air_date, p_status, p_vote_average, 
    p_vote_count, p_number_of_seasons, p_number_of_episodes)
    ON DUPLICATE KEY UPDATE
        name = VALUES(name),
        overview = VALUES(overview),
        first_air_date = VALUES(first_air_date),
        status = VALUES(status),
        vote_average = VALUES(vote_average),
        vote_count = VALUES(vote_count),
        number_of_seasons = VALUES(number_of_seasons),
        number_of_episodes = VALUES(number_of_episodes);
END $$

-- =========================================
-- Procedure : InsertTMDBSeason
-- INSERT TMDB Season Details
-- =========================================
CREATE PROCEDURE InsertTMDBSeason(
    IN p_tmdb_season_id INT,
    IN p_tmdb_show_id INT,
    IN p_season_number INT,
    IN p_name VARCHAR(255),
    IN p_overview TEXT,
    IN p_air_date DATE,
    IN p_episode_count INT,
    IN p_poster_path VARCHAR(255)
)
BEGIN
    INSERT INTO TMDB_SEASON (
        tmdb_season_id, tmdb_show_id, season_number, name, overview, air_date, episode_count, poster_path)
    VALUES 
    (p_tmdb_season_id, p_tmdb_show_id, p_season_number, p_name, p_overview, p_air_date, p_episode_count, p_poster_path)
    ON DUPLICATE KEY UPDATE
        name = VALUES(name),
        overview = VALUES(overview),
        air_date = VALUES(air_date),
        episode_count = VALUES(episode_count),
        poster_path = VALUES(poster_path);
END $$

-- =========================================
-- Procedure : InsertTMDBEpisode
-- INSERT TMDB Episode Details     
-- =========================================
CREATE PROCEDURE InsertTMDBEpisode(
    IN p_tmdb_episode_id INT,
    IN p_tmdb_show_id INT,
    IN p_season_number INT,
    IN p_episode_number INT,
    IN p_name VARCHAR(255),
    IN p_overview TEXT,
    IN p_air_date DATE,
    IN p_runtime INT,
    IN p_vote_average DECIMAL(3,1),
    IN p_vote_count INT,
    IN p_still_path VARCHAR(255)
)           
BEGIN
    INSERT INTO TMDB_EPISODE (
        tmdb_episode_id, tmdb_show_id, season_number, episode_number, name, overview, air_date, runtime, vote_average, vote_count, still_path)
    VALUES 
    (p_tmdb_episode_id, p_tmdb_show_id, p_season_number, p_episode_number, p_name, p_overview, p_air_date, p_runtime, p_vote_average, p_vote_count, p_still_path)
    ON DUPLICATE KEY UPDATE
        name = VALUES(name),
        overview = VALUES(overview),
        air_date = VALUES(air_date),
        runtime = VALUES(runtime),
        vote_average = VALUES(vote_average),
        vote_count = VALUES(vote_count),
        still_path = VALUES(still_path);
END $$  

-- =========================================
-- Procedure : InsertTMDBPerson   
-- Insert TMDB Person Details (for Cast and Crew)  
-- =========================================
CREATE PROCEDURE InsertTMDBPerson(
    IN p_tmdb_person_id INT,
    IN p_name VARCHAR(255),
    IN p_biography TEXT,
    IN p_birthday DATE,
    IN p_gender VARCHAR(50),
    IN p_place_of_birth VARCHAR(255),
    IN p_profile_path VARCHAR(255)
)
BEGIN
    INSERT INTO TMDB_PERSON (
        tmdb_person_id, person_name, biography, birthday, gender, place_of_birth, profile_path)
    VALUES 
    (p_tmdb_person_id, p_name, p_biography, p_birthday, p_gender, p_place_of_birth, p_profile_path)
    ON DUPLICATE KEY UPDATE
        person_name = VALUES(person_name),
        biography   = VALUES(biography),
        birthday    = VALUES(birthday),
        gender      = VALUES(gender),
        place_of_birth = VALUES(place_of_birth),            
        profile_path = VALUES(profile_path);
END $$

-- ===============================================
-- Procedure : InsertTMDBEpisodeCast
-- INSERT TMDB Episode Cast Details   
-- ===============================================
CREATE PROCEDURE InsertTMDBEpisodeCast(
    IN p_tmdb_episode_id INT,
    IN p_tmdb_person_id INT,
    IN p_character VARCHAR(255),
    IN p_order INT
)
BEGIN
    INSERT INTO TMDB_EPISODE_CAST (
        tmdb_episode_id, tmdb_person_id, character_name, cast_order)
    VALUES 
    (p_tmdb_episode_id, p_tmdb_person_id, p_character, p_order)
    ON DUPLICATE KEY UPDATE
        character_name = VALUES(character_name),
        cast_order     = VALUES(cast_order);
END $$

-- ===============================================
-- Procedure : InsertTMDBEpisodeCrew
-- INSERT TMDB Episode Crew Details   
-- ===============================================
CREATE PROCEDURE InsertTMDBEpisodeCrew(
    IN p_tmdb_episode_id INT,
    IN p_tmdb_person_id INT,
    IN p_job VARCHAR(255),
    IN p_department VARCHAR(255)
)
BEGIN
    INSERT INTO TMDB_EPISODE_CREW (
        tmdb_episode_id, tmdb_person_id, job, department)
    VALUES 
    (p_tmdb_episode_id, p_tmdb_person_id, p_job, p_department)
    ON DUPLICATE KEY UPDATE
        job = VALUES(job),
        department = VALUES(department);
END $$

-- ===============================================
-- Procedure : ClearTMDBTables
-- DELETE all data from TMDB-related tables
-- ===============================================
CREATE PROCEDURE ClearTMDBTables()
BEGIN
    DELETE FROM TMDB_EPISODE_CREW;
    DELETE FROM TMDB_EPISODE_CAST;
    DELETE FROM TMDB_PERSON;
    DELETE FROM TMDB_EPISODE;
    DELETE FROM TMDB_SEASON;
    DELETE FROM TMDB_SHOW_NETWORK;
    DELETE FROM TMDB_NETWORK;
    DELETE FROM TMDB_SHOW;
    DELETE FROM TMDB_SHOW_ARTWORK;

    SELECT 
        (SELECT COUNT(*) FROM TMDB_SHOW) AS remaining_shows,
        (SELECT COUNT(*) FROM TMDB_SEASON) AS remaining_seasons,
        (SELECT COUNT(*) FROM TMDB_EPISODE) AS remaining_episodes,
        (SELECT COUNT(*) FROM TMDB_PERSON) AS remaining_persons,
        (SELECT COUNT(*) FROM TMDB_EPISODE_CAST) AS remaining_episode_cast,
        (SELECT COUNT(*) FROM TMDB_EPISODE_CREW) AS remaining_episode_crew,
        (SELECT COUNT(*) FROM TMDB_SHOW_NETWORK) AS remaining_show_network,
        (SELECT COUNT(*) FROM TMDB_NETWORK) AS remaining_networks,
        (SELECT COUNT(*) FROM TMDB_SHOW_ARTWORK) AS remaining_show_artwork;

END $$

-- ===============================================
-- Procedure : InsertTMDBShowNetwork
-- INSERT TMDB Network Details (for a Show)      
-- ===============================================
CREATE PROCEDURE InsertTMDBShowNetwork(
    IN p_tmdb_network_id INT,
    IN p_tmdb_show_id INT
)
BEGIN
    INSERT INTO TMDB_SHOW_NETWORK (
        tmdb_network_id, tmdb_show_id)
    VALUES 
    (p_tmdb_network_id, p_tmdb_show_id)
    ON DUPLICATE KEY UPDATE
        tmdb_network_id = VALUES(tmdb_network_id),
        tmdb_show_id    = VALUES(tmdb_show_id);
END $$

-- ===============================================
-- Procedure : InsertTMDBNetwork
-- INSERT TMDB Network Details
-- ===============================================
CREATE PROCEDURE InsertTMDBNetwork(
    IN p_tmdb_network_id INT,
    IN p_name VARCHAR(255),
    IN p_origin_country VARCHAR(10),
    IN p_logo_path VARCHAR(255)
)
BEGIN
    INSERT INTO TMDB_NETWORK (
        tmdb_network_id, name, origin_country, logo_path)
    VALUES 
    (p_tmdb_network_id, p_name, p_origin_country, p_logo_path)
    ON DUPLICATE KEY UPDATE
        name            = VALUES(name),
        origin_country  = VALUES(origin_country),
        logo_path       = VALUES(logo_path);
END $$

-- ===============================================
-- Procedure : Get_TMDB_Trakt_Delta
-- Get TMDb-Trakt Differences
-- ===============================================
CREATE PROCEDURE Get_TMDB_Trakt_Delta(
)
BEGIN
    SELECT DISTINCT ts.tmdb_id
    FROM TRAKT_STATUS ts
    LEFT JOIN TMDB_SHOW s
    ON s.tmdb_id = ts.tmdb_id
    WHERE ts.tmdb_id IS NOT NULL
    AND s.tmdb_id IS NULL;
END $$

-- =====================================================
-- Procedure : InsertTMDBShowArtwork
-- Insert TMDb Show Artwork Details for a specific show
-- =====================================================
CREATE PROCEDURE InsertTMDBShowArtwork(
    IN p_tmdb_show_id INT,
    IN p_file_path VARCHAR(255),
    IN p_artwork_type VARCHAR(50),
    IN p_width INT,
    IN p_height INT,
    IN p_vote_average DECIMAL(3,1)
)
BEGIN
    INSERT INTO TMDB_SHOW_ARTWORK (
        tmdb_show_id, file_path, artwork_type, width, height, vote_average)
    VALUES 
    (p_tmdb_show_id, p_file_path, p_artwork_type, p_width, p_height, p_vote_average)
    ON DUPLICATE KEY UPDATE
        width = VALUES(width),
        height = VALUES(height),
        vote_average = VALUES(vote_average);
END $$


-- ==========================================================
-- Procedure : GetRndArtwork
-- Get a random artwork for a specific show and artwork type
-- ==========================================================
CREATE PROCEDURE GetRndArtwork(
    IN p_tmdb_show_id INT,
    IN p_artwork_type VARCHAR(50)
)
BEGIN
    SELECT
        file_path,
        artwork_type,
        width,
        height
    FROM TMDB_SHOW_ARTWORK
    WHERE tmdb_show_id = p_tmdb_show_id
        AND artwork_type = p_artwork_type
    ORDER BY RAND()
    LIMIT 1;
END $$

-- ==========================================================
-- Procedure : GetRatedArtwork
-- Get most rated artwork for a specific show and artwork type
-- ==========================================================
CREATE PROCEDURE GetRatedArtwork(
    IN p_tmdb_show_id INT,
    IN p_artwork_type VARCHAR(50)
)
BEGIN
    SELECT
        file_path,
        artwork_type,
        width,
        height
    FROM TMDB_SHOW_ARTWORK
    WHERE tmdb_show_id = p_tmdb_show_id
        AND artwork_type = p_artwork_type
    ORDER BY vote_average DESC
    LIMIT 1;
END $$


-- Reset delimiter back to normal
DELIMITER ;