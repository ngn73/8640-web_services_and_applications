
USE media_analytics_db;

/*
Use this to Drop and Recreate the stored procedures.
DROP PROCEDURE IF EXISTS InsertTraktStatus;
DROP PROCEDURE IF EXISTS UpdateTraktStatus;
DROP PROCEDURE IF EXISTS GetTraktStatus;
DROP PROCEDURE IF EXISTS ClearTraktStatus;
DROP PROCEDURE IF EXISTS GetDistinctTMDBIds;

DROP PROCEDURE IF EXISTS InsertTMDBShow;
DROP PROCEDURE IF EXISTS InsertTMDBSeason;
DROP PROCEDURE IF EXISTS InsertTMDBEpisode;
DROP PROCEDURE IF EXISTS InsertTMDBPerson;
DROP PROCEDURE IF EXISTS InsertTMDBEpisodeCast;
DROP PROCEDURE IF EXISTS InsertTMDBEpisodeCrew;
*/

-- Change delimiter so MySQL doesn't stop at first ;
DELIMITER $$

-- =========================================
-- Procedure 1: InsertTraktStatus
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
    INSERT INTO TRAKT_STATUS (trakt_status_id, tmdb_id, season, episode, last_watched_at, rating, rated_at)
    VALUES (p_trakt_status_id, p_tmdb_id, p_season, p_episode, p_last_watched_at, p_rating, p_rated_at);
END $$

-- =========================================
-- Procedure 2: UpdateTraktStatus
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
    SET last_watched_at = p_last_watched_at, rating = p_rating, rated_at = p_rated_at
    WHERE tmdb_id = p_tmdb_id AND season = p_season AND episode = p_episode;
END $$

-- =========================================
-- Procedure 3: SelectTraktStatus
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
-- Procedure 4: Clear TraktStatus Table
-- =========================================
CREATE PROCEDURE ClearTraktStatus()
BEGIN
    DELETE FROM TRAKT_STATUS;
END $$

-- =========================================
-- Procedure 5: Get Distinct TMDB Ids under "trakt_status" Table
-- =========================================
CREATE PROCEDURE GetDistinctTMDBIds()
BEGIN
    SELECT DISTINCT tmdb_id
    FROM TRAKT_STATUS
    WHERE tmdb_id IS NOT NULL
    ORDER BY tmdb_id;
END $$

-- =========================================
-- Procedure 6: INSERT TMDB Show Details
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
    IN p_number_of_episodes INT,
    IN p_poster_path VARCHAR(255)
)
BEGIN
    INSERT INTO TMDB_SHOW (
        tmdb_id, name, overview, first_air_date, status, vote_average, 
        vote_count, number_of_seasons, number_of_episodes, poster_path)
    VALUES 
    (p_tmdb_id, p_name, p_overview, p_first_air_date, p_status, p_vote_average, 
    p_vote_count, p_number_of_seasons, p_number_of_episodes, p_poster_path);
END $$

-- =========================================
-- Procedure 7: INSERT TMDB Season Details
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
    (p_tmdb_season_id, p_tmdb_show_id, p_season_number, p_name, p_overview, p_air_date, p_episode_count, p_poster_path);
END $$

-- =========================================
-- Procedure 8: INSERT TMDB Episode Details     
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
    (p_tmdb_episode_id, p_tmdb_show_id, p_season_number, p_episode_number, p_name, p_overview, p_air_date, p_runtime, p_vote_average, p_vote_count, p_still_path);
END $$  

-- =========================================
-- Procedure 9: INSERT TMDB Person Details     
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
    (p_tmdb_person_id, p_name, p_biography, p_birthday, p_gender, p_place_of_birth, p_profile_path);
END $$

-- ===============================================
-- Procedure 10: INSERT TMDB Episode Cast Details   
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
    (p_tmdb_episode_id, p_tmdb_person_id, p_character, p_order);
END $$

-- ===============================================
-- Procedure 11: INSERT TMDB Episode Crew Details   
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
    (p_tmdb_episode_id, p_tmdb_person_id, p_job, p_department);
END $$

-- ===============================================
-- Procedure 12: CLEAR TMDB Tables   
-- ===============================================
CREATE PROCEDURE ClearTMDBTables()
BEGIN
    DELETE FROM TMDB_EPISODE_CREW;
    DELETE FROM TMDB_EPISODE_CAST;
    DELETE FROM TMDB_PERSON;
    DELETE FROM TMDB_EPISODE;
    DELETE FROM TMDB_SEASON;
    DELETE FROM TMDB_SHOW;
END $$


-- Reset delimiter back to normal
DELIMITER ;