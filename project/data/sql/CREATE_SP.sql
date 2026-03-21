
USE media_analytics_db;

-- Change delimiter so MySQL doesn't stop at first ;
DELIMITER $$

-- =========================================
-- Procedure 1: InsertTraktStatus
-- =========================================
CREATE PROCEDURE InsertTraktStatus(
    IN p_tmdb_id VARCHAR(100),
    IN p_season INT,
    IN p_episode INT,
    IN p_last_watched_at DATETIME,
    IN p_rating INT,
    IN p_rated_at DATETIME
)
BEGIN
    INSERT INTO TRAKT_STATUS (tmdb_id, season, episode, last_watched_at, rating, rated_at)
    VALUES (p_tmdb_id, p_season, p_episode, p_last_watched_at, p_rating, p_rated_at);
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
    SELECT trakt_status_id, tmdb_id, season, episode, last_watched_at, rating, rated_at
    FROM TRAKT_STATUS
    WHERE tmdb_id = p_tmdb_id 
    AND season = p_season 
    AND episode = p_episode
    ORDER BY last_watched_at DESC;
END $$

-- Reset delimiter back to normal
DELIMITER ;