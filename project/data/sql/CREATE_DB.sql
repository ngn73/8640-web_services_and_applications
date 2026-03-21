-- Create Database
CREATE DATABASE IF NOT EXISTS MEDIA_ANALYTICS_DB
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

-- Use Database
USE MEDIA_ANALYTICS_DB;

-- Create Table
CREATE TABLE TRAKT_STATUS (
    trakt_status_id CHAR(36) PRIMARY KEY,  
    tmdb_id VARCHAR(100),
    season INT,
    episode INT,
    last_watched_at DATETIME,
    rating INT,
    rated_at DATETIME,

    UNIQUE KEY uniq_episode (tmdb_id, season, episode),
    INDEX idx_tmdb (tmdb_id),
    INDEX idx_last_watched (last_watched_at)
);

