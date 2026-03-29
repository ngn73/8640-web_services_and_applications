-- Create Database
CREATE DATABASE IF NOT EXISTS MEDIA_ANALYTICS_DB
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

-- Use Database
USE MEDIA_ANALYTICS_DB;

/*
Use this to Drop and Recreate the TABLES.
DROP TABLE IF EXISTS TRAKT_STATUS;
DROP TABLE IF EXISTS TMDB_SHOW;
DROP TABLE IF EXISTS TMDB_SEASON;
DROP TABLE IF EXISTS TMDB_EPISODE;
DROP TABLE IF EXISTS TMDB_PERSON;
DROP TABLE IF EXISTS TMDB_EPISODE_CAST;
DROP TABLE IF EXISTS TMDB_EPISODE_CREW;
*/


-- Create Table
CREATE TABLE TRAKT_STATUS (
    trakt_status_id CHAR(36) PRIMARY KEY,  
    tmdb_id VARCHAR(100),
    season INT,
    episode INT,
    last_watched_at DATETIME,
    rating INT NULL,
    rated_at DATETIME NULL,

    UNIQUE KEY uniq_episode (tmdb_id, season, episode),
    INDEX idx_tmdb (tmdb_id),
    INDEX idx_last_watched (last_watched_at)
);

-- Create Table
CREATE TABLE TMDB_SHOW (
    tmdb_id VARCHAR(100) PRIMARY KEY,
    name VARCHAR(255),
    overview TEXT,
    first_air_date DATE,
    status VARCHAR(100),
    vote_average DECIMAL(3, 1),
    vote_count INT,
    number_of_seasons INT,
    number_of_episodes INT,
    poster_path VARCHAR(255)
);

CREATE TABLE TMDB_SEASON (
    tmdb_season_id INT PRIMARY KEY,     -- season.id from TMDb
    tmdb_show_id INT NOT NULL,          -- tv show id
    season_number INT NOT NULL,
    
    name VARCHAR(255),
    overview TEXT,
    air_date DATE,
    episode_count INT,
    poster_path VARCHAR(255),

    UNIQUE KEY uniq_show_season (tmdb_show_id, season_number)
);

CREATE TABLE TMDB_EPISODE (
    tmdb_episode_id INT PRIMARY KEY,   -- episode.id from TMDb
    tmdb_show_id INT NOT NULL,         -- tv_id
    season_number INT NOT NULL,
    episode_number INT NOT NULL,
    
    name VARCHAR(255),
    overview TEXT,
    air_date DATE,
    runtime INT,
    
    vote_average DECIMAL(3,1),
    vote_count INT,
    
    still_path VARCHAR(255),

    UNIQUE KEY uniq_episode (tmdb_show_id, season_number, episode_number)
);

CREATE TABLE TMDB_PERSON (
    tmdb_person_id INT PRIMARY KEY,
    person_name VARCHAR(255) NOT NULL,
    biography  TEXT NULL,
    birthday DATE NULL,
    gender VARCHAR(50) NULL,
    place_of_birth VARCHAR(255) NULL,
    profile_path VARCHAR(255)
);

CREATE TABLE TMDB_EPISODE_CAST (
    tmdb_episode_id INT NOT NULL,
    tmdb_person_id INT NOT NULL,
    character_name VARCHAR(255),
    cast_order INT,
    
    PRIMARY KEY (tmdb_episode_id, tmdb_person_id),

    CONSTRAINT fk_episode_cast_episode
        FOREIGN KEY (tmdb_episode_id) REFERENCES TMDB_EPISODE(tmdb_episode_id),

    CONSTRAINT fk_episode_cast_person
        FOREIGN KEY (tmdb_person_id) REFERENCES TMDB_PERSON(tmdb_person_id)
);

CREATE TABLE TMDB_EPISODE_CREW (
    tmdb_episode_id INT NOT NULL,
    tmdb_person_id INT NOT NULL,
    job VARCHAR(255),
    department VARCHAR(255),
    
    PRIMARY KEY (tmdb_episode_id, tmdb_person_id),

    CONSTRAINT fk_episode_crew_episode
        FOREIGN KEY (tmdb_episode_id) REFERENCES TMDB_EPISODE(tmdb_episode_id),

    CONSTRAINT fk_episode_crew_person
        FOREIGN KEY (tmdb_person_id) REFERENCES TMDB_PERSON(tmdb_person_id)
);