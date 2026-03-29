'''
Name:dao_tmdb.py
Description: 
Object-based Data Access Object (DAO) responsible for interacting with the database tables "tmdb_show", "tmdb_season", and "tmdb_episode".
'''

from . import dbManager # Required for database connections and queries
import pandas as pd
import app_logger as logger # for logging operations and errors

class dao_tmdb:

    def __init__(self, mydb: dbManager):
        #Logger to use
        self.mylogger = logger.app_logger(__name__)
        self.db = mydb

    def bulk_insert_shows(self, shows_rows: list):
        for row in shows_rows:
            tmdb_id = row['tmdb_id']
            name = row['name']
            overview = row['overview']
            first_air_date = row['first_air_date']
            status = row['status']
            vote_average = row['vote_average']
            vote_count = row['vote_count']
            number_of_seasons = row['number_of_seasons']
            number_of_episodes = row['number_of_episodes']
            poster_path = row['poster_path']

            self.insert_show(tmdb_id, name, overview, first_air_date, status, vote_average, vote_count, number_of_seasons, number_of_episodes, poster_path)

        
    def insert_show(self, tmdb_id: str, name: str, overview: str, first_air_date: str, status: str, vote_average: float, vote_count: int, number_of_seasons: int, number_of_episodes: int, poster_path: str):
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    args = (tmdb_id, name, overview, first_air_date, status, vote_average, vote_count, number_of_seasons, number_of_episodes, poster_path)
                    cur.callproc("InsertTMDBShow", args)
                conn.commit()
            except:
                conn.rollback()
                raise

    def bulk_insert_seasons(self, seasons_rows: list):
        for row in seasons_rows:
            tmdb_season_id = row['tmdb_season_id']
            tmdb_show_id = row['tmdb_id']
            season_number = row['season_number']
            name = row['name']
            overview = row['overview']
            air_date = row['air_date']
            episode_count = row['episode_count']
            poster_path = row['poster_path']

            self.insert_season(tmdb_season_id, tmdb_show_id, season_number, name, air_date, episode_count, overview, poster_path)

    def insert_season(self, tmdb_season_id: int, tmdb_show_id: int, season_number: int, name:str, air_date: str, episode_count: int, overview: str, poster_path: str):
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    args = (tmdb_season_id, tmdb_show_id, season_number, name, air_date, episode_count, overview, poster_path)
                    cur.callproc("InsertTMDBSeason", args)
                conn.commit()
            except:
                conn.rollback()
                raise

    def bulk_insert_episodes(self, episodes_rows: list):
        for row in episodes_rows:
            tmdb_episode_id = row['tmdb_episode_id']
            tmdb_show_id = row['tmdb_show_id']
            season_number = row['season_number']
            episode_number = row['episode_number']
            name = row['name']
            overview = row['overview']
            air_date = row['air_date']
            runtime = row['runtime']
            vote_average = row['vote_average']
            vote_count = row['vote_count']
            still_path = row['still_path']

            self.insert_episode(tmdb_episode_id, tmdb_show_id, season_number, episode_number, name, overview, air_date, runtime,  vote_average, vote_count, still_path)
    
    def insert_episode(self, tmdb_episode_id: int, tmdb_show_id: int, season_number: int, episode_number: int, name: str, overview: str, air_date: str, runtime: int,  vote_average: float, vote_count: int, still_path: str):
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    args = (tmdb_episode_id, tmdb_show_id, season_number, episode_number, name, overview, air_date, runtime, vote_average, vote_count, still_path)
                    cur.callproc("InsertTMDBEpisode", args)
                conn.commit()
            except:
                conn.rollback()
                raise   
    
    def bulk_insert_person(self, person_rows: list):
        for row in person_rows:
            person_id = row['tmdb_person_id']
            name = row['person_name']
            biography = row['biography']
            birthday = row['birthday']
            gender = row['gender']
            place_of_birth = row['place_of_birth']
            profile_path = row['profile_path']  

            self.insert_person(person_id, name, biography, birthday, gender, place_of_birth, profile_path)


    def insert_person(self, person_id: int, name: str, biography: str, birth_date: str,gender: int,  place_of_birth: str, profile_path: str):
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    args = (person_id, name, biography, birth_date, gender,  place_of_birth, profile_path)
                    cur.callproc("InsertTMDBPerson", args)  
                conn.commit()
            except:
                conn.rollback()
                raise   
    
    def bulk_insert_cast_crew(self, episode_cast_rows: list, episode_crew_rows: list):
        for row in episode_cast_rows:
            tmdb_episode_id = row['tmdb_episode_id']
            person_id = row['tmdb_person_id']
            character = row['character']
            order = row['order']

            self.insert_cast_member(tmdb_episode_id, person_id, character, order)

        for row in episode_crew_rows:
            tmdb_episode_id = row['tmdb_episode_id']
            person_id = row['tmdb_person_id']
            job = row['job']
            department = row['department']

            self.insert_crew_member(tmdb_episode_id, person_id, job, department)  

    def insert_crew_member(self, tmdb_episode_id: int, person_id: int, job: str, department: str):
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    args = (tmdb_episode_id, person_id, job, department)
                    cur.callproc("InsertTMDBEpisodeCrew", args)
                conn.commit()
            except:
                conn.rollback()
                raise

    def insert_cast_member(self, tmdb_episode_id: int, person_id: int, character: str, order: int):
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    args = (tmdb_episode_id, person_id, character, order)
                    cur.callproc("InsertTMDBEpisodeCast", args)
                conn.commit()
            except:
                conn.rollback()
                raise

    def clear_status(self):
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    cur.callproc("ClearTraktStatus")
                conn.commit()
            except:
                conn.rollback()
                raise
    
