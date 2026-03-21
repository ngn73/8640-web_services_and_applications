'''
Name:dao_trakt.py
Description: 
Object-based Data Access Object (DAO) responsible for interacting with the database table "trakt_status".
'''

from . import dbManager # Required for database connections and queries
import pandas as pd
import app_logger as logger # for logging operations and errors

class dao_trakt:
    

    def __init__(self, mydb: dbManager):
        #Logger to use
        self.mylogger = logger.app_logger(__name__)
        self.db = mydb

    def bulk_insert(self, df: pd.DataFrame):
        for index, row in df.iterrows():
            tmdb_id = row['tmdb_id']
            season = row['season_number']
            episode = row['season_episode_number']
            last_watched_at = row['last_watched_at']
            rating = row.get('rating')
            rated_at = row.get('rated_at')

            # Check if the record already exists in the database
            existing_status = self.get_status(tmdb_id, season, episode)

            if existing_status:
                # If it exists, update the record
                self.update_status(tmdb_id, season, episode, last_watched_at, rating, rated_at)
            else:
                # If it doesn't exist, insert a new record
                self.insert_status(tmdb_id, season, episode, last_watched_at, rating, rated_at)
    
    def insert_status(self, tmdb_id: str, season: int, episode: int, last_watched_at, rating=None, rated_at=None):
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    args = (tmdb_id, season, episode, last_watched_at, rating, rated_at)
                    cur.callproc("InsertTraktStatus", args)
                conn.commit()
            except:
                conn.rollback()
                raise

    def update_status(self, tmdb_id: str, season: int, episode: int, last_watched_at, rating=None, rated_at=None):
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    args = (tmdb_id, season, episode, last_watched_at, rating, rated_at)
                    cur.callproc("UpdateTraktStatus", args)
                conn.commit()
            except:
                conn.rollback()
                raise

    def get_status(self, tmdb_id, season, episode):
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    args = (tmdb_id, season, episode)
                    cur.callproc("GetTraktStatus", args)
                conn.commit()
            except:
                conn.rollback()
                raise
    
