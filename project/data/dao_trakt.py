'''
Name:dao_trakt.py
Description: 
Object-based Data Access Object (DAO) responsible for interacting with the database table "trakt_status".
'''

from copy import Error
from . import dbManager # Required for database connections and queries
import pandas as pd
import app_logger as logger # for logging operations and errors

class dao_trakt:
    

    def __init__(self, mydb: dbManager):
        #Logger to use
        self.mylogger = logger.app_logger(__name__)
        self.db = mydb

    def get_trakt_auth(self) -> dict:
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    cur.callproc("GetTraktAuth")

                    for res in cur.stored_results():
                        row = res.fetchone()
                        if row:
                            return {
                                "auth_id": row[0],
                                "client_id": row[1],
                                "client_secret": row[2],
                                "redirect_uri": row[3],
                                "access_token": row[4],
                                "refresh_token": row[5],
                                "token_type": row[6],
                                "expires_in": row[7],
                                "created_at": row[8],
                                "refreshed_at": row[9]
                            }
                        else:
                            return {}
            except Error as e:
                err = f"Error in get_trakt_auth: {str(e)}"
                conn.rollback()
                raise

    def update_trakt_auth(self, access_token: str, refresh_token: str, token_type: str, expires_in: int, created_at: int):
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    args = (access_token, refresh_token, token_type, expires_in, created_at)
                    cur.callproc("UpdateTraktAuth", args)
                conn.commit()
            except Error as e:
                err = f"Error in update_trakt_auth: {str(e)}"
                conn.rollback()
                raise   

    def bulk_insert(self, df: pd.DataFrame):
        # initially clear the table of all records
        self.clear_status()

        # populate the table with the new data
        for index, row in df.iterrows():
            tmdb_id = row['tmdb_id']
            season = row['season_number']
            episode = row['season_episode_number']
            last_watched_at = row['last_watched_at']
            rating = None if pd.isna(row["rating"]) else int(row["rating"])
            rated_at = None if pd.isna(row["rated_at"]) else row["rated_at"]
            trakt_status_id = f"{tmdb_id}_{season}_{episode}"

            # Check if the record already exists in the database
            existing_status = self.get_status(tmdb_id, season, episode)

            if existing_status:
                # If it exists, update the record
                self.update_status(tmdb_id, season, episode, last_watched_at, rating, rated_at)
            else:
                # If it doesn't exist, insert a new record
                self.insert_status(trakt_status_id, tmdb_id, season, episode, last_watched_at, rating, rated_at)

    def insert_status(self,trakt_status_id: str, tmdb_id: str, season: int, episode: int, last_watched_at, rating=None, rated_at=None):
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    args = (trakt_status_id, tmdb_id, season, episode, last_watched_at, rating, rated_at)
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

    def clear_status(self):
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    cur.callproc("ClearTraktStatus")
                conn.commit()
            except:
                conn.rollback()
                raise

    # Get Distinct list of TMDB Ids under "trakt_status" Table
    def get_distinct_shows(self): 
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    cur.callproc("GetDistinctTMDBIds")
                    results = []
                    for res in cur.stored_results():
                        results = [row[0] for row in res.fetchall()]

                    return results
            except Error as e:
                err = f"Error in getdistinct_shows: {str(e)}"
                conn.rollback()
                raise

    # Get "Delta" list of TMDB Ids under "trakt_status" Table
    # i.e. TMDB Ids that are in trakt table but not in tmdb tables
    def get_delta_shows(self):
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    cur.callproc("Get_TMDB_Trakt_Delta")
                    results = []
                    for res in cur.stored_results():
                        results = [row[0] for row in res.fetchall()]

                    return results
            except Error as e:
                err = f"Error in get_delta_shows: {str(e)}"
                conn.rollback()
                raise

