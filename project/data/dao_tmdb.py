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

    # Get all shows in the database (for home page)
    def get_all_shows(self) -> list:
        shows = []

        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn, dictionary=True) as cur:
                    cur.callproc("GetAllShows")

                    for res in cur.stored_results():
                        columns = res.column_names
                        rows = res.fetchall()

                        for row in rows:
                            if isinstance(row, dict):
                                shows.append(row)
                            else:
                                shows.append(dict(zip(columns, row)))

            except Exception as ex:
                self.mylogger.logErrorMessage(
                    f"dao_tmdb.get_all_shows -- Error retrieving all shows: {ex}"
                )
                conn.rollback()
                raise

        return shows

    # Get details for a show by TMDb ID (for show details page)
    def get_show_details(self, tmdb_show_id: int) -> dict:
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn, dictionary=True) as cur:
                    args = (tmdb_show_id,)
                    cur.callproc("GetShowDetailsByTMDBId", args)

                    for res in cur.stored_results():
                        columns = res.column_names
                        row = res.fetchone()

                        if not row:
                            return {}

                        if isinstance(row, dict):
                            return row
                        else:
                            return dict(zip(columns, row))

                return {}  # fallback (no result sets)

            except Exception as ex:
                self.mylogger.logErrorMessage(
                    f"dao_tmdb.get_show_details -- Error retrieving show details with TMDb ID {tmdb_show_id}: {ex}"
                )
                conn.rollback()
                raise

    # Get details for all seasons for a show
    def get_season_details(self, tmdb_show_id: int) -> list:
        seasons = []

        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn, dictionary=True) as cur:
                    args = (tmdb_show_id, -1)
                    cur.callproc("GetSeasonDetailsByTMDBId", args)

                    for res in cur.stored_results():
                        columns = res.column_names
                        rows = res.fetchall()

                        for row in rows:
                            if isinstance(row, dict):
                                seasons.append(row)
                            else:
                                seasons.append(dict(zip(columns, row)))

                return seasons

            except Exception as ex:
                self.mylogger.logErrorMessage(
                    f"dao_tmdb.get_season_details -- Error retrieving show seasons with TMDb ID {tmdb_show_id}: {ex}"
                )
                conn.rollback()
                raise

    # Get details for a specific season of a show (same SP as above)
    def get_season_details_by_number(self, tmdb_show_id: int, season_number: int) -> dict:
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn, dictionary=True) as cur:
                    args = (tmdb_show_id, season_number)
                    cur.callproc("GetSeasonDetailsByTMDBId", args)

                    for res in cur.stored_results():
                        columns = res.column_names
                        row = res.fetchone()
                        if not row:
                            return {}

                        if isinstance(row, dict):
                            return row
                        else:
                            return dict(zip(columns, row))

                return {}  # fallback (no result sets)
            except:
                self.mylogger.logErrorMessage(f"dao_tmdb.get_season_details_by_number -- Error retrieving season details for show ID {tmdb_show_id}, season number {season_number}")
                conn.rollback()
                raise

    # Get details for all episodes for a specific season of a show
    def get_episode_details(self, tmdb_show_id: int, season_number: int) -> list:
        episodes = []

        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn, dictionary=True) as cur:
                    args = (tmdb_show_id, season_number, -1)  # Use -1 to indicate we want all episodes for the season
                    cur.callproc("GetSeasonEpisodeDetailsByTMDBId", args)

                    for res in cur.stored_results():
                        columns = res.column_names
                        rows = res.fetchall() or []

                        for row in rows:
                            if isinstance(row, dict):
                                episodes.append(row)
                            else:
                                episodes.append(dict(zip(columns, row)))
                return episodes
            except:
                self.mylogger.logErrorMessage(f"dao_tmdb.get_episode_details -- Error retrieving episode details for show ID {tmdb_show_id}, season number {season_number}")
                conn.rollback()
                raise

    def get_episode_details_by_number(self, tmdb_show_id: int, season_number: int, episode_number: int) -> dict:
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn, dictionary=True) as cur:
                    args = (tmdb_show_id, season_number, episode_number)
                    cur.callproc("GetSeasonEpisodeDetailsByTMDBId", args)

                    for res in cur.stored_results():
                        row = res.fetchone()
                        if isinstance(row, dict):
                            return row
                        else:
                            return dict(zip(res.column_names, row)) if row else {}
                return {}  # fallback (no result sets)
            except:
                self.mylogger.logErrorMessage(f"dao_tmdb.get_episode_details_by_number -- Error retrieving episode details for show ID {tmdb_show_id}, season number {season_number}, episode number {episode_number}")
                conn.rollback()
                raise

    def get_episode_cast(self, tmdb_show_id: int, season_number: int, episode_number: int) -> list:
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn, dictionary=True) as cur:
                    args = (tmdb_show_id, season_number, episode_number)
                    cur.callproc("GetEpisodeCastByTMDBId", args)

                    cast = []
                    for res in cur.stored_results():
                        rows = res.fetchall()
                        for row in rows:
                            if isinstance(row, dict):
                                cast.append(row)
                            else:
                                cast.append(dict(zip(res.column_names, row)))
                return cast
            except:
                self.mylogger.logErrorMessage(f"dao_tmdb.get_episode_cast -- Error retrieving episode cast details for show ID {tmdb_show_id}, season number {season_number}, episode number {episode_number}")
                conn.rollback()
                raise

    def get_episode_crew(self, tmdb_show_id: int, season_number: int, episode_number: int) -> list:
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn, dictionary=True) as cur:
                    args = (tmdb_show_id, season_number, episode_number)
                    cur.callproc("GetEpisodeCrewByTMDBId", args)

                    crew = []
                    for res in cur.stored_results():
                        rows = res.fetchall()
                        for row in rows:
                            if isinstance(row, dict):
                                crew.append(row)
                            else:
                                crew.append(dict(zip(res.column_names, row)))
                return crew
            except:
                self.mylogger.logErrorMessage(f"dao_tmdb.get_episode_crew -- Error retrieving episode crew details for show ID {tmdb_show_id}, season number {season_number}, episode number {episode_number}")
                conn.rollback()
                raise

    def get_person_details(self, tmdb_person_id: int) -> dict:
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn, dictionary=True) as cur:
                    args = (tmdb_person_id,)
                    cur.callproc("GetPersonDetails", args)

                    for res in cur.stored_results():
                        row = res.fetchone()
                        if isinstance(row, dict):
                            return row
                        else:
                            return dict(zip(res.column_names, row)) if row else {}
                return {}  # fallback (no result sets)
            except:
                self.mylogger.logErrorMessage(f"dao_tmdb.get_person_details -- Error retrieving person details for TMDb Person ID {tmdb_person_id}")
                conn.rollback()
                raise


    def get_person_related_roles(self, tmdb_person_id: int) -> list:
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn, dictionary=True) as cur:
                    args = (tmdb_person_id,)
                    cur.callproc("GetPersonRelatedRoles", args)

                    related_roles = []
                    for res in cur.stored_results():
                        rows = res.fetchall()
                        for row in rows:
                            if isinstance(row, dict):
                                related_roles.append(row)
                            else:
                                related_roles.append(dict(zip(res.column_names, row)))
                return related_roles
            except:
                self.mylogger.logErrorMessage(f"dao_tmdb.get_person_related_roles -- Error retrieving person related roles for TMDb Person ID {tmdb_person_id}")
                conn.rollback()
                raise

    def get_latest_watched_episode_details(self) -> list:
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn, dictionary=True) as cur:
                    cur.callproc("GetLatestWatchedEpisodeDetails")

                    latest_watched_episodes = []
                    for res in cur.stored_results():
                        rows = res.fetchall()
                        for row in rows:
                            if isinstance(row, dict):
                                latest_watched_episodes.append(row)
                            else:
                                latest_watched_episodes.append(dict(zip(res.column_names, row)))
                return latest_watched_episodes
            except:
                self.mylogger.logErrorMessage(f"dao_tmdb.get_latest_watched_episode_details -- Error retrieving latest watched episode details")
                conn.rollback()
                raise

    def  bulk_insert_shows(self, shows_rows: list):
        self.mylogger.logInfoMessage("Starting bulk insert of show details into database...")
        for row in shows_rows:
            tmdb_show_id = row['tmdb_show_id']
            name = row['name']
            overview = row['overview']
            first_air_date = row['first_air_date']
            status = row['status']
            vote_average = row['vote_average']
            vote_count = row['vote_count']
            number_of_seasons = row['number_of_seasons']
            number_of_episodes = row['number_of_episodes']

            self.insert_show(tmdb_show_id, name, overview, first_air_date, status, vote_average, vote_count, number_of_seasons, number_of_episodes)


    #These insert methods with 1 connection and 1 commit per row proved to be inefficient and crash-prone.
    def insert_show(self, tmdb_show_id: str, name: str, overview: str, first_air_date: str, status: str, vote_average: float, vote_count: int, number_of_seasons: int, number_of_episodes: int):
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    args = (tmdb_show_id, name, overview, first_air_date, status, vote_average, vote_count, number_of_seasons, number_of_episodes)
                    cur.callproc("InsertTMDBShow", args)
                conn.commit()
            except:
                self.mylogger.logErrorMessage(f"dao_tmdb.insert_show -- Error inserting show with TMDb ID {tmdb_show_id}, {name}, {overview}, {first_air_date}, {status}, {vote_average}, {vote_count}, {number_of_seasons}, {number_of_episodes}")
                conn.rollback()
                raise

    #Improved efficiency
    def insert_show_batch(self, rows: list[tuple[int, int]]):
        conn = None
        cur = None

        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    for show in rows:
                        args = (show["tmdb_show_id"], show["name"], show["overview"], show["first_air_date"], show["status"], show["vote_average"], show["vote_count"], show["number_of_seasons"], show["number_of_episodes"])
                        cur.callproc("InsertTMDBShow", args)
                conn.commit()
            except Exception as e:
                conn.rollback()
                self.mylogger.logErrorMessage(f"dao_tmdb.insert_show_batch -- Error inserting batch: {e}"
                )
                raise
            finally:
                if cur:
                    cur.close()
                if conn and conn.is_connected():
                    conn.close()

    def bulk_insert_seasons(self, seasons_rows: list):
        self.mylogger.logInfoMessage("Starting bulk insert of season details into database...")
        for row in seasons_rows:
            tmdb_season_id = row['tmdb_season_id']
            tmdb_show_id = row['tmdb_show_id']
            season_number = row['season_number']
            name = row['name']
            overview = row['overview']
            air_date = row['air_date']
            episode_count = row['episode_count']
            poster_path = row['poster_path']

            self.insert_season(tmdb_season_id, tmdb_show_id, season_number, name, air_date, episode_count, overview, poster_path)

    #These insert methods with 1 connection and 1 commit per row proved to be inefficient and crash-prone.
    def insert_season(self, tmdb_season_id: int, tmdb_show_id: int, season_number: int, name:str, air_date: str, episode_count: int, overview: str, poster_path: str):
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    args = (tmdb_season_id, tmdb_show_id, season_number, name, air_date, episode_count, overview, poster_path)
                    cur.callproc("InsertTMDBSeason", args)
                conn.commit()
            except:
                self.mylogger.logErrorMessage(f"dao_tmdb.insert_season -- Error inserting season with TMDb Season ID {tmdb_season_id}, TMDb Show ID {tmdb_show_id}, Season Number {season_number}, Name {name}, Air Date {air_date}, Episode Count {episode_count}, Overview {overview}, Poster Path {poster_path}")
                conn.rollback()
                raise

    #Improved efficiency
    def insert_season_batch(self, rows: list[tuple[int, int]]):
        conn = None
        cur = None

        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    for season in rows:
                        args = (season["tmdb_season_id"], season["tmdb_show_id"], season["season_number"], season["name"], season["overview"], season["air_date"], season["episode_count"],  season["poster_path"])
                        cur.callproc("InsertTMDBSeason", args)
                conn.commit()
            except Exception as e:
                conn.rollback()
                self.mylogger.logErrorMessage(f"dao_tmdb.insert_season_batch -- Error inserting batch: {e}"
                )
                raise
            finally:
                if cur:
                    cur.close()
                if conn and conn.is_connected():
                    conn.close()

    def bulk_insert_show_network(self, show_network_rows: list):
        self.mylogger.logInfoMessage("Starting bulk insert of show network details into database...")
        for row in show_network_rows:
            tmdb_network_id = row['tmdb_network_id']
            tmdb_show_id = row['tmdb_show_id']


            self.insert_show_network(tmdb_network_id, tmdb_show_id)

    #These insert methods with 1 connection and 1 commit per row proved to be inefficient and crash-prone.
    def insert_show_network(self, tmdb_network_id: int, tmdb_show_id: int):
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    args = (tmdb_network_id, tmdb_show_id)
                    cur.callproc("InsertTMDBShowNetwork", args)
                conn.commit()
            except:
                self.mylogger.logErrorMessage(f"dao_tmdb.insert_show_network -- Error inserting show network with TMDb Network ID {tmdb_network_id}, TMDb Show ID {tmdb_show_id}")
                conn.rollback()
                raise

    #Improved efficiency
    def insert_show_network_batch(self, rows: list[tuple[int, int]]):
        conn = None
        cur = None

        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    for show_network in rows:
                        args = (show_network["tmdb_network_id"], show_network["tmdb_show_id"])
                        cur.callproc("InsertTMDBShowNetwork", args)
                conn.commit()
            except Exception as e:
                conn.rollback()
                self.mylogger.logErrorMessage(f"dao_tmdb.insert_show_network_batch -- Error inserting batch: {e}"
                )
                raise
            finally:
                if cur:
                    cur.close()
                if conn and conn.is_connected():
                    conn.close()

    def bulk_insert_networks(self, network_rows: list):
        self.mylogger.logInfoMessage("Starting bulk insert of network details into database...")
        for row in network_rows:
            tmdb_network_id = row['tmdb_network_id']
            name = row['name']
            origin_country = row['origin_country']
            logo_path = row['logo_path']

            self.insert_network(tmdb_network_id, name, origin_country, logo_path)

    #These insert methods with 1 connection and 1 commit per row proved to be inefficient and crash-prone.
    def insert_network(self, tmdb_network_id: int, name: str, origin_country: str, logo_path: str):
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    args = (tmdb_network_id, name, origin_country, logo_path)
                    cur.callproc("InsertTMDBNetwork", args)
                conn.commit()
            except:
                self.mylogger.logErrorMessage(f"dao_tmdb.insert_network -- Error inserting network with TMDb Network ID {tmdb_network_id}, Name {name}, Origin Country {origin_country}, Logo Path {logo_path}")
                conn.rollback()
                raise

    #Improved efficiency
    def insert_network_batch(self, rows: list[tuple[int, int]]):
        conn = None
        cur = None

        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    for network in rows:
                        args = (network["tmdb_network_id"], network["name"], network["origin_country"], network["logo_path"])
                        cur.callproc("InsertTMDBNetwork", args)
                conn.commit()
            except Exception as e:
                conn.rollback()
                self.mylogger.logErrorMessage(f"dao_tmdb.insert_network_batch -- Error inserting batch: {e}"
                )
                raise
            finally:
                if cur:
                    cur.close()
                if conn and conn.is_connected():
                    conn.close()

    def insert_show_artwork_batch(self, rows: list[tuple[int, int]]):
        conn = None
        cur = None

        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    for artwork in rows:
                        args = (artwork["tmdb_show_id"], artwork["file_path"], artwork["artwork_type"], artwork["width"], artwork["height"], artwork["vote_avg"])
                        cur.callproc("InsertTMDBShowArtwork", args)
                conn.commit()
            except Exception as e:
                conn.rollback()
                self.mylogger.logErrorMessage(f"dao_tmdb.insert_show_artwork_batch -- Error inserting batch: {e}"
                )
                raise
            finally:
                if cur:
                    cur.close()
                if conn and conn.is_connected():
                    conn.close()

    def bulk_insert_episodes(self, episodes_rows: list):
        self.mylogger.logInfoMessage("Starting bulk insert of episode details into database...")
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

    #These insert methods with 1 connection and 1 commit per row proved to be inefficient and crash-prone.
    def insert_episode(self, tmdb_episode_id: int, tmdb_show_id: int, season_number: int, episode_number: int, name: str, overview: str, air_date: str, runtime: int,  vote_average: float, vote_count: int, still_path: str):
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    args = (tmdb_episode_id, tmdb_show_id, season_number, episode_number, name, overview, air_date, runtime, vote_average, vote_count, still_path)
                    cur.callproc("InsertTMDBEpisode", args)
                conn.commit()
            except:
                self.mylogger.logErrorMessage(f"dao_tmdb.insert_episode -- Error inserting episode with TMDb Episode ID {tmdb_episode_id}, TMDb Show ID {tmdb_show_id}, Season Number {season_number}, Episode Number {episode_number}, Name {name}, Overview {overview}, Air Date {air_date}, Runtime {runtime}, Vote Average {vote_average}, Vote Count {vote_count}, Still Path {still_path}")
                conn.rollback()
                raise

    #Improved efficiency
    def insert_episode_batch(self, rows: list[tuple[int, int]]):
        conn = None
        cur = None

        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    for episode in rows:
                        args = (episode["tmdb_episode_id"], episode["tmdb_show_id"], episode["season_number"], episode["episode_number"], episode["name"], episode["overview"], episode["air_date"], episode["runtime"], episode["vote_average"], episode["vote_count"], episode["still_path"])
                        cur.callproc("InsertTMDBEpisode", args)
                conn.commit()
            except Exception as e:
                conn.rollback()
                self.mylogger.logErrorMessage(f"dao_tmdb.insert_episode_batch -- Error inserting batch: {e}"
                )
                raise
            finally:
                if cur:
                    cur.close()
                if conn and conn.is_connected():
                    conn.close()


    def bulk_insert_person(self, person_rows: list):
        self.mylogger.logInfoMessage("Starting bulk insert of person details into database...")
        for row in person_rows:
            person_id = row['tmdb_person_id']
            name = row['person_name']
            biography = row['biography']
            birthday = row['birthday']
            gender = row['gender']
            place_of_birth = row['place_of_birth']
            profile_path = row['profile_path']

            self.insert_person(person_id, name, biography, birthday, gender, place_of_birth, profile_path)

    #These insert methods with 1 connection and 1 commit per row proved to be inefficient and crash-prone.
    def insert_person(self, person_id: int, name: str, biography: str, birth_date: str,gender: int,  place_of_birth: str, profile_path: str):
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    args = (person_id, name, biography, birth_date, gender,  place_of_birth, profile_path)
                    cur.callproc("InsertTMDBPerson", args)
                conn.commit()
            except:
                self.mylogger.logErrorMessage(f"dao_tmdb.insert_person -- Error inserting person with TMDb Person ID {person_id}, Name {name}, Biography {biography[0:10] + ' ..... '}, Birth Date {birth_date}, Gender {gender}, Place of Birth {place_of_birth}, Profile Path {profile_path}")
                conn.rollback()
                raise

    #Improved efficiency
    def insert_person_batch(self, rows: list[tuple[int, int]]):
        conn = None
        cur = None

        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    for person in rows:
                        args = (person["tmdb_person_id"], person["person_name"], person["biography"], person["birthday"], person["gender"], person["place_of_birth"], person["profile_path"])
                        cur.callproc("InsertTMDBPerson", args)
                conn.commit()

            except Exception as e:
                if conn:
                    conn.rollback()
                self.mylogger.logErrorMessage(f"dao_tmdb.insert_person_batch -- Error inserting batch: {e}"
                )
                raise

            finally:
                if cur:
                    cur.close()
                if conn and conn.is_connected():
                    conn.close()


    def bulk_insert_cast_crew(self, episode_cast_rows: list, episode_crew_rows: list):
        self.mylogger.logInfoMessage("Starting bulk insert of episode cast and crew details into database...")
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

    #These insert methods with 1 connection and 1 commit per row proved to be inefficient and crash-prone.
    def insert_crew_member(self, tmdb_episode_id: int, person_id: int, job: str, department: str):
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    args = (tmdb_episode_id, person_id, job, department)
                    cur.callproc("InsertTMDBEpisodeCrew", args)
                conn.commit()
            except:
                self.mylogger.logErrorMessage(f"dao_tmdb.insert_crew_member -- Error inserting crew member with TMDb Episode ID {tmdb_episode_id}, TMDb Person ID {person_id}, Job {job}, Department {department}")
                conn.rollback()
                raise

    #Improved efficiency
    def insert_crew_batch(self, rows: list[tuple[int, int]]):
        conn = None
        cur = None

        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    for crew_member in rows:
                        args = (crew_member["tmdb_episode_id"], crew_member["tmdb_person_id"], crew_member["job"], crew_member["department"])
                        cur.callproc("InsertTMDBEpisodeCrew", args)
                conn.commit()
            except Exception as e:
                if conn:
                    conn.rollback()
                self.mylogger.logErrorMessage(f"dao_tmdb.insert_crew_batch -- Error inserting batch: {e}"
                )
                raise

            finally:
                if cur:
                    cur.close()
                if conn and conn.is_connected():
                    conn.close()

    #These insert methods with 1 connection and 1 commit per row proved to be inefficient and crash-prone.
    def insert_cast_member(self, tmdb_episode_id: int, person_id: int, character: str, order: int):
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    args = (tmdb_episode_id, person_id, character, order)
                    cur.callproc("InsertTMDBEpisodeCast", args)
                conn.commit()
            except:
                self.mylogger.logErrorMessage(f"dao_tmdb.insert_cast_member -- Error inserting cast member with TMDb Episode ID {tmdb_episode_id}, TMDb Person ID {person_id}, Character {character}, Order {order}")
                conn.rollback()
                raise

    #Improved efficiency
    def insert_cast_batch(self, rows: list[tuple[int, int]]):
        conn = None
        cur = None
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    for cast_member in rows:
                        args = (cast_member["tmdb_episode_id"], cast_member["tmdb_person_id"], cast_member["character"], cast_member["order"])
                        cur.callproc("InsertTMDBEpisodeCast", args)
                conn.commit()

            except Exception as e:
                if conn:
                    conn.rollback()
                self.mylogger.logErrorMessage(f"dao_tmdb.insert_cast_batch -- Error inserting batch: {e}"
                )
                raise

            finally:
                if cur:
                    cur.close()
                if conn and conn.is_connected():
                    conn.close()

    def clear_tmdb(self):
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn) as cur:
                    cur.callproc("ClearTMDBTables")
                conn.commit()
            except:
                self.mylogger.logErrorMessage("dao_tmdb.clear_tmdb -- Error clearing TMDB tables")
                conn.rollback()
                raise

    #Extract all artwork for a show (posters, backdrops, logos)
    def get_show_artwork(self, tmdb_show_id: int):
        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn, dictionary=True) as cur:
                    args = (tmdb_show_id,)
                    cur.callproc("GetTMDBShowArtwork", args)

                    results = []
                    for res in cur.stored_results():
                        rows = res.fetchall()
                        for row in rows:
                            results.append(row)
                    return results
                conn.commit()
            except:
                self.mylogger.logErrorMessage(f"dao_tmdb.get_show_artwork -- Error retrieving show artwork with TMDb Show ID {tmdb_show_id}")
                conn.rollback()
                raise

    # Extract a rated (or random) piece of artwork for each type (poster, backdrop, logo) for a show
    def get_show_artwork(self, tmdb_show_id: int, rated: bool) -> dict:
        artwork = {
            "poster": {},
            "backdrop": {},
            "logo": {}
        }

        with self.db.get_connection() as conn:
            try:
                with self.db.get_cursor(conn, dictionary=True) as cur:
                    for artwork_type in ["poster", "backdrop", "logo"]:
                        args = (tmdb_show_id, artwork_type)

                        if rated:
                            cur.callproc("GetRatedArtwork", args)
                        else:
                            cur.callproc("GetRndArtwork", args)

                        for res in cur.stored_results():
                            columns = res.column_names
                            row = res.fetchone()

                            if not row:
                                continue

                            if not isinstance(row, dict):
                                row = dict(zip(columns, row))

                            artwork[artwork_type] = {
                                "file_path": row.get("file_path"),
                                "artwork_type": row.get("artwork_type"),
                                "width": row.get("width"),
                                "height": row.get("height")
                            }

                return artwork

            except Exception as ex:
                self.mylogger.logErrorMessage(
                    f"dao_tmdb.get_show_artwork -- Error retrieving artwork for show ID {tmdb_show_id}: {ex}"
                )
                conn.rollback()
                raise
