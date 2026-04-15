

import pandas as pd
import config as cfg
import api.api_tmdb as api # separate API client
import data.dao_tmdb as dao
import data.dao_trakt as dao_trakt # needed to initially extract list of watched shows.
import data.dbManager as db # separate Data Access Object
import app_logger as logger

class tmdb_mgr:

    def __init__(self):
        #==========================================
        #  Initialise API and DB Clients, and Lists
        #==========================================

        self.myLogger = logger.app_logger(__name__)
        self.api_client = api.api_tmdb()
        self.db_client = self._get_db_mgr()
        self.dao_tmdb = dao.dao_tmdb(self.db_client)

        self.shows_rows = []
        self.seasons_rows = []
        self.show_network_rows = []
        self.network_rows = []
        self.episodes_rows = []
        self.episode_cast_rows = []
        self.episode_crew_rows = []
        self.extracted_person_ids = set() # utilize a set to avoid duplicates as the same person may appear in multiple episodes
        self.person_rows = []
        self.show_artwork_rows = []


    # Get reference to the database manager instance
    def _get_db_mgr(self):
        db_client = db.dbManager(
            host=cfg.CRUD["host"],
            user=cfg.CRUD["user"],
            password=cfg.CRUD["password"],
            database=cfg.CRUD["database"]
        )
        return db_client

    # Load the TMDb details for the list of shows(delta or full)
    def _load_trakt_data(self, trakt_shows:list):

        #trakt_shows = ['60585', '456', '4546', '93870'] # for testing, comment out in production
        
        show_idx = 0
        for tmdb_id in trakt_shows:
            show_idx += 1
            if(show_idx % 10 == 0): # log progress every 10 shows
                self.myLogger.logInfoMessage(f"Extracting details for show {show_idx} of {len(trakt_shows)} with TMDb ID {tmdb_id}...", ntfy=True)

            try:
                self._extract_TMDB_show_details(tmdb_id)    #Start the recursive extraction process to populate all the relevant lists
            except Exception as e:
                self.myLogger.logErrorMessage(f"Error occurred while extracting details for show {show_idx} with TMDb ID {tmdb_id}: {str(e)}")

        self.myLogger.logInfoMessage("TMDb data extraction process completed. All lists populated with extracted data.")

    def extract_and_save_delta_tmdb_details(self):
        self.myLogger.logInfoMessage("Starting TMDb data extraction for delta load...", ntfy=True)
        trakt_shows = self.get_delta_shows()  #Get the delta list of shows in trakt_status table
        if(not trakt_shows or len(trakt_shows) == 0):
            self.myLogger.logInfoMessage("No shows found in trakt_status table. Skipping TMDb data extraction and insertion.")
            return  
        else:
            self.myLogger.logInfoMessage(f"Found {len(trakt_shows)} shows in trakt_status table. Starting extraction of TMDb details for these shows.", ntfy=True)
            self._load_trakt_data(trakt_shows)  #Populate the lists
            self._save_to_db()                  #Save the lists to the database using the DAO methods
        
        self.myLogger.logInfoMessage("TMDb data extraction and insertion for delta load completed.", ntfy=True)

    def extract_and_save_all_tmdb_details(self):
        self.myLogger.logInfoMessage("Starting TMDb data extraction for full load...", ntfy=True)
        trakt_shows = self.get_distinct_shows()  #Get the list of distinct shows in trakt_status table (for full load) or the delta list (for delta load)
        if(not trakt_shows or len(trakt_shows) == 0):
            self.myLogger.logInfoMessage("No shows found in trakt_status table. Skipping TMDb data extraction and insertion.")
            return  
        else:
            self.myLogger.logInfoMessage(f"Found {len(trakt_shows)} shows in trakt_status table. Starting extraction of TMDb details for these shows.", ntfy=True)
            self._load_trakt_data(trakt_shows)  #Populate the lists
            self._save_to_db()                  #Save the lists to the database using the DAO methods
        self.myLogger.logInfoMessage("TMDb data extraction and insertion for full load completed.", ntfy=True)

    #Save the extracted TMDb details into the database using the DAO methods
    def _save_to_db(self):
        # Insert the list data into the database
        self.myLogger.logInfoMessage("TMDb data insertion starting ...")

        self.dao_tmdb.clear_tmdb() # Clear the existing TMDB data before inserting the new data.
        self.dao_tmdb.insert_show_batch(self.shows_rows)
        self.dao_tmdb.insert_season_batch(self.seasons_rows)
        self.dao_tmdb.insert_show_network_batch(self.show_network_rows)
        self.dao_tmdb.insert_network_batch(self.network_rows)
        self.dao_tmdb.insert_episode_batch(self.episodes_rows)
        self.dao_tmdb.insert_cast_batch(self.episode_cast_rows)
        self.dao_tmdb.insert_crew_batch(self.episode_crew_rows)
        self.dao_tmdb.insert_person_batch(self.person_rows)
        self.dao_tmdb.insert_show_artwork_batch(self.show_artwork_rows)

        self.myLogger.logInfoMessage("TMDb data insertion completed.")


    def get_distinct_shows(self):
        # get distinct TMDB Ids in "trakt_status" Table
        try:
            trakt_dao = dao_trakt.dao_trakt(self.db_client)
            trakt_shows = trakt_dao.get_distinct_shows()
        except Exception as e:
            self.myLogger.logErrorMessage(f"Error occurred while fetching distinct shows from trakt_status table: {str(e)}")
            trakt_shows = None

        return trakt_shows
    
    def get_delta_shows(self):
        # get delta list of TMDB Ids in "trakt_status" Table
        try:
            trakt_dao = dao_trakt.dao_trakt(self.db_client)
            trakt_shows = trakt_dao.get_delta_shows()
        except Exception as e:
            self.myLogger.logErrorMessage(f"Error occurred while fetching delta shows from trakt_status table: {str(e)}")
            trakt_shows = None

        return trakt_shows

    def _extract_TMDB_show_details(self, tmdb_id: str):
        try:
            show_details = self.api_client.get_show_season_details(tmdb_id)
        except Exception as e:
            self.myLogger.logErrorMessage(f"Error occurred while fetching show details for TMDb ID {tmdb_id}: {str(e)}")
            raise

        # Extract the relevant 'Show' details from the response json
        self.shows_rows.append({
            "tmdb_id": tmdb_id,
            "name": show_details.get("name"),
            "overview": show_details.get("overview"),
            "first_air_date": show_details.get("first_air_date"),
            "status": show_details.get("status"),
            "vote_average": show_details.get("vote_average"),
            "vote_count": show_details.get("vote_count"),
            "number_of_seasons": show_details.get("number_of_seasons"),
            "number_of_episodes": show_details.get("number_of_episodes")
        })

        # Extract the relevant details for each Season from the response json
        for season in show_details.get("seasons", []):
            self.seasons_rows.append({
                "tmdb_season_id": season.get("id"),
                "tmdb_id": tmdb_id,
                "season_number": season.get("season_number"),
                "name": season.get("name"),
                "overview": season.get("overview"),
                "air_date": season.get("air_date"),
                "episode_count": season.get("episode_count"),
                "poster_path": season.get("poster_path")
            })
            self._extract_TMDB_episode_details(tmdb_id, season.get("season_number"))

        # extract the network details for the show (from same API call)
        for network in show_details.get("networks", []):
            self.network_rows.append({
                "tmdb_network_id": network.get("id"),
                "name": network.get("name"),
                "origin_country": network.get("country", {}).get("iso_3166_1"),
                "logo_path": network.get("logo_path")
            })
            self.show_network_rows.append({
                "tmdb_show_id": tmdb_id,
                "tmdb_network_id": network.get("id")
            })
        
        #finally extract all artwork paths for the show (from separate API call)
        try:
            artwork_details = self.api_client.get_show_artwork(tmdb_id) 
        except Exception as e:
            self.myLogger.logErrorMessage(f"Error occurred while fetching artwork details for TMDb ID {tmdb_id}: {str(e)}")
            raise
        
        for artwork in artwork_details.get("posters", []):
            if artwork.get("iso_639_1") in ("en", None): # some shows have non-english images, so check before inserting
                self.show_artwork_rows.append({
                    "tmdb_show_id": tmdb_id,
                    "file_path": artwork.get("file_path"),
                    "artwork_type": "poster",
                    "width": artwork.get("width"),
                    "height": artwork.get("height")
                })
        for artwork in artwork_details.get("backdrops", []):
            self.show_artwork_rows.append({
                "tmdb_show_id": tmdb_id,
                "file_path": artwork.get("file_path"),
                "artwork_type": "backdrop",
                "width": artwork.get("width"),
                "height": artwork.get("height")
            })

        # for logos, prefer English language if available, 
        # otherwise take the first logo (even if non-English or no language specified)
        logos = artwork_details.get("logos", [])
        english = [l for l in logos if l.get("iso_639_1") == "en"]
        logo = None
        if(english):
            logo = english[0] 
        else :
            logo = next(
                (l for l in logos if l.get("iso_639_1") is None),
                None
            )

        if logo:
            file_path = logo.get("file_path")
            if file_path:   # important safety check
                self.show_artwork_rows.append({
                    "tmdb_show_id": tmdb_id,
                    "file_path": file_path,
                    "artwork_type": "logo",
                    "width": logo.get("width"),
                    "height": logo.get("height")
                })
    
    def _extract_TMDB_episode_details(self, tmdb_id: str, season_number: int):
        try:
            episode_details = self.api_client.get_episode_details(tmdb_id, season_number)
        except Exception as e:
            self.myLogger.logErrorMessage(f"Error occurred while fetching episode details for TMDb ID {tmdb_id}, Season {season_number}: {str(e)}")
            raise

        for episode in episode_details.get("episodes", []):
            self.episodes_rows.append({
                "tmdb_episode_id": episode.get("id"),
                "tmdb_show_id": tmdb_id,
                "season_number": season_number,
                "episode_number": episode.get("episode_number"),
                "name": episode.get("name"),
                "overview": episode.get("overview"),
                "air_date": episode.get("air_date"),
                "runtime": episode.get("runtime"),
                "vote_average": episode.get("vote_average"),
                "vote_count": episode.get("vote_count"),
                "still_path": episode.get("still_path")
            })
            
            #get cast and crew details for the episode
            self._extract_TMDB_cast_crew_details(tmdb_id, season_number, episode.get("episode_number"))



    def _extract_TMDB_cast_crew_details(self, tmdb_id: str, season_number: int, episode_number: int):
        try:
            cast_crew_details = self.api_client.get_episode_cast_crew_details(tmdb_id, season_number, episode_number)
        except Exception as e:
            self.myLogger.logErrorMessage(f"Error occurred while fetching cast and crew details for TMDb ID {tmdb_id}, Season {season_number}, Episode {episode_number}: {str(e)}")
            raise

        tmdb_episode_id = cast_crew_details.get("id")
        for cast_member in cast_crew_details.get("cast", []):
            self.episode_cast_rows.append({ 
                "tmdb_episode_id": tmdb_episode_id,
                "tmdb_person_id": cast_member.get("id"),
                "character": cast_member.get("character"),
                "order": cast_member.get("order")
            })
            #Insert Person details for cast member (if not already extracted for this person_id)
            self._extract_TMDB_person_details(cast_member.get("id"))


        for crew_member in cast_crew_details.get("crew", []):
            if(crew_member.get("job") in ["Director", "Writer", "Producer"]): # only Insert these key roles
                self.episode_crew_rows.append({
                    "tmdb_episode_id": tmdb_episode_id,
                    "tmdb_person_id": crew_member.get("id"),
                    "job": crew_member.get("job"),
                    "department": crew_member.get("department"),
                })
                #Insert Person details for crew member (if not already extracted for this person_id)
                self._extract_TMDB_person_details(crew_member.get("id"))



    def _extract_TMDB_person_details(self, person_id: str):
        if(person_id not in self.extracted_person_ids): # check if we have already extracted details for this person (as they may appear in multiple episodes)
            try:
                full_person_details = self.api_client.get_person_details(person_id)
            except Exception as e:
                self.myLogger.logErrorMessage(f"Error occurred while fetching person details for TMDb ID {person_id}: {str(e)}")
                raise

            self.person_rows.append({
                "tmdb_person_id": full_person_details.get("id"),
                "person_name": full_person_details.get("name"),
                "biography": full_person_details.get("biography"),
                "birthday": full_person_details.get("birthday"),
                "gender": full_person_details.get("gender"),
                "place_of_birth": full_person_details.get("place_of_birth"),
                "profile_path": full_person_details.get("profile_path")
            })
            self.extracted_person_ids.add(person_id)





