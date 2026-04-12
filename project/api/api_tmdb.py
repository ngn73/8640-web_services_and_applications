'''
Name: api_tmdb.py
Description: 
Object-based API client responsible for interacting with the TMDb API.
'''


import time

import requests
import urllib3
import config as cfg # Configuration Details for TMDb API
import app_logger as logger

class api_tmdb:
    def __init__(self):
        # Disable IPv6 to avoid annoying issues with long API responses due to proxies
        urllib3.util.connection.HAS_IPV6 = False

        self.mylogger = logger.app_logger(__name__)

        # This file is for testing the TMDb API connection and tokens. 
        self.base_url = cfg.TMDB_TOKENS['tmdb_url']
        self.session = requests.Session()
        self.session.trust_env = False # proxies cause issues with long API responses
        self.session.headers.update({
            "Authorization": f"Bearer {cfg.TMDB_TOKENS['tmdb_api_key']}",
            "Accept": "application/json"
        })
        self.timeout = 30

    def _get(self, endpoint: str):
        try:    
            url = f"{self.base_url}{endpoint}"
            print(f"Calling TMDb: {url}")   # useful while debugging
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            time.sleep(1) # Sleep for 1 second to respect TMDb API rate limits
        except requests.RequestException as e:
            self.mylogger.logErrorMessage(f"api_tmdb._get -- Error calling TMDb API for endpoint {endpoint}: {e}")
            raise

        return response.json()
    
    def get_show_season_details(self, tmdb_id: str):
        #Show details endpoint also returns season and network details
        return self._get(f"/tv/{tmdb_id}")
    
    def get_episode_details(self, tmdb_id: str, season_number: int):
        #This endpoint returns the episode details for all episodes in the season
        return self._get(f"/tv/{tmdb_id}/season/{season_number}")
    
    def get_episode_cast_crew_details(self, tmdb_id: str, season_number: int, episode_number: int):
        # This endpoint returns both cast and crew details for the episode
        return self._get(f"/tv/{tmdb_id}/season/{season_number}/episode/{episode_number}/credits")
    
    def get_person_details(self, person_id: str):
        # This endpoint returns the person's details
        return self._get(f"/person/{person_id}")
    
    def get_show_artwork(self, tmdb_id: str):
        # This endpoint returns all artwork for the show (posters, backdrops, logos)
        return self._get(f"/tv/{tmdb_id}/images")