'''
Name: api_trakt.py
Description: 
Object-based API client responsible for interacting with the Trakt API.
'''


import requests
import urllib3
import config as cfg # Configuration Details for Trakt API

class api_trakt:
    def __init__(self):
        # Disable IPv6 to avoid issues with long API responses due to proxies
        urllib3.util.connection.HAS_IPV6 = False

        # This file is for testing the Trakt API connection and tokens. It will print the name/id/watcheddate of shows in my watched history.
        self.base_url = cfg.TRAKT_TOKENS['trakt_url']
        self.headers = {
            "Authorization": f"Bearer {cfg.TRAKT_TOKENS['trakt_access_token']}",
            "trakt-api-key": cfg.TRAKT_TOKENS['trakt_client_id'],
            "trakt-api-version": "2",
            "Content-Type": "application/json"
        }

    def get_watched_shows(self):
        url = f"{self.base_url}/sync/watched/shows"  # Endpoint for getting watched shows
        r = requests.get(url, headers=self.headers)
        r.raise_for_status()
        return r.json()

    def get_episode_ratings(self, page=1, limit=100):
        url = f"{self.base_url}/sync/ratings/episodes" # Endpoint for getting episode ratings
        r = requests.get(
            url,
            headers=self.headers,
            params={"page": page, "limit": limit}
        )
        r.raise_for_status()
        return r.json()