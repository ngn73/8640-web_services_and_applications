'''
Name: api_trakt.py
Description: 
Object-based API client responsible for interacting with the Trakt API.
'''


from ast import Call

import requests
import urllib3
import config as cfg # Configuration Details for Trakt API
import data.dao_trakt as dao

class api_trakt:
    def __init__(self, trakt_auth: dict, dao_trakt: dao.dao_trakt, logger):
        # Disable IPv6 to avoid annoying issues with long API responses due to proxies
        urllib3.util.connection.HAS_IPV6 = False

        self.base_url = cfg.TRAKT_TOKENS["trakt_url"]
        self.user_agent = cfg.TRAKT_TOKENS["user_agent"]

        self.dao_trakt = dao_trakt
        self.logger = logger

        self.auth_id = trakt_auth["auth_id"]
        self.client_id = trakt_auth["client_id"]
        self.client_secret = trakt_auth["client_secret"]
        self.redirect_uri = trakt_auth["redirect_uri"]

        self.access_token = trakt_auth["access_token"]
        self.refresh_token = trakt_auth["refresh_token"]
        self.token_type = trakt_auth.get("token_type", "bearer")
        self.expires_in = trakt_auth.get("expires_in")
        self.created_at = trakt_auth.get("created_at")
        self.refreshed_at = trakt_auth.get("refreshed_at")

        self.session = requests.Session()
        self.session.trust_env = False

        self._set_api_headers()

    def _set_api_headers(self) -> None:
        self.session.headers.update({
            "Authorization": f"Bearer {self.access_token}",
            "trakt-api-key": self.client_id,
            "trakt-api-version": "2",
            "Content-Type": "application/json",
            "User-Agent": self.user_agent
        })        

    # Refresh the Trakt access token and persist the new token pair.
    # Unlike TMDb, the Trakt access token expires after a certain time and needs to be refreshed using the refresh token.
    def refresh_access_token(self) -> None:
        url = f"{self.base_url}/oauth/token"

        payload = {
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "redirect_uri": self.redirect_uri,
            "grant_type": "refresh_token"
        }

        headers = {
            "Content-Type": "application/json",
            "User-Agent": self.user_agent
        }

        self.logger.logInfoMessage("Refreshing Trakt access token")

        response = self.session.post(url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()

        token_data = response.json()

        self.access_token = token_data["access_token"]
        self.refresh_token = token_data["refresh_token"]
        self.token_type = token_data.get("token_type", self.token_type)
        self.expires_in = token_data.get("expires_in", self.expires_in)
        self.created_at = token_data.get("created_at", self.created_at)

        self._set_api_headers()

        # Persist latest tokens immediately
        self.dao_trakt.update_trakt_auth(
            access_token=self.access_token,
            refresh_token=self.refresh_token,
            token_type=self.token_type,
            expires_in=self.expires_in,
            created_at=self.created_at
        )

        self.logger.logInfoMessage("Trakt access token refreshed successfully")
    
    # Call this once at the start of the nightly job.
    def refresh_at_job_start(self) -> None:
        self.refresh_access_token()
    
    def _get(self, endpoint: str, params: dict | None = None, retry_on_401: bool = True):
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = self.session.get(url, params=params, timeout=60)

        if response.status_code == 401 and retry_on_401:
            self.logger.logInfoMessage(f"Received 401 Unauthorized for {endpoint}. Attempting to refresh access token and retry...")
            self.refresh_access_token()
            response = self.session.get(url, params=params, timeout=60)

        response.raise_for_status()
        return response.json()

    def get_watched_shows(self):
        endpt = "/sync/watched/shows"  # Endpoint for getting watched shows
        return self._get(endpt)

    def get_episode_ratings(self, page: int = 1, limit: int = 100):
        endpt = "/sync/ratings/episodes"  # Endpoint for getting episode ratings
        return self._get(endpt, params={"page": page, "limit": limit})