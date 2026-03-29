import requests
import config as cfg  # configuration details
import urllib3


def _get(endpoint: str):
    url = f"{base_url}{endpoint}"
    print(f"Calling TMDb: {url}")   # useful while debugging
    response = session.get(url, timeout= timeout)
    response.raise_for_status()
    return response.json()

def get_show_season_details(show_id: str):
    return _get(f"/tv/{show_id}")

#==========================================
#           Main Execution
#==========================================


if __name__ == "__main__":
        # This file is for testing the TMDb API connection and tokens. 
        base_url = cfg.TMDB_TOKENS['tmdb_url']
        session = requests.Session()
        session.trust_env = False # proxies cause issues with long API responses
        session.headers.update({
            "Authorization": f"Bearer {cfg.TMDB_TOKENS['tmdb_api_key']}",
            "Accept": "application/json"
        })
        timeout = 15
        urllib3.util.connection.HAS_IPV6 = False

        try:
            show_id = "1399"  # Example TMDb ID for "Game of Thrones"
            show_details = get_show_season_details(show_id)
            print("Show Details:", show_details)
        except requests.RequestException as e:
            print(f"Error fetching show details: {e}")  