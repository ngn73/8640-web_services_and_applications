
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

LOG_DIR = BASE_DIR / "logs"
LOG_FILE = LOG_DIR / "media_app.log"


TRAKT_TOKENS ={
    "trakt_url": "https://api.trakt.tv",
    "user_agent": "g00473476@atu.ie",
    "trakt_client_id": "8cdad2e8cb32b0a5398b1295e512b10cf7a2abcb0b09d5259d5e788d047103b0",
    "trakt_client_secret": "e9110f2b617f75312b49fe2e25f01a044650d072da2daec9e5d00d36239e8db4",
    "trakt_access_token": "ee6ce38d3ceb064a62dd7826cb1ec37bf6d74c9bcd357b3c893979ba7d4c6606"
}
TMDB_TOKENS = {
    "tmdb_url": "https://api.themoviedb.org/3",
    "tmdb_api_key": "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJmMzUwN2NlMWZjNmE2ODA4ZmYzMGI4YTIyNGM1YzYwMSIsIm5iZiI6MTc3MzU4Njc1MC42NzgwMDAyLCJzdWIiOiI2OWI2YzkzZWU3OTZmOTRiZWQ5NWY2YjQiLCJzY29wZXMiOlsiYXBpX3JlYWQiXSwidmVyc2lvbiI6MX0.1Z3rvtr-8qj8X3VBcegH0B2ZprorlMRPToJIffE5rMk"
}
NTFY_TOKENS = {
    "ntfy_url": "https://ntfy.sh",
    "ntfy_topic": "ngn73_ntfy"
}
CRUD = {
    # Local (Wamp) MySQL Database Connection Details
    "host": "127.0.0.1",
    "user": "root",
    "password": "",
    "database": "MEDIA_TRACKER_DB"

    # ===== For Hosted PythonAnywhere, the database connection details will be different =====
    # "host": "ngnWatchs.mysql.pythonanywhere-services.com",  # important
    # "user": "ngnWatchs",
    # "password": "%Lilaboc_94_FG%",
    # "database": "ngnWatchs$Media_Tracker"
    
}
LOGGING = {
    "active": True,
    "filename": LOG_FILE,
    "level": "INFO"
}
