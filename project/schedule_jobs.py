import sys
import scheduler.tmdb_manager as tmdb_manager
import scheduler.trakt_manager as trakt_manager
from datetime import datetime

tmdb = tmdb_manager.tmdb_mgr()
trakt = trakt_manager.trakt_mgr()
today = datetime.now().strftime("%A")

if __name__ == "__main__":
    mode = "test"  #default to full tmdb update if no argument is provided
    if len(sys.argv) > 1:
        mode = sys.argv[1]

    if mode == "tmdb_delta":
        tmdb.extract_and_save_delta_tmdb_details()

    elif mode == "tmdb_full" and today == "Sunday": # PythonAnywhere only allows Daily/Hourly Tasks
        tmdb.extract_and_save_all_tmdb_details()

    elif mode == "trakt":
        trakt.extract_and_save_trakt_data()

    elif mode == "test":
        trakt.test_data_connection()

    else:
        print(f"Invalid mode specified: {mode}. Valid modes are 'tmdb_delta', 'tmdb_full', and 'trakt'.")