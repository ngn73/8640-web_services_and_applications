import pandas as pd
import config as cfg  # configuration details
import api.api_trakt as api #separate API client
import data.dao_trakt as dao
import data.dbManager as db #separate Data Access Object

# Convert the raw json "watched" data from the API into a DataFrame
def watched_data_to_df(self, watched_data):
    watched_rows = []
    # Flatten the watched data to get a row for each episode
    # with the show title/id, season number, episode number, and last watched date.
    for show_item in watched_data:
        show = show_item.get("show", {})
        show_ids = show.get("ids", {})
        tmdb_id = show_ids.get("tmdb")
        title = show.get("title")

        for season in show_item.get("seasons", []):
            season_number = season.get("number")

            for episode in season.get("episodes", []):
                watched_rows.append({
                    "tmdb_id": tmdb_id,
                    "title": title,
                    "season_number": season_number,
                    "season_episode_number": episode.get("number"),
                    "last_watched_at": episode.get("last_watched_at")
                })

    return pd.DataFrame(watched_rows)

# Convert the raw json "rating" data from the API into a DataFrame
def ratings_data_to_df(ratings_data):
    ratings_rows = []
    # Flatten the ratings data to get a row for each episode rating 
    # with the show id, season number, episode number, rating, and rated date.
    for rating_item in ratings_data:
        show = rating_item.get("show", {})
        show_ids = show.get("ids", {})
        episode = rating_item.get("episode", {})

        ratings_rows.append({
            "tmdb_id": show_ids.get("tmdb"),
            "season_number": episode.get("season"),
            "season_episode_number": episode.get("number"),
            "rated_at": rating_item.get("rated_at"),
            "rating": rating_item.get("rating")
        })

    return pd.DataFrame(ratings_rows)

# Merge the "watched" and "ratings" DataFrames on tmdb_id, season number, and episode number.
def merge_watched_and_ratings(watched_df, ratings_df):
    return watched_df.merge(
        ratings_df,
        on=["tmdb_id", "season_number", "season_episode_number"],
        how="left" # use left join as not all watched episodes will have ratings
    )

# Get a database manager instance
def get_db_mgr():
    db_client = db.dbManager(
        host=cfg.CRUD["host"],
        user=cfg.CRUD["user"],
        password=cfg.CRUD["password"],
        database=cfg.CRUD["database"]
    )
    return db_client

def save_to_db(trakt_df):
    db_client = get_db_mgr()
    dao_trakt = dao.dao_trakt(db_client)
    dao_trakt.bulk_insert(trakt_df)


#==========================================
#           Main Execution
#==========================================
if __name__ == "__main__":
    # I will use the "watched" and "ratings" api endpoints to get the list of shows in my watched history and their ratings.
    api_client = api.api_trakt()
    watched_data = api_client.get_watched_shows()
    ratings_data = api_client.get_episode_ratings()

    #Convert the raw json data into DataFrames and merge them.
    watched_df = watched_data_to_df(watched_data)
    ratings_df = ratings_data_to_df(ratings_data)
    final_df = merge_watched_and_ratings(watched_df, ratings_df)    

    #Update DataTable "trakt_status" 
    save_to_db( final_df)

