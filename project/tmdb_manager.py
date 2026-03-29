

import pandas as pd
import config as cfg  # configuration details
import api.api_tmdb as api # separate API client
import api.api_ntfy as api_ntfy # for sending notifications 
import data.dao_tmdb as dao
import data.dao_trakt as dao_trakt # needed to initially extract list of watched shows.
import data.dbManager as db # separate Data Access Object



# Get a database manager instance
def get_db_mgr():
    db_client = db.dbManager(
        host=cfg.CRUD["host"],
        user=cfg.CRUD["user"],
        password=cfg.CRUD["password"],
        database=cfg.CRUD["database"]
    )
    return db_client

def extract_TMDB_show_details(tmdb_id: str):
    try:
        api_client = api.api_tmdb()
        show_details = api_client.get_show_season_details(tmdb_id)
    except Exception as e:
        api_ntfy_client.send_notification("TMDB Event", f"Error occurred while fetching show details for TMDb ID {tmdb_id}: {str(e)}")
        raise

    # Extract the relevant 'Show' details from the response json
    shows_rows.append({
        "tmdb_id": show_details.get("tmdb_id"),
        "name": show_details.get("name"),
        "overview": show_details.get("overview"),
        "first_air_date": show_details.get("first_air_date"),
        "status": show_details.get("status"),
        "vote_average": show_details.get("vote_average"),
        "vote_count": show_details.get("vote_count"),
        "number_of_seasons": show_details.get("number_of_seasons"),
        "number_of_episodes": show_details.get("number_of_episodes"),
        "poster_path": show_details.get("poster_path")
    })

    # Extract the relevant details for each Season from the response json
    for season in show_details.get("seasons", []):
        seasons_rows.append({
            "tmdb_season_id": season.get("id"),
            "tmdb_id": show_details.get("tmdb_id"),
            "season_number": season.get("season_number"),
            "name": season.get("name"),
            "overview": season.get("overview"),
            "air_date": season.get("air_date"),
            "episode_count": season.get("episode_count"),
            "poster_path": season.get("poster_path")
        })

        extract_TMDB_episode_details(tmdb_id, season.get("season_number"))


def extract_TMDB_episode_details(tmdb_id: str, season_number: int):
    api_client = api.api_tmdb()
    episode_details = api_client.get_episode_details(tmdb_id, season_number)

    for episode in episode_details.get("episodes", []):
        episodes_rows.append({
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
        extract_TMDB_cast_crew_details(tmdb_id, season_number, episode.get("episode_number"))



def extract_TMDB_cast_crew_details(tmdb_id: str, season_number: int, episode_number: int):
    api_client = api.api_tmdb()
    cast_crew_details = api_client.get_episode_cast_crew_details(tmdb_id, season_number, episode_number)
    tmdb_episode_id = cast_crew_details.get("id")
    for cast_member in cast_crew_details.get("cast", []):
        episode_cast_rows.append({ 
            "tmdb_episode_id": tmdb_episode_id,
            "tmdb_person_id": cast_member.get("id"),
            "character": cast_member.get("character"),
            "order": cast_member.get("order")
        })
        #Insert Person details for cast member (if not already extracted for this person_id)
        extract_TMDB_person_details(cast_member.get("id"))


    for crew_member in cast_crew_details.get("crew", []):
        if(crew_member.get("job") in ["Director", "Writer", "Producer"]): # only Insert key roles
            episode_crew_rows.append({
                "tmdb_episode_id": tmdb_episode_id,
                "tmdb_person_id": crew_member.get("id"),
                "job": crew_member.get("job"),
                "department": crew_member.get("department"),
            })
            #Insert Person details for crew member (if not already extracted for this person_id)
            extract_TMDB_person_details(crew_member.get("id"))



def extract_TMDB_person_details(person_id: str):
    if(person_id not in extracted_person_ids): # check if we have already extracted details for this person (as they may appear in multiple episodes)
        api_client = api.api_tmdb()
        full_person_details = api_client.get_person_details(person_id)

        person_rows.append({
            "tmdb_person_id": full_person_details.get("id"),
            "person_name": full_person_details.get("name"),
            "biography": full_person_details.get("biography"),
            "birthday": full_person_details.get("birthday"),
            "gender": full_person_details.get("gender"),
            "place_of_birth": full_person_details.get("place_of_birth"),
            "profile_path": full_person_details.get("profile_path")
        })
        extracted_person_ids.add(person_id)



#==========================================
#           Main Execution
#==========================================


if __name__ == "__main__":
    # initially get distinct TMDB Ids in "trakt_status" Table
    db_client = get_db_mgr()
    trakt_dao = dao_trakt.dao_trakt(db_client)
    trakt_shows = trakt_dao.getdistinct_shows()

    api_ntfy_client = api_ntfy.api_ntfy()
    api_ntfy_client.send_notification("TMDB Event", "Starting TMDb data extraction process with " + str(len(trakt_shows)) + " shows to extract details for.")

    shows_rows = []
    seasons_rows = []
    episodes_rows = []
    episode_cast_rows = []
    episode_crew_rows = []
    extracted_person_ids = set() # utilise a set to avoid duplicates as the same person may appear in multiple episodes
    person_rows = []

    show_idx = 0
    for tmdb_id in trakt_shows:
        show_idx += 1
        api_ntfy_client.send_notification("TMDB Event", f"Extracting details for show {show_idx} of {len(trakt_shows)} with TMDb ID {tmdb_id}...")
        if(show_idx % 3 == 0): 
            break
        try:
            extract_TMDB_show_details(tmdb_id)
        except Exception as e:
            api_ntfy_client.send_notification("TMDB Event", f"Error occurred while extracting details for show {show_idx} with TMDb ID {tmdb_id}: {str(e)}")    

    api_ntfy_client.send_notification("TMDB Event", "TMDb data extraction process completed. Now inserting data into database...")
    
    # Insert the show details into the database
    dao_tmdb = dao.dao_tmdb(db_client)
    dao_tmdb.bulk_insert_shows(shows_rows)
    dao_tmdb.bulk_insert_seasons(seasons_rows)  
    dao_tmdb.bulk_insert_episodes(episodes_rows)
    dao_tmdb.bulk_insert_cast_crew(episode_cast_rows, episode_crew_rows)
    dao_tmdb.bulk_insert_person(person_rows)
    api_ntfy_client.send_notification("TMDB Event", "TMDb data insertion completed.")

