# ===========================================================================
# flask_app.py
# This file contains the Flask application that serves the web pages. 
# It replaced app_server.py (after pythonanywhere deployment)
# ===========================================================================

from flask import Flask, render_template # also import render_template to render HTML templates (with jinja2)
from data.dao_tmdb import dao_tmdb
import data.dbManager as dbManager
import config as cfg


def get_db_mgr():
    # Get reference to the database manager instance
    db_client = dbManager.dbManager(
        host=cfg.CRUD["host"],
        user=cfg.CRUD["user"],
        password=cfg.CRUD["password"],
        database=cfg.CRUD["database"]
    )
    return db_client

app = Flask(__name__, template_folder="pages")
db_client = get_db_mgr()
dao = dao_tmdb(db_client)

@app.route('/')
def hello_world():
    return 'Hello from Flask!'

# endpoint for the home page
@app.route("/shows")
def shows():
    shows = dao.get_all_shows()

    print(f"Shows type: {type(shows)}")
    print(f"Shows count: {len(shows) if shows else 0}")
    print(f"First show: {shows[0] if shows else 'NO SHOWS'}")

    return render_template("shows.html", shows=shows)

# endpoint for the show details page
@app.route("/show/<int:tmdb_show_id>")
def show_details(tmdb_show_id):
    # Get the show details
    show = dao.get_show_details(tmdb_show_id)
    # Get the show artwork Url's
    artwork = dao.get_show_artwork(tmdb_show_id, rated=False)
    # Get the show seasons
    seasons = dao.get_season_details(tmdb_show_id)



    if not show:
        return "TV Show not found", 404

    # pass the show details (and artwork) to the template for rendering
    return render_template("show_details.html", show=show, artwork=artwork, seasons=seasons)

# endpoint for the season details page
@app.route("/show/<int:tmdb_show_id>/season/<int:season_number>")
def season_details(tmdb_show_id, season_number):
    show = dao.get_show_details(tmdb_show_id)
    season = dao.get_season_details_by_number(tmdb_show_id, season_number)
    episodes = dao.get_episode_details(tmdb_show_id, season_number)

    return render_template(
        "season_details.html",
        show=show,
        season=season,
        episodes=episodes
    )

@app.route("/show/<int:tmdb_show_id>/season/<int:season_number>/episode/<int:episode_number>/crew")
def episode_crew(tmdb_show_id, season_number, episode_number):
    pass

@app.route("/person/<int:tmdb_person_id>/show/<int:tmdb_show_id>/season/<int:season_number>/episode/<int:episode_number>")
def person_detail(tmdb_person_id, tmdb_show_id, season_number, episode_number):
    person = dao.get_person_details(tmdb_person_id)
    related_roles = dao.get_person_related_roles(tmdb_person_id)
    source = {  # Needed to return the episode page
        "tmdb_show_id": tmdb_show_id,
        "season_number": season_number,
        "episode_number": episode_number
    }

    return render_template(
        "person_detail.html",
        person=person,
        related_roles=related_roles,
        source=source
    )


@app.route("/show/<int:tmdb_show_id>/season/<int:season_number>/episode/<int:episode_number>/cast")
def episode_cast(tmdb_show_id, season_number, episode_number):
    show = dao.get_show_details(tmdb_show_id)
    season = dao.get_season_details_by_number(tmdb_show_id, season_number)
    episode = dao.get_episode_details_by_number(tmdb_show_id, season_number, episode_number)
    cast = dao.get_episode_cast(tmdb_show_id, season_number, episode_number)
    crew = dao.get_episode_crew(tmdb_show_id, season_number, episode_number)

    return render_template(
        "episode_cast.html",
        show=show,
        season=season,
        episode=episode,
        cast=cast,
        crew=crew
    )


@app.route("/weekly_watch_plan")
def weekly_watch_plan():
    weekly_rows = dao.get_latest_watched_episode_details()
    return render_template("weekly_watch_plan.html", weekly_rows=weekly_rows)

if __name__ == '__main__':
    app.run(debug=True)
