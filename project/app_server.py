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

# endpoint for the home page
@app.route("/shows")
def shows():
    shows = dao.get_all_shows()
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
    cast_crew = dao.get_season_cast_crew(tmdb_show_id, season_number)

    return render_template(
        "season_details.html",
        show=show,
        season=season,
        episodes=episodes
    )

@app.route("/show/<int:tmdb_show_id>/season/<int:season_number>/episode/<int:episode_number>/crew")
def episode_crew(tmdb_show_id, season_number, episode_number):
    pass


@app.route("/show/<int:tmdb_show_id>/season/<int:season_number>/episode/<int:episode_number>/cast")
def episode_cast(tmdb_show_id, season_number, episode_number):
    show = dao.get_show_details(tmdb_show_id)
    season = dao.get_season_details_by_number(tmdb_show_id, season_number)
    episode = dao.get_episode_details_by_number(tmdb_show_id, season_number, episode_number)
    cast = dao.get_episode_cast(tmdb_show_id, season_number, episode_number)

    return render_template(
        "episode_cast.html",
        show=show,
        season=season,
        episode=episode,
        cast=cast
    )

if __name__ == '__main__':
    app.run(debug=True) 
