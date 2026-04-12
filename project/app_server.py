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

# Import endpoints
@app.route("/show/<int:tmdb_show_id>")
def show_details(tmdb_show_id):
    # Get the show details
    show = dao.get_show_details(tmdb_show_id)
    # Get the show artwork Url's
    artwork = dao.get_rnd_show_artwork(tmdb_show_id)
    
    if not show:
        return "TV Show not found", 404

    # pass the show details (and artwork) to the template for rendering
    return render_template("show_details.html", show=show, artwork=artwork) 



if __name__ == '__main__':
    app.run(debug=True) 
