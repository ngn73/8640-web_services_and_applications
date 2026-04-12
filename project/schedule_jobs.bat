#==========================================================================================
#=====                       (Called by Windows Task Scheduler)                 ===========   
#=====  Command lines used in setting up the scheduler to refresh tmdb data     ===========
#=====  Input parameter (%1) is the type of refresh to perform (daily/weekly).  ===========
#=====  "tmdb_full", "tmdb_delta", or "trakt" are the expected values.          ===========
#==========================================================================================

# Daily/Weekly refresh of all tmdb/trakt data ( tv shows, seasons, episodes, networks, people, watched status, ratings)
"C:\Users\Niall\anaconda3\python.exe" "C:\Users\Niall\Documents\College\8640-web_services_and_applications\project\schedule_jobs.py" %1
