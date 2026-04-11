#==========================================================================================
#=====                           (Never executed directly)                      ===========   
#=====  Command lines used in setting up the scheduler to refresh tmdb data     ===========
#==========================================================================================

#Weekly refresh of all tmdb data ( tv shows, seasons, episodes, networks, people)
"C:\Users\Niall\anaconda3\python.exe" "C:\Users\Niall\Documents\College\8640-web_services_and_applications\project\schedule_jobs.py" tmdb_full
#Daily refresh of tmdb data that was watched in the last 24 hours
"C:\Users\Niall\anaconda3\python.exe" "C:\Users\Niall\Documents\College\8640-web_services_and_applications\project\schedule_jobs.py" tmdb_delta
#Daily refresh of trakt data (watched history, ratings)
"C:\Users\Niall\anaconda3\python.exe" "C:\Users\Niall\Documents\College\8640-web_services_and_applications\project\schedule_jobs.py" trakt
