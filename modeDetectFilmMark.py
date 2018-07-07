import sqlite3 as lite
import pandas as pd


compName = 'notebook'
driverPath = ""
dbasePath = ""
if compName == "Ilya":
    driverPath = "C:/Users/илья/Dropbox/Ilya-Papa/father_files/drivers/chromedriver.exe"
    dbasePath = "C:/tmp/kinoman.db"
elif compName == "work":
    driverPath = "C:/Program Files (x86)/Google/Chrome/Application/chromedriver.exe"
    dbasePath = "C:/Users/vkovalenko/Dropbox/Ilya-Papa/father_files/bases/kinoman.db"
elif compName == "notebook":
    driverPath = "C:/Users/Vlad/Dropbox/Ilya-Papa/father_files/drivers/chromedriver.exe"
    dbasePath = "C:/Users/Vlad/Dropbox/Ilya-Papa/father_files/bases/kinoman.db"

conn = lite.connect(dbasePath)
c = conn.cursor()

TARGET_FILM_ID = 1064  # DOCTOR STRANGE

# get target film's marks and users
target_dict = {}
target_users_rows = c.execute('SELECT user_id, mark FROM marks WHERE film_id = {}'.format(str(TARGET_FILM_ID))).fetchall()
for row in target_users_rows:
    target_dict[row[0]] = [row[1], 0]
df_target_film_marks = pd.DataFrame(target_dict)

# get target users' films and marks
target_dict.clear()
target_users_films_rows = c.execute('SELECT id, user_id, film_id, mark FROM marks WHERE user_id IN (SELECT user_id FROM marks WHERE film_id = {})'.format(str(TARGET_FILM_ID))).fetchall()
for row in target_users_films_rows:
    target_dict[row[0]] = [row[1], row[2], row[3]]
df_target_users_films = pd.DataFrame(target_dict)

# group films by the number of marks

