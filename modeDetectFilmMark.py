import sqlite3 as lite
import pandas as pd
import serviceFunctions as sf
from paths import *

db_path = sf.get_right_path(db_paths)
conn_main = lite.connect(db_path + '/kinoman_imdb_main.db')
conn1 = lite.connect(db_path + '/kinoman_imdb_marks_1.db')
conn2 = lite.connect(db_path + '/kinoman_imdb_marks_2.db')
c_main = conn_main.cursor()
c1 = conn1.cursor()
c2 = conn2.cursor()

TARGET_FILM_ID = 1064  # DOCTOR STRANGE

# get target film's marks and users
target_dict = {}
target_users = []
for c in [c_main,c1,c2]:
    target_users_rows = c.execute('SELECT user_id, mark FROM marks WHERE film_id = {}'.format(str(TARGET_FILM_ID))).fetchall()
    for row in target_users_rows:
        user_id = row[0]
        mark = row[1]
        target_dict[user_id] = [TARGET_FILM_ID, mark]
        target_users.append(row[0])
        print('{};{};{}'.format(user_id, TARGET_FILM_ID, mark))


# get target users' films and marks
for c in [c_main,c1,c2]:
    for user_id in target_users:
        target_users_films_rows = c.execute('SELECT film_id, mark FROM marks WHERE user_id = {}'.format(user_id)).fetchall()
        for row in target_users_films_rows:
            film_id = row[0]
            mark = row[1]
            target_dict[user_id] = [film_id, mark]
            print('{};{};{}'.format(user_id, film_id, mark))
df_target_users_films = pd.DataFrame(target_dict)

# group films by the number of marks

