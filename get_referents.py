import sqlite3 as lite
from paths import *
import serviceFunctions as sf
import numpy

class User:
    id = -1
    c = None
    marks = {}
    linked_users = []
    def __init__(self, _id, _c):
        self.id = _id
        self.c = _c

    def get_marks(self):
        m_rows = c.execute("SELECT mark, filmID FROM marks WHERE userID = {}".format(self.id)).fetchall()
        for row in m_rows:
            self.marks[row[1]] = row[0]

    def get_linked_users(self):
        if len(self.marks) > 0:
            for film_id in self.marks.keys():
                f = Film(film_id,self.c)
                f.get_users()
                for u in f.users.keys():
                    self.linked_users.append(u)

        


class Film:
    id = -1
    c = None
    users = {}
    def __init__(self,_id,_c):
        self.id = _id
        self.c = _c

    def get_users(self):
        u_rows = c.execute("SELECT user_id,mark FROM marks WHERE film_id = {}".format(self.id)).fetchall()
        for row in u_rows:
            self.users[row[0]] = row[1]




db_path = sf.get_right_path(db_paths)
conn = lite.connect(db_path)
c = conn.cursor()
users = c.execute("SELECT id FROM users WHERE flgChecked = 0 ORDER BY id")
for u_row in users.fetchall():
    cr_user = User(u_row[0],c)
    cr_user.get_marks()
    cr_user.get_linked_users()



    cr_user_id = u_row[0]
    cr_user_marks = {}
    cr_user_marks_rows = c.execute("SELECT mark, filmID FROM marks WHERE userID = {}".format(cr_user_id)).fetchall()
    for mark in cr_user_marks_rows:
        cr_user_marks[cr_user_marks_rows[1]] = cr_user_marks_rows[0]



    users_to_compare = c.execute("SELECT id FROM users WHERE id > " + next_user)
    cnt_commit = 0
    for u1_row in users_to_compare.fetchall():
        user_compare_id = u1_row[0]
        user_compare_films = c.execute("SELECT mark, filmID FROM marks WHERE userID = " + str(user_compare_id)).fetchall()
        cnt_fits = 0
        diff_sum = 0
        mid_diff = 0
        for cr_mark in cr_user_films:
            mark_to_remove = 0
            for compare_mark in user_compare_films:
                if cr_mark[1] == compare_mark[1]:
                    mark_to_remove = compare_mark
                    cnt_fits += 1
                    diff = abs(cr_mark[0] - compare_mark[0])
                    diff_sum += diff
                    break
            if mark_to_remove:
                user_compare_films.remove(mark_to_remove)
        if cnt_fits > 0:
            mid_diff = diff_sum / cnt_fits
        common_share = int(100 * cnt_fits / (len(cr_user_films) + len(user_compare_films)))
        c.execute("INSERT INTO pairs (user1ID, user2ID, distance, commonShare) VALUES ("+ str(cr_user_id) + "," + str(user_compare_id) + "," + str(mid_diff) + "," + str(common_share) +")")
        #cnt_commit +=1
        #if cnt_commit == 5000:
        #    conn.commit()
        #    cnt_commit = 0
    c.execute("UPDATE users SET flgChecked = 1 WHERE id = " + str(cr_user_id))
    conn.commit()
    # print(cr_user_id)
