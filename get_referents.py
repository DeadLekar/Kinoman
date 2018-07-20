import sqlite3 as lite
from paths import *
import serviceFunctions as sf
import pandas as pd
import datetime

class User:
    id = -1
    c = None # db connection
    df_my_marks = None # user's marks, pandas DataFrame
    checked_users = []
    def __init__(self, _id, _c):
        self.id = _id
        self.c = _c
        self._get_marks()

    def _get_marks(self):
        l_users_rows = c.execute("SELECT film_id,mark_bin FROM marks WHERE user_id = {}".format(self.id)).fetchall()
        film_id_col = []
        mark_col = []
        d = {}
        for row in l_users_rows:
            film_id_col.append(row[0])
            mark_col.append(row[1])
            d = {'film_id':film_id_col, 'mark':mark_col}

        self.df_my_marks = pd.DataFrame(d)
        self.df_my_marks.index = self.df_my_marks.film_id
        # self.df_my_marks.drop(self.df_my_marks.film_id,inplace=True)
        del self.df_my_marks['film_id']

class UserMatrix:
    matrix = {}
    def __init__(self, user1_df, user2_df, min_intersect):
        """
        creates matrix from users data
        :param user1: pandas.DataFrame
        :param user2: pandas.DataFrame
        """
        df = user1_df.merge(user2_df, left_index = True, right_index = True)
        if df.shape[0] >= min_intersect:
            self.matrix['pp'] = df[(df.mark_x == 1) & (df.mark_y == 1)].shape[0]
            self.matrix['pn'] = df[(df.mark_x == 1) & (df.mark_y == -1)].shape[0]
            self.matrix['np'] = df[(df.mark_x == -1) & (df.mark_y == 1)].shape[0]
            self.matrix['nn'] = df[(df.mark_x == -1) & (df.mark_y == -1)].shape[0]
            self.matrix['total_sum'] = df.size/2


db_path = sf.get_right_path(db_paths)
conn = lite.connect(db_path + '/kinoman_imdb_marks.db')
c = conn.cursor()
users_rows = c.execute("SELECT user_id FROM grp_users WHERE is_checked = 0 ORDER BY user_id")
users_id_list = []

for u_row in users_rows.fetchall():
    users_id_list.append(u_row[0])

MIN_INTERSECT = 3 # minimum number of films seen by both users in a pair
for i in range(0,len(users_id_list)):
    now = datetime.datetime.now()
    user = User(users_id_list[i],c)
    print('+++ user {} +++'.format(user.id))
    total_spect = 0
    for film_id in user.df_my_marks.index:
        spectators_rows = c.execute("SELECT user_id FROM marks WHERE film_id={} AND user_id > {}".format(film_id,user.id)).fetchall()
        for row in spectators_rows:
            total_spect += len(spectators_rows)
            ref_user_id = row[0]
            if ref_user_id not in user.checked_users:
                ref_user = User(ref_user_id,c)
                m = UserMatrix(user.df_my_marks, ref_user.df_my_marks, MIN_INTERSECT)
                user.checked_users.append(ref_user.id)
                if m.matrix:
                    c.execute("INSERT INTO PAIRS(user1_id,user2_id,pp,pn,np,nn,total_sum) VALUES ({},{},{},{},{},{},{})".format(user.id,ref_user_id,m.matrix['pp'],m.matrix['pn'],m.matrix['np'],m.matrix['nn'],m.matrix['total_sum']))

    then = datetime.datetime.now()
    delta = then - now
    print('{} to check the user {}'.format(delta.seconds, user.id))
    print('{} spectators found'.format(total_spect))
    c.execute("UPDATE grp_users SET is_checked = 1 WHERE user_id = {}".format(user.id))
    conn.commit()

