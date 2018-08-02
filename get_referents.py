import sqlite3 as lite
from paths import *
import serviceFunctions as sf
import pandas as pd
import datetime
import os.path

class User:
    id = -1
    df_my_marks = None # user's marks, pandas DataFrame
    checked_users = []
    ds = None # DataSource object
    def __init__(self, _id, _ds):
        self.id = _id
        self.ds = _ds
        d = self.ds.get_marks(self.id)
        if d.size > 0:
            self.df_my_marks = pd.DataFrame(d)
            self.df_my_marks.index = self.df_my_marks.film_id
            del self.df_my_marks['film_id']

class UserMatrix:

    def __init__(self, user1_df, user2_df, min_intersect):
        """
        creates matrix from users data
        :param user1: pandas.DataFrame
        :param user2: pandas.DataFrame
        """
        self.matrix = {}
        df = user1_df.merge(user2_df, left_index = True, right_index = True)
        if df.shape[0] >= min_intersect:
            self.matrix['pp'] = df[(df.mark_x == 1) & (df.mark_y == 1)].shape[0]
            self.matrix['pn'] = df[(df.mark_x == 1) & (df.mark_y == -1)].shape[0]
            self.matrix['np'] = df[(df.mark_x == -1) & (df.mark_y == 1)].shape[0]
            self.matrix['nn'] = df[(df.mark_x == -1) & (df.mark_y == -1)].shape[0]
            self.matrix['total_sum'] = df.size/2

class DataSource:
    flg_from_db = False # True - data will be get from data base, False - from file system
    conn = None
    c = None
    files_path = ''
    def __init__(self, _flg_from_db = True):
        # fills user_id_list
        self.files_path = sf.get_right_path(files)
        self.flg_from_db = _flg_from_db
        db_path = sf.get_right_path(db_paths)
        self.conn = lite.connect(db_path + '/kinoman_imdb_marks.db')
        self.c = self.conn.cursor()

    def get_spectators(self, film_id,user_id):
        if self.flg_from_db:
            return self._get_spectators_db(film_id,user_id)
        else:
            return self._get_spectators_files(film_id,user_id)

    def get_marks(self, user_id):
        if self.flg_from_db:
            return self._get_marks_db(user_id)
        else:
            return self._get_marks_files(user_id)


    def _get_spectators_db(self, film_id, user_id):
        result = []
        spectators_rows = self.c.execute("SELECT user_id FROM marks WHERE film_id={} AND user_id > {}".format(film_id, user_id)).fetchall()
        for row in spectators_rows:
            result.append(row[0])
        return result

    def _get_spectators_files(self, film_id, user_id):
        path = '{}films/{}.txt'.format(self.files_path,film_id)
        if os.path.isfile(path):
            df = pd.read_csv(path,sep=';',names=['user_id','mark'])
            return list(df[df.user_id>user_id].user_id)
        else: return []

    def _get_marks_db(self, user_id):
        l_users_rows = self.c.execute("SELECT film_id,mark_bin FROM marks WHERE user_id = {}".format(user_id)).fetchall()
        film_id_col = []
        mark_col = []
        for row in l_users_rows:
            film_id_col.append(row[0])
            mark_col.append(row[1])
        return {'film_id': film_id_col, 'mark': mark_col}

    def _get_marks_files(self, user_id):
        path = '{}users/{}.txt'.format(self.files_path,user_id)
        if os.path.isfile(path):
            return pd.read_csv(path, sep=';', names=['film_id', 'mark'])
        else: return None


class ReferentFinder:

    MIN_INTERSECT = 3  # minimum number of films seen by both users in a pair
    users_id_list = []
    source = ''
    flg_to_db = False
    files_path = ''
    def __init__(self, ds, _flg_to_db=True):
        # fills user_id_list
        self.files_path = sf.get_right_path(files) + 'pairs/'
        users_rows = ds.c.execute("SELECT user_id FROM grp_users WHERE is_checked = 0 ORDER BY user_id")
        for u_row in users_rows.fetchall():
            self.users_id_list.append(u_row[0])

    def get_referents(self):
        users = {}
        # load users data

        for user_id in self.users_id_list:
            users[user_id] = User(user_id,ds)
            if (len(users)%1000)==0:print(int(len(users)/len(self.users_id_list)*100))

        print('==== start linking ====')
        for i in range(len(self.users_id_list)-1):
            now = datetime.datetime.now()
            cnt_referents = -1
            pairs_data = pd.DataFrame(columns=['user1_id','user2_id','pp','pn','np','nn','total_sum'])

            for j in range(i+1, len(self.users_id_list)):
                m = UserMatrix(users[self.users_id_list[i]].df_my_marks, users[self.users_id_list[j]].df_my_marks, self.MIN_INTERSECT)
                if m.matrix:
                    cnt_referents += 1
                    if self.flg_to_db:
                        ds.c.execute("INSERT INTO pairs(user1_id,user2_id,pp,pn,np,nn,total_sum) VALUES ({},{},{},{},{},{},{})".format(self.users_id_list[i], self.users_id_list[j], m.matrix['pp'], m.matrix['pn'], m.matrix['np'], m.matrix['nn'], m.matrix['total_sum']))
                    else:
                        pairs_data = pairs_data.append(pd.DataFrame({'user1_id':self.users_id_list[i],'user2_id':self.users_id_list[j],'pp':m.matrix['pp'],'pn':m.matrix['pn'],'np':m.matrix['np'],'nn':m.matrix['nn'],'total_sum':m.matrix['total_sum']},index=[cnt_referents]))

            if not self.flg_to_db:
                pairs_data.to_csv('{}/{}.txt'.format(self.files_path, self.users_id_list[i]), sep=';', index=False)

            then = datetime.datetime.now()
            delta = then - now
            print('{}: {} sec, {} referents'.format(self.users_id_list[i],delta,cnt_referents))
            ds.c.execute("UPDATE grp_users SET is_checked = 1 WHERE user_id = {}".format(users[self.users_id_list[i]].id))
            ds.conn.commit()


ds = DataSource(False)
rf = ReferentFinder(ds,False)
rf.get_referents()