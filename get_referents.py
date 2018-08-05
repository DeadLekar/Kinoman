import sqlite3 as lite
from paths import *
import serviceFunctions as sf
import pandas as pd
import datetime
import os.path

class Film:
    id = -1
    df_my_spectators = None  # user's marks, pandas DataFrame
    ds = None  # DataSource object

    def __init__(self, _id, _ds):
        self.id = _id
        self.ds = _ds
        d = self.ds.get_spectators(self.id) # pandas data frame
        if d.size > 0:
            self.df_my_spectators = pd.DataFrame(d,dtype='int32')
            # self.df_my_spectators.index = self.df_my_spectators.user_id
            # del self.df_my_spectators['user_id']

class User:
    id = -1
    df_my_marks = None # user's marks, pandas DataFrame
    checked_users = []
    ds = None # DataSource object
    def __init__(self, _id, _ds):
        self.id = _id
        self.ds = _ds
        d = self.ds.get_user_marks(self.id)
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

    def get_spectators(self, film_id,user_id=0):
        if self.flg_from_db:
            return self._get_spectators_db(film_id,user_id)
        else:
            return self._get_spectators_files(film_id,user_id)

    def get_user_marks(self, user_id):
        if self.flg_from_db:
            return self._get_marks_db(user_id)
        else:
            return self._get_marks_files(user_id)


    def _get_spectators_db(self, film_id, user_id):
        spectators_arr = []
        marks_arr = []
        spectators_rows = self.c.execute("SELECT user_id,mark_bin FROM marks WHERE film_id={} AND user_id > {}".format(film_id, user_id)).fetchall()
        for row in spectators_rows:
            spectators_arr.append(row[0])
            marks_arr.append(row[1])
        df = pd.DataFrame({'user_id':spectators_arr,'mark':marks_arr})
        return spectators_arr

    def _get_spectators_files(self, film_id, user_id):
        path = '{}films/{}.txt'.format(self.files_path,film_id)
        if os.path.isfile(path):
            df = pd.read_csv(path,sep=';',names=['user_id','mark'])
            return df[df.user_id>user_id]
        else: return False

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
    films_id_list = []
    source = ''
    ds = None
    flg_to_db = False
    pairs_path = ''
    films_path = ''
    def __init__(self, _ds, _flg_to_db=True):
        # fills user_id_list
        self.ds = _ds
        self.pairs_path = sf.get_right_path(files) + 'pairs/'
        self.films_path = sf.get_right_path(files) + 'films/'

        users_rows = self.ds.c.execute("SELECT user_id FROM grp_users WHERE is_checked = 0 ORDER BY user_id")
        for u_row in users_rows.fetchall():
            self.users_id_list.append(u_row[0])

        films_rows = self.ds.c.execute("SELECT film_id FROM grp_films WHERE is_checked = 0")
        for f_row in films_rows.fetchall():
            self.films_id_list.append(f_row[0])

    def get_referents(self):
        # try to bild pairs via users
        users = {}
        # load users data

        for user_id in self.users_id_list:
            users[user_id] = User(user_id,self.ds)
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
                        self.ds.c.execute("INSERT INTO pairs(user1_id,user2_id,pp,pn,np,nn,total_sum) VALUES ({},{},{},{},{},{},{})".format(self.users_id_list[i], self.users_id_list[j], m.matrix['pp'], m.matrix['pn'], m.matrix['np'], m.matrix['nn'], m.matrix['total_sum']))
                    else:
                        pairs_data = pairs_data.append(pd.DataFrame({'user1_id':self.users_id_list[i],'user2_id':self.users_id_list[j],'pp':m.matrix['pp'],'pn':m.matrix['pn'],'np':m.matrix['np'],'nn':m.matrix['nn'],'total_sum':m.matrix['total_sum']},index=[cnt_referents]))

            if not self.flg_to_db:
                pairs_data.to_csv('{}/{}.txt'.format(self.pairs_path, self.users_id_list[i]), sep=';', index=False)

            then = datetime.datetime.now()
            delta = then - now
            print('{}: {} sec, {} referents'.format(self.users_id_list[i],delta,cnt_referents))
            self.ds.c.execute("UPDATE grp_users SET is_checked = 1 WHERE user_id = {}".format(users[self.users_id_list[i]].id))
            self.ds.conn.commit()

    def get_films_spectators(self):
        # try to build pairs via films
        df_template = pd.DataFrame(columns=['user_id','pp','pn','np','nn','total_sum'])
        for film_id in self.films_id_list:
            f = Film(film_id,self.ds)
            for i in range(len(f.df_my_spectators.index)):
                cr_user_id = f.df_my_spectators.loc[i].user_id
                cr_user_mark = f.df_my_spectators.loc[i].mark
                df_referents = f.df_my_spectators[f.df_my_spectators.user_id != cr_user_id]
                df_result = pd.DataFrame({'user_id':list(df_referents.user_id)},dtype='int32')
                user_file = '{}{}.txt'.format(self.pairs_path,cr_user_id)
                if not os.path.isfile(user_file): # a new user detected
                    the_same_matrix = df_referents[df_referents.mark == cr_user_mark]
                    the_same_matrix.mark = 1
                    df_result = df_result.merge(the_same_matrix, how='left', on='user_id')
                    df_result.columns = ['user_id','the_same']

                    opposit_matrix = df_referents[df_referents.mark != cr_user_mark]
                    opposit_matrix.mark = 1
                    df_result = df_result.merge(opposit_matrix, how='left', on='user_id')

                    if cr_user_mark > 0:
                        df_result.columns = ['user_id','pp', 'pn']
                        df_result['np'] = 0
                        df_result['nn'] = 0
                    else:
                        df_result.columns = ['user_id','nn', 'np']
                        df_result['pn'] = 0
                        df_result['pp'] = 0
                    df_result.fillna(0,inplace=True)
                    df_result.pp = df_result.pp.astype('int32')
                    df_result.pn = df_result.pn.astype('int32')
                    df_result.np = df_result.np.astype('int32')
                    df_result.nn = df_result.nn.astype('int32')
                    df_result.to_csv(user_file, sep=';',index=False, header=True)
                else:
                    df_saved = pd.read_csv(user_file,sep=';',header=0)
                    a = 1


ds = DataSource(False)
rf = ReferentFinder(ds,False)
rf.get_films_spectators()