import sqlite3 as lite
from paths import *
import serviceFunctions as sf
import pandas as pd
import datetime
import os.path


class Film:
    def __init__(self, _id, _ds):

        self.id = _id
        self.ds = _ds
        d = self.ds.get_spectators(self.id) # pandas data frame
        if d is not None:
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
            # self.df_my_marks.index = self.df_my_marks.film_id
            # del self.df_my_marks['film_id']


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
        else: return None

    def _get_marks_db(self, user_id):
        l_users_rows = self.c.execute("SELECT film_id,mark_bin FROM marks WHERE user_id = {}".format(user_id)).fetchall()
        film_id_col = []
        mark_col = []
        for row in l_users_rows:
            film_id_col.append(row[0])
            mark_col.append(row[1])
        return {'film_id': film_id_col, 'mark': mark_col, 'user_id':[user_id]}

    def _get_marks_files(self, user_id):
        path = '{}users/{}.txt'.format(self.files_path,user_id)
        if os.path.isfile(path):
            df = pd.read_csv(path, sep=';', names=['film_id', 'mark'])
            df['user_id']=user_id
            return df
        else: return None


class ReferentFinder:
    MIN_INTERSECT = 3  # minimum number of films seen by both users in a pair
    users_id_list = []
    films_id_list = []
    root_files_path = ''
    source = ''
    ds = None
    flg_to_db = False
    pairs_path = ''
    films_path = ''
    def __init__(self, _ds, _flg_to_db=True):
        # fills user_id_list
        self.ds = _ds
        self.root_files_path = sf.get_right_path(files)
        self.pairs_path = self.root_files_path + 'pairs/'
        self.films_path = self.root_files_path + 'films/'

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

    def get_referents_v2(self):
        """ get referents through films spectators"""
        users = {}
        # load users data

        for user_id in self.users_id_list:
            users[user_id] = User(user_id, self.ds)
            if (len(users) % 1000) == 0: print(int(len(users) / len(self.users_id_list) * 100))

        print('==== start linking ====')
        for u_id in self.users_id_list:
            now = datetime.datetime.now()
            pairs_data = pd.DataFrame(columns=['user1_id', 'user2_id', 'pp', 'pn', 'np', 'nn', 'total_sum'])
            # get referents
            referents_all = pd.Series([])
            for film_id in users[u_id].df_my_marks.index:
                f = Film(film_id,self.ds)
                if hasattr(f, 'df_my_spectators'):
                    referents_all = referents_all.append(pd.Series(f.df_my_spectators.user_id))
            referents_unique = referents_all.unique()
            for r_id in referents_unique:
                if r_id > u_id:
                    m = UserMatrix(users[u_id].df_my_marks, users[r_id].df_my_marks, self.MIN_INTERSECT)
                    if m.matrix:
                        if self.flg_to_db:
                            self.ds.c.execute("INSERT INTO pairs(user1_id,user2_id,pp,pn,np,nn,total_sum) VALUES ({},{},{},{},{},{},{})".format(u_id, r_id, m.matrix['pp'], m.matrix['pn'], m.matrix['np'], m.matrix['nn'], m.matrix['total_sum']))
                        else:
                            pairs_data = pairs_data.append(pd.DataFrame({'user1_id': [u_id], 'user2_id': [r_id], 'pp': m.matrix['pp'], 'pn': m.matrix['pn'], 'np': m.matrix['np'], 'nn': m.matrix['nn'], 'total_sum': m.matrix['total_sum']}))

            if not self.flg_to_db:
                pairs_data.to_csv('{}/{}.txt'.format(self.pairs_path, u_id), sep=';', index=False)

            then = datetime.datetime.now()
            delta = then - now
            print('{}: {} sec, {} referents'.format(u_id, delta, len(referents_unique)))
            self.ds.c.execute("UPDATE grp_users SET is_checked = 1 WHERE user_id = {}".format(u_id))
            self.ds.conn.commit()

    def get_referents_v3(self):
        cnt_users = 0
        df_all_marks = pd.DataFrame(columns=['user_id','film_id','mark'])
        for user_id in self.users_id_list:
            cnt_users += 1
            df_user = self.ds.get_user_marks(user_id)
            df_all_marks = df_all_marks.append(df_user)
            if (cnt_users % 1000) == 0: print(cnt_users / len(self.users_id_list) * 100)

        for user_id in self.users_id_list:
            df_cr = df_all_marks['user_id'==user_id]


    def get_films_spectators(self):
        # try to build pairs via films
        for film_id in self.films_id_list:
            # check if the programm needs to be stopped

            flg_stop = int(open(self.root_files_path + 'stop.txt').read(1))
            if flg_stop: break
            f = Film(film_id,self.ds)
            now = datetime.datetime.now()
            for i in range(len(f.df_my_spectators.index)):  # create pairs table for each spectator
                cr_user_id = f.df_my_spectators.loc[i].user_id
                cr_user_mark = f.df_my_spectators.loc[i].mark
                df_referents = f.df_my_spectators[f.df_my_spectators.user_id != cr_user_id]
                df_result = pd.DataFrame({'user_id': list(df_referents.user_id)},dtype='int32')

                the_same_matrix = df_referents[df_referents.mark == cr_user_mark]
                the_same_matrix.mark = 1
                df_result = df_result.merge(the_same_matrix, how='left', on='user_id')
                df_result.columns = ['user_id', 'the_same']

                opposite_matrix = df_referents[df_referents.mark != cr_user_mark]
                opposite_matrix.mark = 1
                df_result = df_result.merge(opposite_matrix, how='left', on='user_id')

                if cr_user_mark > 0:
                    df_result.columns = ['user_id', 'pp', 'pn']
                    df_result['np'] = 0
                    df_result['nn'] = 0
                else:
                    df_result.columns = ['user_id', 'nn', 'np']
                    df_result['pn'] = 0
                    df_result['pp'] = 0
                df_result.fillna(0,inplace=True)
                df_result.pp = df_result.pp.astype('int32')
                df_result.pn = df_result.pn.astype('int32')
                df_result.np = df_result.np.astype('int32')
                df_result.nn = df_result.nn.astype('int32')

                # save data
                user_file = '{}{}.txt'.format(self.pairs_path, cr_user_id)
                if not os.path.isfile(user_file):  # a new user detected, just save data frame
                    df_result.to_csv(user_file, sep=';', index=False, header=True)
                else: # the user has saved data, merge
                    df_saved = pd.read_csv(user_file, sep=';', header=0)
                    df_concat = pd.concat([df_result, df_saved], ignore_index=True)
                    df_grouped = df_concat.groupby(['user_id']).sum()
                    df_grouped.to_csv(user_file, sep=';', index=True, header=True)

            self.ds.c.execute("UPDATE grp_films SET is_checked=1 WHERE film_id={}".format(f.id))
            self.ds.conn.commit()

            then = datetime.datetime.now()
            delta = then - now
            print('{}: {} sec, {} users'.format(f.id, delta, len(f.df_my_spectators.index)))


ds = DataSource(False)
rf = ReferentFinder(ds,False)
rf.get_referents_v3()