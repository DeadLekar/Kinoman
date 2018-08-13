import sqlite3 as lite
from paths import *
import serviceFunctions as sf
import pandas as pd
import datetime
import os.path
from os import listdir
import numpy as np

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

    def get_user_pairs(self, user_id):
        path = '{}pairs/{}.txt'.format(self.files_path, user_id)
        if os.path.isfile(path):
            return pd.read_csv(path, sep=';', header=0)
        else:
            return None

    def _get_spectators_db(self, film_id, user_id):
        spectators_arr = []
        marks_arr = []
        spectators_rows = self.c.execute("SELECT user_id,mark_bin FROM marks WHERE film_id={} AND user_id > {}".format(film_id, user_id)).fetchall()
        for row in spectators_rows:
            spectators_arr.append(row[0])
            marks_arr.append(row[1])
        #  df = pd.DataFrame({'user_id':spectators_arr,'mark':marks_arr})
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

    def separate_marks(self):
        # creates files for positive and negative marks
        films_path = '{}films/'.format(self.files_path)
        # f_pos = open('{}pos.txt'.format(self.files_path))
        # f_neg = open('{}neg.txt'.format(self.files_path))
        films_num = len(listdir(films_path))
        cnt_films = 0
        for f_name in listdir(films_path):
            df = pd.read_csv(films_path + f_name, sep=';', names=['user_id','mark'])
            df['film_id'] = int(f_name.split('.')[0])
            df_pos = df[df.mark==1]
            df_pos.to_csv('{}pos.txt'.format(self.files_path), mode='a', sep=';', header=False, index=False)
            df_neg = df[df.mark==-1]
            df_neg.to_csv('{}neg.txt'.format(self.files_path), mode='a', sep=';', header=False, index=False)
            cnt_films += 1
            if cnt_films % 1000 == 0: print(cnt_films)


class ReferentFinder:
    MIN_INTERSECT = 3  # minimum number of films seen by both users in a pair
    users_id_list = []
    films_id_list = []
    df_users_grp = None  # pandas data frame
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

        cnt_marks_arr = []
        cluster_num_arr = []
        dist_center_arr = []
        users_rows = self.ds.c.execute("SELECT user_id, cnt_marks, cluster_id, dist_center FROM grp_users WHERE is_checked = 0 ORDER BY user_id")
        for u_row in users_rows.fetchall():
            self.users_id_list.append(u_row[0])
            cnt_marks_arr.append(u_row[1])
            cluster_num_arr.append((u_row[2]))
            dist_center_arr.append((u_row[3]))
        self.df_users_grp = pd.DataFrame({'user_id': self.users_id_list, 'cnt_marks': cnt_marks_arr, 'cluster_id': cluster_num_arr, 'dist_center': dist_center_arr})

        films_rows = self.ds.c.execute("SELECT film_id FROM grp_films WHERE is_checked = 0")
        for f_row in films_rows.fetchall():
            self.films_id_list.append(f_row[0])


    def get_referents(self):
        cnt_users = 0
        MAX_USER_ID = len(self.users_id_list)
        index_arr = list(range(0, MAX_USER_ID, 1))
        if MAX_USER_ID not in index_arr:
            index_arr.append(MAX_USER_ID)
        for i in range(0,len(index_arr)-1):
            self._get_user_referents(self.users_id_list[index_arr[i]:index_arr[i+1]])
            flg_stop = int(open(self.root_files_path + 'stop.txt').read(1))
            if flg_stop: break

    def _get_user_referents(self, user_id_arr):
        films = []
        s = pd.Series()
        users = {}
        for user_id in user_id_arr:
            users[user_id] = User(user_id, self.ds)
            s = s.append(users[user_id].df_my_marks.film_id)
        films = s.unique()


        df_pos = pd.read_csv('{}pos.txt'.format(self.root_files_path), sep=';', names=['user_id', 'mark', 'film_id'])
        del (df_pos['mark'])
        df_pos = df_pos[df_pos.film_id.isin(list(films))]

        df_neg = pd.read_csv('{}neg.txt'.format(self.root_files_path), sep=';', names=['user_id', 'mark', 'film_id'])
        del (df_neg['mark'])
        df_neg = df_neg[df_neg.film_id.isin(list(films))]

        for user_id in users.keys():

            df_ref_pos = df_pos[df_pos.film_id.isin(list(users[user_id].df_my_marks.film_id))]  # positive marks of other spectators of the films the user saw
            df_ref_neg = df_neg[df_neg.film_id.isin(list(users[user_id].df_my_marks.film_id))]  # negative marks of other spectators of the films the user saw

            df_cr_pos = users[user_id].df_my_marks[users[user_id].df_my_marks.mark == 1]  # positive user's marks
            del (df_cr_pos['mark'])
            df_cr_neg = users[user_id].df_my_marks[users[user_id].df_my_marks.mark == -1]  # negative user's marks
            del (df_cr_neg['mark'])

            # get pp matrix (current user's positive mark - other users' positive marks)
            df_pp = df_cr_pos.merge(df_ref_pos, how='left', on='film_id')
            df_pp = self._prepare_matrix(df_pp, 'pp')  # got DataFrame with index=referent's id and count of marks in 'pp_marks_cnt' column

            # get pn matrix (current user's positive mark - other users' negative marks)
            df_pn = df_cr_pos.merge(df_ref_neg, how='left', on='film_id')
            df_pn = self._prepare_matrix(df_pn, 'pn')  # got DataFrame with index=referent's id and count of marks in 'pn_marks_cnt' column

            # get np matrix (current user's negative mark - other users' positive marks)
            df_np = df_cr_neg.merge(df_ref_pos, how='left', on='film_id')
            df_np = self._prepare_matrix(df_np, 'np')  # got DataFrame with index=referent's id and count of marks in 'np_marks_cnt' column

            # get nn matrix (current user's negative mark - other users' negative marks)
            df_nn = df_cr_neg.merge(df_ref_neg, how='left', on='film_id')
            df_nn = self._prepare_matrix(df_nn, 'nn')  # got DataFrame with index=referent's id and count of marks in 'np_marks_cnt' column

            # get the whole matrix
            df_total = df_pp.merge(df_pn, on='user_id', how='outer', sort=False)
            df_total = df_total.merge(df_np, on='user_id', how='outer', sort=False)
            df_total = df_total.merge(df_nn, on='user_id', how='outer', sort=False)
            df_total = df_total.fillna(0)
            df_total.pp = df_total.pp.astype('int32')
            df_total.np = df_total.np.astype('int32')
            df_total.pn = df_total.pn.astype('int32')
            df_total.nn = df_total.nn.astype('int32')
            df_total['sum'] = df_total.pp + df_total.np + df_total.pn + df_total.nn
            df_total = df_total[['user_id','pp','pn','np','nn','sum']]

            # save data
            df_total.to_csv('{}/{}.txt'.format(self.pairs_path, user_id), sep=';', index=False)
            self.ds.c.execute("UPDATE grp_users SET is_checked = 1 WHERE user_id = {}".format(user_id))
            print(user_id)
        self.ds.conn.commit()

    def _prepare_matrix(self, df, col_name):
        del (df['film_id'])
        df = df.fillna(0)
        df.user_id_x = df.user_id_x.astype('int32')
        df.user_id_y = df.user_id_y.astype('int32')
        df = df.groupby('user_id_y').count()
        df.columns = [col_name]
        df['user_id'] = df.index
        return df


if __name__ == '__main__':
    ds = DataSource(False)
    rf = ReferentFinder(ds,False)
    rf.get_referents()