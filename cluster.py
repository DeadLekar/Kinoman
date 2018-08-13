import sqlite3 as lite
from paths import *
import serviceFunctions as sf
from get_referents import DataSource, ReferentFinder
import pandas as pd
import numpy as np



class ClusterMaker:
    def __init__(self, _rf):
        self.rf = _rf
        self.rf.df_users_grp.index = self.rf.df_users_grp.user_id
        del (self.rf.df_users_grp['user_id'])

    def make_clusters(self):
        for user_id in self.rf.df_users_grp.index:
            if self.rf.df_users_grp.loc[user_id].cluster_id == -1:
                print('Looking for cluster for user {}:'.format(user_id))
                if user_id == 1250134:
                    a = 1
                next_cluster_id = max(self.rf.df_users_grp.cluster_id) + 1
                cluster_members = [user_id]
                closest_user_arr = self.get_closest_users(user_id,1)
                if closest_user_arr:
                    closest_user = closest_user_arr[0]
                    while self.rf.df_users_grp.loc[closest_user].cluster_id == -1 and closest_user not in cluster_members:
                        print('Closest: {}'.format(closest_user))
                        cluster_members.append(closest_user)  # add next closer user to the future cluster
                        closest_user = self.get_closest_users(closest_user, 1)[0]  # get nearest user to the nearest user
                    if self.rf.df_users_grp.loc[closest_user].cluster_id != -1:
                        # connection with existing cluster detected, add users to it
                        existing_cluster_id = self.rf.df_users_grp.loc[closest_user].cluster_id
                        existing_dist_center = self.rf.df_users_grp.loc[closest_user].dist_center
                        self.add_to_cluster(cluster_members, existing_cluster_id, existing_dist_center)
                        print('Existing cluster found: {}'.format(existing_cluster_id))
                    else:
                        # create a new cluster
                        self.add_to_cluster(cluster_members, next_cluster_id)
                        print('New cluster: {}'.format(next_cluster_id))

    def get_closest_users(self, user_id, k):
        # returns k closest users
        result_arr = []
        df_pairs = self.rf.ds.get_user_pairs(user_id)
        df_pairs.index = df_pairs.user_id
        del (df_pairs['user_id'])

        for id_to_drop in [0, user_id]:
            if id_to_drop in df_pairs.index:
                df_pairs = df_pairs.drop(id_to_drop)

        if df_pairs.size == 0: return []

        df_pairs = df_pairs.merge(self.rf.df_users_grp, left_index=True, right_index=True)
        # user_id (index), pp, pn, np, nn, sum, cnt_marks - df now
        df_pairs['intersection'] = df_pairs['sum'] / (self.rf.df_users_grp.loc[user_id].cnt_marks + df_pairs.cnt_marks - df_pairs['sum']) * 100
        df_pairs['coherency'] = (df_pairs.pp + df_pairs.nn) / (df_pairs.np + df_pairs.pn)  # coherency > 1 means users are close
        df_pairs = df_pairs.replace([np.inf], 0)
        df_pairs['inter_index'] = df_pairs.intersection / max(df_pairs.intersection)
        df_pairs['coh_index'] = df_pairs.coherency / max(df_pairs.coherency)
        df_pairs['proximity'] = df_pairs.inter_index * df_pairs.coh_index
        df_pairs = df_pairs.sort_values(by='proximity', ascending=False)
        for user_id in df_pairs.index:
            if len(result_arr) == k: break
            result_arr.append(user_id)
        return result_arr

    def add_to_cluster(self, users_arr, cluster_id, existing_dist_center = 0):
        while len(users_arr) > 0:
            u_id = users_arr.pop(0)
            self.rf.df_users_grp.loc[u_id, 'cluster_id'] = cluster_id
            self.rf.df_users_grp.loc[u_id, 'dist_center'] = len(users_arr) + existing_dist_center + 1
            self.rf.ds.c.execute("UPDATE grp_users SET cluster_id = {} WHERE user_id = {}".format(cluster_id, u_id))
            self.rf.ds.c.execute("UPDATE grp_users SET dist_center = {} WHERE user_id = {}".format(len(users_arr) + existing_dist_center + 1, u_id))
            self.rf.ds.conn.commit()

if __name__ == '__main__':
    ds = DataSource()
    rf = ReferentFinder(ds)
    clm = ClusterMaker(rf)
    clm.make_clusters()
