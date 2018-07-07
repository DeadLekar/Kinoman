# parse all afisha users
import pandas as pd
from bs4 import BeautifulSoup
import sqlite3 as lite
import requests
import serviceFunctions as sf
import numpy as np

class ReviewItem:
    def __init__(self, _film_link, _rating, _dt):
        self.film_link = _film_link
        self.rating = _rating
        self.dt = _dt

class FilmItem:
    def __init__(self, _name, _year, _genre, _producer, _country):
        self.name = _name
        self.year = _year
        self.genre = _genre
        self.producer = _producer
        self.country = _country


def GetUserData(user_id):
    result = []
    url = 'https://www.afisha.ru/personalpage/{}/feed/'.format(str(user_id))
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    elements = soup.findAll('div', 'b-review')
    for el in elements:
        film_link = ''
        review_block = el.findAll('div', 'user-review-header')
        if review_block:
            object_type = review_block[0].findAll('p', 'object-type')[0].text
            if object_type == 'Фильм':
                # get rating
                rating = review_block[0].findAll('div', 'b-rating')[0].text
                try:
                    rating = int(rating[12])
                except: rating = -1

                # get film link
                for cont in review_block[0].contents:
                    if hasattr(cont, 'contents'):
                        for cont1 in cont:
                            if hasattr(cont1, 'attrs'):
                                if 'href' in cont1.attrs.keys():
                                    film_link = cont.contents[0].attrs['href']
                                    film_link = film_link.replace('https://','')
                                    break

                # get review data
                dt = el.findAll('div', 'b-entry-info')
                if dt:
                    dt = dt[0].text
                    dt = dt.replace('\r', '')
                    dt = dt.replace('\n', '')
                    dt = dt.replace('\t', '')
                else: dt = ''

                result.append(ReviewItem(film_link, rating, dt))

    return result

def GetFilmData(url):
    if 'https://' not in url: url = 'https://' + url
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    elements = soup.findAll('div','b-object-summary')
    if elements:
        # get name
        name = genre = producer = country = ''
        year = 0
        names_data = elements[0].findAll('div', 'b-object-header')
        if names_data:
            name = names_data[0].contents[1].contents[1]
            name = name.replace('\n', '').strip()
            name = sf.clear_string(name, sf.rus_letters + sf.lat_letters + sf.puncts + sf.digits)
        else:
            print("Error parsling name in {}".format(url))
            return 0

        additional_data = elements[0].findAll('div', 'm-margin-btm')
        if additional_data:
            # get genre
            genres = additional_data[0].findAll('div', 'b-tags')
            if genres:
                genre = genres[0].text.replace('\n','')
                genre = sf.clear_string(genre, sf.rus_letters + sf.lat_letters + sf.puncts)

            # get country
            countries = additional_data[0].findAll('span','creation')
            country_arr = []
            if countries:
                country_arr = countries[0].text.split(',')
                for c_id in range(len(country_arr)-2):
                    country += country_arr[c_id] + ','
                country = country[:len(country)-1]
                country = sf.clear_string(country, sf.rus_letters+sf.lat_letters+sf.puncts)

            # get year
            if len(country_arr) > 1:
                for i in range(len(country_arr),0,-1):
                    try:
                        year = int(country_arr[i])
                        if year in range(1900,2020): break
                    except:
                        pass

            # get producer
            flg_found = False
            try:
                for cont in additional_data[0].contents:
                    if flg_found:
                        producer = sf.clear_string(cont.text, sf.rus_letters+sf.lat_letters+sf.puncts)
                        break
                    if 'Режиссер' in cont:
                        flg_found = True
            except:
                print("Error parsling producer in {}".format(url))

        else:
            print("Error parsling additional data in {}".format(url))
            return 0

        return FilmItem(name, year, genre, producer, country)

class Spectator:
    base_marks = {} # film_id:mark
    referents = {} # a dictionary of dictionaries
    user_ranking = {}

    def __init__(self, user_id, conn):
        self.c = conn.cursor()
        self.id = user_id
        if user_id > 0:
            self.base_marks = self._fill_base_marks('marks')
        else:
            self.base_marks = self._fill_base_marks('base_marks')


    def _fill_base_marks(self,target_table):
        base_marks = {}
        marks_rows = self.c.execute('SELECT film_id,mark FROM {} WHERE user_id={}'.format(target_table,self.id)).fetchall()
        for row in marks_rows:
            base_marks[row[0]]=row[1]
        return base_marks


    def get_referent_users(self):
        for film_id in self.base_marks.keys():
            user_rows = self.c.execute('SELECT user_id,mark FROM marks WHERE film_id={}'.format(film_id))
            for row in user_rows:
                user_id = row[0]
                mark = row[1]
                if self.referents.get(user_id):
                    self.referents[user_id][film_id] = mark
                else:
                    self.referents[user_id] = {film_id:mark}

    def get_users_ranking(self):

        for user_id in self.referents.keys():
            cnt_equal = 0
            if len(self.referents[user_id]) > 1:
                for film_id in self.referents[user_id].keys():
                    if self.referents[user_id][film_id]==self.base_marks[film_id]:
                        cnt_equal+=1
                if cnt_equal > 1:
                    self.user_ranking[user_id] = cnt_equal

    def get_distance_hamming(self, another_user_marks):
        """
        calculates Hamming measure between films in both base_makrs and another_user_marks
        :param another_user_marks: {film_id:mark}
        :return: cosine measure
        """
        # get marks for common films
        v1 = []
        v2 = []
        for k in another_user_marks.keys():
            if self.base_marks.get(k):
                v1.append(self.base_marks[k])
                v2.append(another_user_marks[k])

        cnt_ham  = 0
        if len(v1) > 0:
            for i in range(len(v1)):
                if v1[i] == v2[i]:
                    cnt_ham += 1

        return cnt_ham , len(v1)


#compName = "Ilya"
#compName = 'work'
compName = 'work'
driverPath = ""
dbasePath = ""
if compName == "Ilya":
    driverPath = "C:/Users/илья/Dropbox/Ilya-Papa/father_files/drivers/chromedriver.exe"
    dbasePath = "C:/Users/илья/Dropbox/Ilya-Papa/father_files/bases/kinoman.db"
elif compName == "work":
    driverPath = "C:/Program Files (x86)/Google/Chrome/Application/chromedriver.exe"
    dbasePath = "C:/Kovalenko/data_center/dbases/kinoman_afisha.db"
elif compName == "notebook":
    driverPath = "C:/Users/Vlad/Dropbox/Ilya-Papa/father_files/drivers/chromedriver.exe"
    dbasePath = "C:/Users/Vlad/Dropbox/Ilya-Papa/father_files/bases/kinoman.db"


conn = lite.connect(dbasePath)
c = conn.cursor()

"""
min_user_id = 2456438
MAX_USER_ID = 2877814
while 1:
    try:
        for user_id in range(min_user_id, MAX_USER_ID):
            film_id = 0
            reviews = GetUserData(user_id)
            if reviews:
                for item in reviews:
                    if item.rating != -1:
                        film_id = c.execute("SELECT id FROM films WHERE link = '{}'".format(item.film_link)).fetchone()
                        if film_id:
                            film_id = film_id[0]
                        else: # a new film found - save it to db
                            film_data = GetFilmData(item.film_link)
                            if film_data:
                                req = "INSERT INTO films (name,genre,producer,country,link,year) VALUES ('{}','{}','{}','{}','{}',{})".format(film_data.name,film_data.genre,film_data.producer,film_data.country,item.film_link,film_data.year)
                                c.execute(req)
                                conn.commit()
                                film_id = c.execute("SELECT id FROM films WHERE link='{}'".format(item.film_link)).fetchone()[0]

                        if film_id:
                            req = "INSERT INTO marks (mark,dt,user_id,film_id) VALUES ({},'{}',{},{})".format(item.rating,item.dt,user_id,film_id)
                            c.execute(req)
                conn.commit()
            #time.sleep(0.2)
            if film_id: print(str(user_id) + '!' + str(film_id))
            else: print(user_id)
            min_user_id = user_id + 1
        else:
            break
    except: # restart with new min_user_id
        pass
"""

"""
# stage 2: marks to binomials

# get all users
users = []
user_rows = c.execute('SELECT DISTINCT user_id FROM marks WHERE id > 28').fetchall()
for row in user_rows:
    users.append(row[0])

for user_id in users:
    marks = []
    mark_rows = c.execute('SELECT mark FROM marks WHERE user_id={} ORDER BY mark'.format(user_id))
    for row in mark_rows:
        marks.append(row[0])

    if len(marks) % 2 == 0:
        mediana = (marks[len(marks)-1] + marks[len(marks)-1]) / 2
    else:
        mediana = marks[int((len(marks)-1)/2)]
    #qstr =
    c.execute('UPDATE marks SET mark={} WHERE user_id={} AND mark<{}'.format(-1, user_id, mediana))
    c.execute('UPDATE marks SET mark={} WHERE user_id={} AND mark>={}'.format(1,user_id,mediana))
    conn.commit()
    print(user_id)
"""

# calculate modules for each user
users = []
users_rows = c.execute("SELECT user_id FROM grp_users WHERE cnt_marks > 1 and user_id >= 494288 ORDER BY user_id").fetchall()
for row in users_rows:
    users.append(row[0])

for user_id in users:
    print(user_id)
    films = []
    films_rows = c.execute("SELECT film_id FROM marks WHERE user_id={}".format(user_id))
    for row in films_rows:
        films.append(row[0])

    referents = []
    for film_id in films:
        ref_rows = c.execute("SELECT user_id FROM marks WHERE film_id = {} AND user_id > {}".format(film_id,user_id)).fetchall()
        for row in ref_rows:
            if row[0] in users and row[0] not in referents:
                referents.append(row[0])

    if referents:
        for ref_id in referents:
            s1 = Spectator(user_id, conn)
            s2 = Spectator(ref_id, conn)
            cnt_ham, cnt_common = s1.get_distance_hamming(s2.base_marks)
            if cnt_common > 1:
                cnt_ham = round(cnt_ham / cnt_common, 3)
                if cnt_ham < 1:

                    c.execute('INSERT INTO pairs_users (user1_id,user2_id,dist,cnt_common) VALUES ({},{},{},{})'.format(user_id, ref_id, cnt_ham, cnt_common))
        conn.commit()

