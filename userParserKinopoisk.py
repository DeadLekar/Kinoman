# parse all kinopoisk users
import time
import re
from bs4 import BeautifulSoup
import sqlite3 as lite
import requests
import serviceFunctions as sf

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


def GetUserData(user_page, request_headers):
    print('Getting ratings from user ' + user_page)
    film_link_mask = 'http://www.imdb.com/title/{}/reviews?ref_=tt_urv'
    user_link_prefix = 'http://www.imdb.com'
    result = []
    film_link = ''
    rating = -1
    with requests.Session() as session:
        session.headers = request_headers
        i = 0
        while 1:
            i +=1
            time.sleep(1)
            try:
                page = session.get(user_page)
            except: break
            soup = BeautifulSoup(page.content, "html.parser")
            elements = soup.findAll('div', 'lister-item-content') # get table with marks
            for el in elements:
                is_series = False
                el_data = el.findAll('h3','lister-item-header')
                if el_data:
                    # get film link
                    for cont in el_data[0].contents:
                        if hasattr(cont, 'attrs'):
                            if cont.attrs.get('href'):
                                film_id = cont.attrs.get('href').split('/')[2]
                                film_link = film_link_mask.format(film_id)
                            # check if item is series
                            if cont.attrs.get('class') and 'lister-item-year' in cont.attrs.get('class'):
                                if ('–') in cont.text:
                                    is_series = True
                                    break
                                else:
                                    year = sf.clear_string(cont.text, sf.digits)
                                    if not year or int(year) < 2000:
                                        is_series = True
                                        break
                if not is_series: # do not take series ratings or films earlier 2000 year
                    # get mark
                    rating_widget = el.findAll('div','ipl-rating-widget')
                    if rating_widget:
                        for cont in rating_widget[0].contents:
                            if hasattr(cont, 'attrs'):
                                if 'ipl-rating-star--other-user' in cont.attrs.get('class'):
                                    rating = cont.text.replace('\n','')
                        result.append(ReviewItem(film_link, rating, ''))
            # look for next page
            paginator = soup.findAll('div','list-pagination')
            user_page = ''
            if paginator:
                for cont in paginator[0].contents:
                    if hasattr(cont,'attrs'):
                        user_page = cont.attrs.get('href')
            if not user_page: break
            user_page = user_link_prefix + user_page
            print ('-{}'.format(i))
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

def GetFilmUsers(film_link, request_headers):
    print('Film {}: collecting users'.format(film_link))
    film_id_arr = film_link.split('/')[4].split('-')
    film_id = film_id_arr[len(film_id_arr) - 1]
    i = 0
    users = []

    with requests.Session() as session:
        request_headers['Referer'] = film_link
        session.headers = request_headers
        # get token
        response = session.get(film_link)
        soup = BeautifulSoup(response.content, 'html.parser')

        # check if item is not series and have russian version and was issued in 2000 or later
        parent_div = soup.findAll('div','parent')
        if parent_div:
            year_str = parent_div[0].findAll('span', 'nobr')
            if '–' in year_str[0].text:
                return
            year = sf.clear_string(year_str[0].text, sf.digits)
            if int(year) < 2000:
                return
            is_russian = False
            for cont in parent_div[0].contents:
                if hasattr(cont, 'contents') and hasattr(cont, 'text'):
                    for letter in sf.rus_letters:
                        if letter in cont.text:
                            is_russian = True
                            break
                    if is_russian: break
            if not is_russian:
                return

        elements = soup.findAll('div', 'lister-item')
        if elements:
            for el in elements:
                user_link_row = el.findAll('span', 'display-name-link')
                if user_link_row:
                    user_link = user_link_row[0].contents[0].attrs['href']
                    user_id = user_link[8:len(user_link) - 13]
                    users.append(user_id)

        pattern = re.compile('data-key="(.*)\sd')
        token_search = pattern.search(response.text)
        if token_search: # next page is founded
            token = token_search.group(1)
            token = token[:len(token)-1]
            token_link = film_link.split('?')[0]
            prev_review_cnt = 0
            while 1: # adding new reviews
                time.sleep(1)
                i +=1
                print('-{}'.format(i))
                if token:
                    response = session.get('{}/_ajax?ref_=undefined&paginationKey={}'.format(token_link, token))
                else:
                    response = session.get(film_link)
                soup = BeautifulSoup(response.content, 'html.parser')
                elements = soup.findAll('div', 'lister-item')
                if elements:
                    for el in elements:
                        user_link_row = el.findAll('span', 'display-name-link')
                        if user_link_row:
                            user_link = user_link_row[0].contents[0].attrs['href']
                            user_id = user_link[8:len(user_link) - 13]
                            users.append(user_id)
                pattern = re.compile('data-key="(.*)"')
                try:
                    token = pattern.search(response.text).group(1)
                except: break
    return users




#compName = "Ilya"
#compName = 'work'
compName = 'work'
driverPath = ""
dbasePath = dbasePath_marks = ""
if compName == "Ilya":
    driverPath = "C:/Users/илья/Dropbox/Ilya-Papa/father_files/drivers/chromedriver.exe"
    dbasePath = "C:/Users/илья/Dropbox/Ilya-Papa/father_files/bases/kinoman.db"
elif compName == "work":
    driverPath = "C:/Program Files (x86)/Google/Chrome/Application/chromedriver.exe"
    dbasePath = "C:/Kovalenko/data_center/dbases/kinoman_imdb.db"
    dbasePath_marks = "C:/Kovalenko/data_center/dbases/kinoman_imdb_marks_2.db"
elif compName == "notebook":
    driverPath = "C:/Users/Vlad/Dropbox/Ilya-Papa/father_files/drivers/chromedriver.exe"
    dbasePath = "C:/Users/Vlad/Dropbox/Ilya-Papa/father_files/bases/kinoman.db"


conn = lite.connect(dbasePath)
conn_marks = lite.connect(dbasePath_marks)
c = conn.cursor()
c_marks = conn_marks.cursor()
user_page_mask = 'http://www.imdb.com/user/ur{}/ratings'
request_headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36',
                           'Accept': '*/*',
                           'Accept-Encoding': 'gzip, deflate',
                           'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,sr;q=0.6',
                           'Connection': 'keep-alive',
                           'Cookie': 'uu=BCYons09VGpNpxlaYOC5cXhui7poC6BSn2fEwGS99H7VI3O13NxAxI8KgE1UHoZCHibkV64ImTFa%0D%0AZGehGTOYeT6cADNJH3JDl38SpkTWrCYX0wEj1xEPv2xRB81InXzsVknag2FkFrU5Vrk7H3mSPSV7%0D%0Afbuxr3zDbe8S7FAaqZaWuYA3DGeWzE6giCd96Q4xfg5NbcZL99nN-mSCfKmx8tktEC7OYXMx-OhK%0D%0ApY2oon4HSP2tMl_odgsCe26LJOBlw_8P3O1OsAqa3E9r1TCZ5bl1aQ%0D%0A; session-id=135-1802609-1821751; session-id-time=2146639399; ubid-main=135-1161782-5091158; as=%7B%22h%22%3A%7B%22t%22%3A%5B0%2C0%5D%2C%22tr%22%3A%5B0%2C0%5D%2C%22ib%22%3A%5B0%2C0%5D%7D%2C%22n%22%3A%7B%22t%22%3A%5B0%2C0%5D%2C%22tr%22%3A%5B0%2C0%5D%2C%22in%22%3A%5B0%2C0%5D%2C%22ib%22%3A%5B0%2C0%5D%7D%7D; session-token=4zfuOiM3FAiAw/tELHNfvG76KkJ7+1ZvPqUPfz882J8J+ceS2X7UDlCq8sk7REDp1qdxaPzvShiCxNFw3RBGgb4MvehMFPdLu3PvEPV9U7LH5bWOfB+91k4vwIWyB8phQxXuRWCvtKtPnpZvHUsQSuTsy3/ntVfe2rSX9sczVOsbfrp6MO/GRmWfgQ2Fc8fI',
                           'Host': 'www.imdb.com',
                           'Referer': '',
                           'X-Requested-With': 'XMLHttpRequest'
                           }
# TEST AREA
# user_page = 'http://www.imdb.com/user/ur3404495/ratings'
# reviews = GetUserData(user_page, request_headers)
# film_link = 'http://www.imdb.com/title/tt1534184/reviews?ref_=tt_urv'
# users = GetFilmUsers(film_link, request_headers)

while 1:
    # get unchecked film
    new_film_row = c.execute("SELECT link,id FROM films WHERE isChecked=0 LIMIT 1").fetchone()
    if new_film_row:
        new_film_link = new_film_row[0]
        new_film_id = new_film_row[1]
        # get film's users
        users = []
        try:
            users = GetFilmUsers(new_film_link, request_headers)
        except:
            print('Unable to add new users')
        if users:
            cnt_new_users = cnt_old_users = 0
            for user_id in users:
                saved_id_row = c.execute('SELECT id FROM users WHERE name = {} LIMIT 1'.format(user_id)).fetchone()
                if not saved_id_row: # got new user
                    c.execute('INSERT INTO users(name) VALUES ({})'.format(user_id))
                    cnt_new_users += 1
                else: cnt_old_users += 1
            conn.commit()
            print ('{} users added; {} old users founded'.format(cnt_new_users, cnt_old_users))
            # check if we have new users
            new_users_row = c.execute('SELECT name FROM users WHERE isChecked = 0').fetchall()
            if new_users_row:
                for new_user_id in new_users_row:
                    new_user_id = new_user_id[0]
                    user_page = user_page_mask.format(new_user_id)
                    # get user marks
                    reviews = GetUserData(user_page, request_headers)
                    if reviews:
                        cnt_new_films = cnt_old_films = 0
                        for item in reviews:
                            film_id = c.execute("SELECT id FROM films WHERE link = '{}'".format(item.film_link)).fetchone()
                            if film_id:
                                film_id = film_id[0]
                                cnt_old_films += 1
                            else:  # a new film found - save it to db
                                # film_data = GetFilmData(item.film_link)
                                # if film_data:
                                c.execute("INSERT INTO films (link) VALUES ('{}')".format(item.film_link))
                                conn.commit()
                                cnt_new_films += 1
                                film_id = c.execute("SELECT id FROM films WHERE link='{}'".format(item.film_link)).fetchone()[0]
                            if film_id:
                                req = "INSERT INTO marks (mark,dt,user_id,film_id) VALUES ({},'{}',{},{})".format(item.rating, item.dt, new_user_id, film_id)
                                try:
                                    c_marks.execute(req) # insert mark in special marks data base
                                except: print('Error:' + req)
                        conn_marks.commit()
                        print('{} new films added; {} old films founded'.format(cnt_new_films, cnt_old_films))
                    c.execute("UPDATE users SET isChecked=1 WHERE name={}".format(new_user_id))
                    conn.commit()
        else: print('Cannot get users for ' + new_film_link)
        c.execute('UPDATE films SET isChecked=1 WHERE id={}'.format(new_film_id))
        conn.commit()
    else: break