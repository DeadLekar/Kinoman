from selenium import webdriver
import time
from bs4 import BeautifulSoup
import sqlite3 as lite
import serviceFunctions as sf
#from selenium.webdriver.common.keys import Keys

def find_cont(str_to_find, element):
    my_str = str(str_to_find)
    if hasattr(element, "contents"):
        for cont in element.contents:
            if hasattr(cont, "text"):
                year_ind = cont.text.find(my_str)
            else:
                year_ind = cont.find(my_str)
            if year_ind > 0:
                return cont

#compName = "Ilya"
#compName = 'work'
compName = 'notebook'
driverPath = ""
dbasePath = ""
if compName == "Ilya":
    driverPath = "C:/Users/илья/Dropbox/Ilya-Papa/father_files/drivers/chromedriver.exe"
    dbasePath = "C:/Users/илья/Dropbox/Ilya-Papa/father_files/bases/kinoman.db"
elif compName == "work":
    driverPath = "C:/Program Files (x86)/Google/Chrome/Application/chromedriver.exe"
    dbasePath = "C:/Users/vkovalenko/Dropbox/Ilya-Papa/father_files/bases/kinoman.db"
elif compName == "notebook":
    driverPath = "C:/Users/Vlad/Dropbox/Ilya-Papa/father_files/drivers/chromedriver.exe"
    dbasePath = "C:/Users/Vlad/Dropbox/Ilya-Papa/father_files/bases/kinoman.db"

conn = lite.connect(dbasePath)
c = conn.cursor()
c_user = conn.cursor()

#read default data from db
films_array_name = ""
txt_length_limit = 0
review_next_page_name = ""
review_link_suffix = ""
review_link_prefix = "http://"
review_page_name = ""
c.execute("SELECT * FROM properties")
for row in c:
    if row[0] == "txt_length_limit":
        txt_length_limit = int(row[1])
    elif row[0] == "films_array_name":
        films_array_name = row[1]
    elif row[0] == "review_next_page_name":
        review_next_page_name = row[1]
    elif row[0] == "review_link_suffix":
        review_link_suffix = row[1]
    elif row[0] == "review_page_name":
        review_page_name = row[1]

driver = webdriver.Chrome(driverPath)
c.execute("SELECT id, link FROM films WHERE isCheckedReviews is NULL")
for film in c.fetchall(): #go through films
    link = review_link_prefix + film[1] + review_link_suffix

    legitimate_symbols = sf.digits + sf.lat_letters + sf.rus_letters + sf.puncts
    driver.get(link)
    time.sleep(5)
    while 1: #go through all the film's reviews
        txt = driver.execute_script("return document.body.innerHTML")
        soup = BeautifulSoup(''.join(txt), "html.parser")
        elements = soup.find_all("div", review_page_name)
        for el in elements:
            #get review data
            user_id = 0
            mark_str = el.contents[1].contents[1].contents[1].contents[3].text
            if mark_str.find(":") != -1:
                mark_str_arr = mark_str.split(":")
                mark_str = mark_str_arr[1].strip()
                mark_str = mark_str[0:2].strip()
            else:
                mark_str = "0"
            user_name = el.contents[3].contents[3].contents[1].text
            user_name = sf.clear_string(user_name, legitimate_symbols)
            user_link = el.contents[3].contents[3].contents[1].contents[0].attrs['href'][2:]

            #save new user
            c_user.execute("SELECT id FROM users WHERE link='" + user_link + "'")
            for user in c_user.fetchall():
                user_id = user[0]
            if not user_id:
                c_user.execute("INSERT INTO users (name, link) VALUES ('" + user_name + "', '" + user_link + "')")
                conn.commit()
                c_user.execute("SELECT id FROM users WHERE link='" + user_link + "'")
                user_id = c_user.fetchone()[0]
            c_user.execute("INSERT INTO marks (mark, user_id, film_id) VALUES (" + mark_str + ", " + str(user_id) + ", " + str(film[0]) + ")")
            conn.commit()
        try:
            nextPage = driver.find_element_by_id(review_next_page_name)
            nextPage.click()
            time.sleep(5)
        except:
            c_user.execute("UPDATE films SET isCheckedReviews = 1 WHERE id =" + str(film[0]))
            conn.commit()
            break
conn.close()
driver.close()
