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

compName = "Ilya"
#compName = 'work'
#compName = 'notebook'
driverPath = ""
dbasePath = ""
if compName == "Ilya":
    driverPath = "C:/Users/илья/Dropbox/Ilya-Papa/father_files/drivers/chromedriver.exe"
elif compName == "work":
    driverPath = "C:/Users/илья/Dropbox/Ilya-Papa/father_files/drivers/chromedriver.exe"
elif compName == "notebook":
    driverPath = "C:/Users/илья/Dropbox/Ilya-Papa/father_files/drivers/chromedriver.exe"


conn = lite.connect(dbasePath)
c = conn.cursor()

#read default data from db
films_array_name = ""
txt_length_limit = 0
next_page_name = ""
c.execute("SELECT * FROM properties")
for row in c:
    if row[0] == "txt_length_limit":
        txt_length_limit = int(row[1])
    if row[0] == "films_array_name":
        films_array_name = row[1]
    if row[0] == "next_page_name":
        next_page_name = row[1]

curr_year = 2010
contInd = 0
flgBreak = False
assert films_array_name != 0, "films_array_name is empty"
assert txt_length_limit != "", "txt_length_limit is empty"
legitimate_symbols = sf.digits + sf.lat_letters + sf.rus_letters + sf.puncts

driver = webdriver.Chrome(driverPath)
driver.get("http://www.afisha.ru/movie/y" + str(curr_year) + "/cinema/")
# driver.get("http://www.afisha.ru/movie/y2014/cinema/page12/")
time.sleep(5)
while 1:  # go through all the films' pages
    txt = driver.execute_script("return document.body.innerHTML")
    soup = BeautifulSoup(''.join(txt), "html.parser")
    elements = soup.find("div", films_array_name)
    for el in elements.contents:
        if hasattr(el, "contents"):
            if len(el.contents) > 10:
                # get movie data
                country = ""
                producer = ""
                genre = ""
                link = el.contents[1].attrs['href'][2:]
                marksNum = int(el.contents[3].contents[1].text)
                midMarkStr = el.contents[3].contents[3].contents[0].contents[3].text
                midMarkArr = midMarkStr.split(":")
                midMark = midMarkArr[1].strip()
                midMarkStr = midMark[0:3]
                if midMarkStr.find(",") != -1:
                    midMark = float(midMarkStr.replace(",", "."))
                else:
                    midMarkStr = sf.clear_string(midMarkStr, sf.digits)
                    midMark = float(midMarkStr)
                filmName = sf.clear_string(el.contents[5].contents[1].text, legitimate_symbols)
                contInd = 7
                if len(el.contents[contInd].contents) < 3:
                    contInd += 2
                if hasattr(el.contents[contInd], "text"):
                    if el.contents[contInd].text != "":
                        # genre = el.contents[contInd].contents[1].text[0:len(el.contents[contInd].contents[1].text)-9]
                        genre = el.contents[contInd].contents[1].text
                contInd += 2
                if len(el.contents) > contInd:
                    if el.contents[contInd].text.find("Режиссер") == -1:
                        contInd += 2
                if len(el.contents) > contInd:
                    if hasattr(el.contents[contInd].contents[1].contents[0], "text"):
                        producer = sf.clear_string(el.contents[contInd].contents[1].contents[0].text, legitimate_symbols)
                    else:
                        producer = sf.clear_string(el.contents[contInd].contents[1].contents[1].text, legitimate_symbols)
                else:
                    contInd -= 2
                year_el = find_cont(curr_year, el.contents[contInd])
                while year_el and hasattr(year_el, "contents"):
                    year_el = find_cont(curr_year, year_el)
                year_ind = year_el.find(str(curr_year))
                country = year_el[0:year_ind - 2]
                country = sf.clear_string(country[5:], legitimate_symbols)

                # save movie data
                if marksNum > 0:
                    command = "INSERT INTO films (name, year, genre, producer, country, link, marksNum, midMark) VALUES ('" + filmName + "', " + str(curr_year) + ", '" + genre + "', '" + producer + "', '" + country + "', '" + link + "', " + str(marksNum) + ", " + str(midMark) + ")"
                    c.execute(command)
                    conn.commit()
                else:
                    flgBreak = True
                    break
    #get next page
    if flgBreak: break
    nextPage = driver.find_element_by_id("ctl00_CenterPlaceHolder_ucPager_NextPageLink")
    if nextPage:
        nextPage.click()
        time.sleep(5)
conn.close()
driver.close()
