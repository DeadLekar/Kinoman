import sqlite3 as lite

#compName = "Ilya"
#compName = 'work'
compName = 'Ilya'
driverPath = ""
dbasePath = ""
if compName == "Ilya":
    driverPath = "C:/Users/илья/Dropbox/Ilya-Papa/father_files/drivers/chromedriver.exe"
    dbasePath = "C:/tmp/kinoman.db"
elif compName == "work":
    driverPath = "C:/Program Files (x86)/Google/Chrome/Application/chromedriver.exe"
    dbasePath = "C:/Users/vkovalenko/Dropbox/Ilya-Papa/father_files/bases/kinoman.db"
elif compName == "notebook":
    driverPath = "C:/Users/Vlad/Dropbox/Ilya-Papa/father_files/drivers/chromedriver.exe"
    dbasePath = "C:/Users/Vlad/Dropbox/Ilya-Papa/father_files/bases/kinoman.db"

conn = lite.connect(dbasePath)
c = conn.cursor()
users = c.execute("SELECT id FROM users WHERE flgChecked = 0 ORDER BY id")
for u_row in users.fetchall():
    flg_stop = c.execute("SELECT propertyValue FROM properties WHERE propertyName = 'stop'").fetchone()
    if flg_stop[0] == 1:
        break
    cr_user_id = u_row[0]
    cr_user_films = c.execute("SELECT mark, filmID FROM marks WHERE userID = " + str(cr_user_id)).fetchall()
    next_user = str(u_row[0])

    users_to_compare = c.execute("SELECT id FROM users WHERE id > " + next_user)
    cnt_commit = 0
    for u1_row in users_to_compare.fetchall():
        user_compare_id = u1_row[0]
        user_compare_films = c.execute("SELECT mark, filmID FROM marks WHERE userID = " + str(user_compare_id)).fetchall()
        cnt_fits = 0
        diff_sum = 0
        mid_diff = 0
        for cr_mark in cr_user_films:
            mark_to_remove = 0
            for compare_mark in user_compare_films:
                if cr_mark[1] == compare_mark[1]:
                    mark_to_remove = compare_mark
                    cnt_fits += 1
                    diff = abs(cr_mark[0] - compare_mark[0])
                    diff_sum += diff
                    break
            if mark_to_remove:
                user_compare_films.remove(mark_to_remove)
        if cnt_fits > 0:
            mid_diff = diff_sum / cnt_fits
        common_share = int(100 * cnt_fits / (len(cr_user_films) + len(user_compare_films)))
        c.execute("INSERT INTO pairs (user1ID, user2ID, distance, commonShare) VALUES ("+ str(cr_user_id) + "," + str(user_compare_id) + "," + str(mid_diff) + "," + str(common_share) +")")
        #cnt_commit +=1
        #if cnt_commit == 5000:
        #    conn.commit()
        #    cnt_commit = 0
    c.execute("UPDATE users SET flgChecked = 1 WHERE id = " + str(cr_user_id))
    conn.commit()
    # print(cr_user_id)
