import sqlite3

con = sqlite3.connect("backend/data/arboviroses.db")
for row in con.execute("SELECT city, COUNT(*) FROM weekly_cases GROUP BY city;"):
    print(row)
con.close()