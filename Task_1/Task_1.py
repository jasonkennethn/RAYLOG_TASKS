import mysql.connector
import pandas as pd
import os
import time

# connecting
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root123"
)
cur = conn.cursor()
cur.execute("CREATE DATABASE IF NOT EXISTS studentdb")
cur.execute("USE studentdb")
cur.execute("""
CREATE TABLE IF NOT EXISTS students(
    name VARCHAR(100),
    class VARCHAR(50),
    section VARCHAR(10),
    roll INT PRIMARY KEY,
    mobile VARCHAR(15)
)
""")

print("Database ready.")

# checking csv folder
folder = "csv_files"
if not os.path.exists(folder):
    os.mkdir(folder)
    print("Folder created. Add csv files and run again.")
    exit()

while True:
    print("\nChecking for files...")
    for path, dirs, files in os.walk(folder):
        for f in files:
            if f.endswith(".csv"):
                file_path = os.path.join(path, f)
                data = pd.read_csv(file_path)

                for i, r in data.iterrows():
                    cur.execute("SELECT * FROM students WHERE roll=%s", (r["roll"],))
                    check = cur.fetchone()
                    if not check:
                        sql = "INSERT INTO students(name,class,section,roll,mobile) VALUES(%s,%s,%s,%s,%s)"
                        val = (r["name"], r["class"], r["section"], r["roll"], r["mobile"])
                        try:
                            cur.execute(sql, val)
                            conn.commit()
                            print("Inserted:", r["name"])
                        except:
                            print("Error inserting:", r["name"])
    print("Waiting for next check...")
    time.sleep(600)
