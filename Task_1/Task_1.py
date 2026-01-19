import mysql.connector
import pandas as pd
import os
import time

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
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    class VARCHAR(50),
    section VARCHAR(10),
    roll INT,
    mobile VARCHAR(15)
)
""")

print("Database ready.")

folder = "csv_files"
if not os.path.exists(folder):
    os.mkdir(folder)
    print("Folder created. Add csv files and run again.")
    exit()

tracking_file = "processed_files.txt"

def load_processed():
    if os.path.exists(tracking_file):
        with open(tracking_file, "r") as f:
            return set(f.read().splitlines())
    return set()

def mark_done(fpath):
    with open(tracking_file, "a") as f:
        f.write(fpath + "\n")

done_files = load_processed()

while True:
    print("\nChecking for files...")
    for root, dirs, files in os.walk(folder):
        for fname in files:
            if fname.endswith(".csv"):
                fpath = os.path.join(root, fname)
                
                if fpath in done_files:
                    print(f"Skipping: {fname}")
                    continue
                
                df = pd.read_csv(fpath)

                for idx, row in df.iterrows():
                    query = "INSERT INTO students(name,class,section,roll,mobile) VALUES(%s,%s,%s,%s,%s)"
                    values = (row["name"], row["class"], row["section"], row["roll"], row["mobile"])
                    try:
                        cur.execute(query, values)
                        conn.commit()
                        print("Added:", row["name"])
                    except:
                        print("Failed:", row["name"])
                
                done_files.add(fpath)
                mark_done(fpath)
                print(f"Done with {fname}")
                
    print("Waiting...")
    time.sleep(600)
