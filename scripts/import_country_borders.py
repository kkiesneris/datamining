import oracledb
import csv

connection = oracledb.connect(
    user="user",
    password="password",
    dsn="//dbserver:1521/PDB1")

insertCursor = connection.cursor()
insertData = "insert into country_borders (country, border_country) values (:1, :2)"

with open('country_borders.csv',newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    next(reader, None)
    for row in reader:
        if len(row[0])>0 and len(row[2])>0:
            print("[%s] [%s]"%(row[0],row[2]))
            insertCursor.execute(insertData,(row[0],row[2]))
    connection.commit()
