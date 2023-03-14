#Dieses Skript kümmert sich um das Loading der Source JobAgent in die Maria DB.

import mariadb
import pandas as pd
import sys

# Settings für PyCharm
desired_width = 400
pd.set_option('display.width', desired_width)
pd.set_option('display.max_rows', 300)
pd.set_option('display.max_columns', 10)
pd.set_option("display.max_colwidth", 15)

def check_csv():
    #Check the CSV, which will get imported to Maria DB
    csv_name = "JobsConcat_stage.csv"
    path = (f"/home/student/Cloud/Owncloud/SyncVM/CIP Project/JobsMerged/{csv_name}")
    df_JobsConcat = pd.read_csv(path, sep = ";")
    print(df_JobsConcat.columns)
    print(df_JobsConcat.index) #Check Index
    print(df_JobsConcat.head(5))

def connect():
    #Verbinde mit Maria DB
    try:
        conn = mariadb.connect (
            user = "daniel",
            password = "Le1729-1781!",
            host = "localhost",
            port =3306,
            database = "CIP"
        )

    except mariadb.Error as e:
        print (f'Error connecting to MariaDB: {e}')
        sys.exit(1)

    #Create the cursor:
    #A cursor is a structure that allows you to go over records sequentially, and perform processing based on the result.
    cur = conn.cursor()

    return cur, conn

def create_table(cur):
    try:
        cur.execute("CREATE OR REPLACE TABLE JobsConcat("
                     "JobNr INT, Primärschlüssel VARCHAR(100), Stellentitel VARCHAR(50), Arbeitsort VARCHAR(50), Arbeitgeber VARCHAR(50), Arbeitgeber_Bewertung FLOAT,"
                     "Datum_Veröffentlichung DATE, Programmier_und_Softwarekenntnisse VARCHAR(100),Kanton CHAR(2), Unternehmensgrösse VARCHAR(30), Arbeitspensum_min INT, Arbeitspensum_max INT, "
                    "Branche VARCHAR(50), Min_Ausbildungsanforderung VARCHAR(30), Führungsposition VARCHAR(50), Quelle VARCHAR(10), PRIMARY KEY (Primärschlüssel));")
        print("Tabelle wurde erstellt")

    except mariadb.Error as e:
        print(f"Error: {e}")

def load_data(cur, conn):
    try:
        cur.execute("LOAD DATA LOCAL INFILE '/home/student/Cloud/Owncloud/SyncVM/CIP Project/JobsMerged/JobsConcat_stage.csv'"
                    "INTO TABLE JobsConcat FIELDS TERMINATED BY ';' LINES TERMINATED BY '\n' IGNORE 1 ROWS "
                    "(JobNr, Primärschlüssel, Stellentitel, Arbeitsort, Arbeitgeber, Arbeitgeber_Bewertung, "
                    "Datum_Veröffentlichung, Programmier_und_Softwarekenntnisse, Kanton, Unternehmensgrösse, Arbeitspensum_min, Arbeitspensum_max, Branche,"
                    "Min_Ausbildungsanforderung, Führungsposition, Quelle);")
        conn.commit()
        print("Daten wurden erfolgreich hochgeladen")
    except mariadb.Error as e:
        print(f"Error: {e}")

def query(cur):
    #Abfragen zum Testen:
    #Query1
    print("Testing one query:")
    cur.execute ("SELECT * FROM JobsConcat ORDER BY JobNr")
    myresult = cur.fetchall()
    for x in myresult:
         print(x)

    print(30*"*")
    #Query2
    print("Testing another one:")
    cur.execute ("SELECT PRIMÄRSCHLÜSSEL FROM JobsConcat WHERE JobNr = 5")
    myresult2 = cur.fetchall()
    for x in myresult2:
         print(x)



def deconnect (conn):
    conn.close()

def main():
    check_csv()
    cur,conn = connect()
    create_table(cur)
    load_data (cur,conn)
    query(cur)
    deconnect (conn)


if __name__ == "__main__":
    main()




