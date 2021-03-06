import sqlite3
import pandas as pd

dbname = 'marketdataSQL.db'


def createTable(dbname='marketdataSQL.db'):
    """
    create Tabme in sqlite3 DB
    """
    conn = sqlite3.connect(dbname)
    c = conn.cursor()
    c.execute('''CREATE TABLE quotes (
        Symbol varchar(10), 
        `Date` Date, 
        Open float, 
        High float, 
        Low float, 
        Close float,
        Volume int)''')

    conn.commit()
    conn.close()


def dfToSqlite(df, tableName, path='utils/marketdataSQL.db'):
    """
    Df to sqlite3 local DB

    :param 1: dataframe to send to sqlite DB
    :param 2: target table name in sqlite
    :param 3: path to salite db
    """
    conn = sqlite3.connect('utils/marketdataSQL.db')
    c = conn.cursor()
    df.to_sql(f'{tableName}', conn, if_exists='append', index=False)

    conn.commit()
    conn.close()


query = "select * from quotes"


def readSqlite(tableName, path='utils/marketdataSQL.db', query):
    conn = sqlite3.connect('utils/marketdataSQL.db')
    mycur = conn.cursor()
    mycur.execute(f"{query}")
    available_table = (mycur.fetchall())


df = pd.read_csv('utils/NASDAQ.csv')
dfToSqlite(df, 'quotes')


"""
Just creating a new DB and populating it
to test the DB swich (path switch) in the main flask app
"""


def createTable2(dbname='other.db'):
    """
    create Tabme in sqlite3 DB
    """
    conn = sqlite3.connect(dbname)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS projects (
                                        id integer PRIMARY KEY,
                                        name text NOT NULL,
                                        begin_date text,
                                        end_date text
                                    );''')

    conn.commit()
    conn.close()


def adddata(dbname='other.db'):
    """
    create Tabme in sqlite3 DB
    """
    conn = sqlite3.connect(dbname)
    c = conn.cursor()
    c.execute('''INSERT INTO projects
    (id,name,begin_date,end_date)
    VALUES
    (1,'test','2020','2021')''')

    conn.commit()
    conn.close()


# createTable2()
adddata()
