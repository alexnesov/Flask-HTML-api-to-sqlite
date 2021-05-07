"""
Author: Alexandre Nesovic
"""
from flask import Flask, render_template, request
import sqlite3
import pandas as pd
import os
import time
import traceback


app = Flask(__name__, static_url_path='/static')

sql_output = []
SQL_COMMAND = ""
DB_USER_INPUT = ""
INIT_DB_USER_INPUT = True
currentWD = os.path.dirname(__file__)  # WD = working directory
tempDB = "temp.db"
temp_db = os.path.join(currentWD, tempDB)

chunk_size = 300000


# To: Add traceback to sql error
# Nb lines total and preview warning
def getNRows():
    """
    Returns an integer, being the number of rows for the last query executed
    by the user
    """

    temp_con = sqlite3.connect(f'{temp_db}')
    tempCursor = temp_con.cursor()

    tempCursor.execute("SELECT COUNT(*) FROM temp_table;")

    nRows = tempCursor.fetchall()
    nRows = nRows[0][0]

    temp_con.commit()
    temp_con.close()

    return nRows


def toCSVinChunks(CSVname):
    """
    Parameters
    ----------
    CSVname : string
        name of CSV (as specified by user input or as "sqloutput.csv" if user input empty), is going to be saved in root folder
    Returns
    -------
    None.

    The function is designed with big data in mind.
    Meaning that if the size of the table is big, it will transfer the data from sqlite to csv in chunks of data.
    """
    if CSVname == "":
        CSVname = "sqloutput"

    temp_con = sqlite3.connect(f'{temp_db}')
    tempCursor = temp_con.cursor()

    nRows = getNRows()

    try:
        if nRows < 600000:
            tempCursor.execute(f"SELECT * FROM temp_table")

            colNames = list(map(lambda x: x[0], tempCursor.description))
            result = tempCursor.fetchmany(chunk_size)

            init = True
            ITER = 1
            while len(result) != 0:
                print("Iteration nb°: ", ITER)
                ITER += 1

                print("Converting result to df")
                df_result = pd.DataFrame(result, columns=colNames)
                if init == True:
                    print("Saving as CSV. . .")
                    print(df_result)
                    df_result.to_csv(
                        f'{currentWD}/{CSVname}.csv', index=False)
                    init = False
                else:
                    print("Saving as CSV. . .")
                    df_result.to_csv(f'{currentWD}/{CSVname}.csv', mode='a', header=False,
                                     index=False)

                del df_result  # liberating memory because of potentially big data
                result = tempCursor.fetchmany(chunk_size)
        else:
            print("Number exceeds reasonnable capacity for a CSV format (>600000 rows). Please refine the query (group the data or aggregate it) \
                  to obtain a lighter output.")
    except PermissionError:
        print(
            "File used by another person, yourself, or simply not authorized to overwrite")

    temp_con.commit()
    temp_con.close()


def readSqlite(dbpath, SQL_COMMAND):
    """
    Parameters
    ----------
    dbpath : string
        Path to db to query
    SQL_COMMAND : string
        User input, being the SQL command to execute
    Returns
    -------
    sql_output : List of tuples
        The output generated after the user input (sql comand) is executed
    colNames : List
        Name of the generated table columns
    dbCursor : sqlite3.Cursor
        Cursor pointing towards the sqlite db to open, contains the sql 
        command information
    """

    conn = sqlite3.connect(f'{dbpath}')
    dbCursor = conn.cursor()

    if os.path.isfile(temp_db):
        print('Temp DB exists. Old Data is overwritten, and new data is added.')
    else:
        print('Creating the temporary table.')
        temp_con = sqlite3.connect(f'{temp_db}')
        temp_con.commit()
        temp_con.close()
    
    print('Populating the temporary table. . .')
    dbCursor.execute(f"ATTACH DATABASE '{temp_db}' AS temp_db;")
    dbCursor.execute("DROP TABLE IF EXISTS temp_db.temp_table")
    dbCursor.execute(f"CREATE TABLE temp_db.temp_table AS {SQL_COMMAND};")
    dbCursor.execute(f"DETACH DATABASE 'temp_db';")
    print('Temporary table created and populated.')
    conn.commit()
    conn.close()
    # Getting the name of the table in the query, to be able to place it in
    # the coming select count(*) n rows
    nRows = getNRows()
    print("nRows: ", nRows)

    temp_con = sqlite3.connect(f'{temp_db}')
    tempCursor = temp_con.cursor()

    
    print('Reading data from temporary table to be displayed in browser.')
    tempCursor.execute(f"SELECT * FROM temp_table")

    if nRows > 500:
        sql_output = tempCursor.fetchmany(500)
    else:
        sql_output = tempCursor.fetchall()

    colNames = list(map(lambda x: x[0], tempCursor.description))

    temp_con.commit()
    temp_con.close()

    return sql_output, colNames, nRows


@app.route('/')
def home():

    return render_template('mainpage.html',
                           currentWD=currentWD,
                           DB_USER_INPUT=DB_USER_INPUT,
                           valid=True)
# to do: init command to avoid red arrow


def readTempDB():

    temp_con = sqlite3.connect(f'{temp_db}')
    tempCursor = temp_con.cursor()
    tempCursor.execute(f"SELECT * FROM temp_table")

    nRows = getNRows()

    if nRows > 500:
        sql_output = tempCursor.fetchmany(500)
    else:
        sql_output = tempCursor.fetchall()

    colNames = list(map(lambda x: x[0], tempCursor.description))

    temp_con.commit()
    temp_con.close()

    return sql_output, colNames, nRows


def STD_FUNC_TRUE(dbpath, CSVSaveMode=False):
    """
    Set of standard variables to be passed inside each render_template()
    func to be displayed through HTML
    """
    global INIT_DB_USER_INPUT

    if CSVSaveMode == False:
        sql_output, colNames, nRows = readSqlite(
            dbpath, SQL_COMMAND=SQL_COMMAND)
    else:
        sql_output, colNames, nRows = readTempDB()

    widthDF = list(range(len(colNames)))
    df = pd.DataFrame(sql_output)

    standard_args = dict(
        sql_output=sql_output,
        colNames=colNames,
        widthDF=widthDF,
        valid=True,
        currentWD=currentWD,
        nRows=nRows,
        INIT=INIT_DB_USER_INPUT,
        DB_USER_INPUT=DB_USER_INPUT
    )

    return standard_args, df


def STD_FUNC_FALSE():
    """
    Set of standard variables to be passed inside each render_template()
    func, to be displayed through HTML, when no data can be queried due to
    an error 
    """

    standard_args = dict(
        currentWD=currentWD,
        valid=False,
        DB_USER_INPUT=DB_USER_INPUT
    )

    return standard_args


DB_USER_INPUT_previous = ""


@app.route('/executed', methods=['POST'])
def executeSQL():
    global SQL_COMMAND
    global DB_USER_INPUT
    global DB_USER_INPUT_previous
    global INIT_DB_USER_INPUT

    if INIT_DB_USER_INPUT == False:
        print("INIT 2: ", INIT_DB_USER_INPUT)
        DB_USER_INPUT_previous = DB_USER_INPUT

    SQL_COMMAND = request.form['textarea']

    if INIT_DB_USER_INPUT == False:
        if request.form.get('dbPathForm') != "":
            DB_USER_INPUT = request.form.get('dbPathForm')
            print("New DB user input used.")
        else:
            print("User input empty, hence using old one.")

    if INIT_DB_USER_INPUT == True:
        print("very first cmd execution: ")
        DB_USER_INPUT = request.form.get('dbPathForm')
        INIT_DB_USER_INPUT = False

    print("excuted SQL DB_USER_INPUT: ", DB_USER_INPUT)
    print("cmd:", SQL_COMMAND)

    if SQL_COMMAND:
        try:
            std_args, df = STD_FUNC_TRUE(dbpath=DB_USER_INPUT)

            return render_template(
                'mainpage.html',
                SQL_COMMAND=SQL_COMMAND,
                **std_args)
        except sqlite3.OperationalError:
            # If error in sql comand
            print("error in the SQL command")
            print(traceback.format_exc())
            std_args = STD_FUNC_FALSE()

            return render_template('mainpage.html',
                                   SQL_COMMAND=SQL_COMMAND,
                                   **std_args)
    else:
        print("Empty command")
        std_args = STD_FUNC_FALSE()

        return render_template('mainpage.html',
                               SQL_COMMAND=SQL_COMMAND,
                               **std_args)


@app.route('/csv', methods=['POST'])
def generateCSV():

    CSVname_user = request.form.get('CSVname_user')
    print(CSVname_user)
    std_args, df = STD_FUNC_TRUE(dbpath=DB_USER_INPUT, CSVSaveMode=True)
    toCSVinChunks(CSVname=CSVname_user)

    return render_template('mainpage.html',
                           SQL_COMMAND=SQL_COMMAND,
                           **std_args)


if __name__ == '__main__':
    app.run(debug=True)
