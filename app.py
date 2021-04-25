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


chunk_size = 300000


# To: Add traceback to sql error
# Nb lines total and preview warning
def getNRows(dbCursor, SQL_COMMAND):
    """
    Returns an integer, being the number of rows for the last query executed
    by the user
    """

    """
    try:
        table = SQL_COMMAND.split("FROM")[1].split(" ")[1]
    except IndexError:
        table = SQL_COMMAND.split("from")[1].split(" ")[1]
    """

    dbCursor.execute(f"select count(*) from ({SQL_COMMAND})")
    nRows = dbCursor.fetchall()[0][0]

    return nRows


def toCSVinChunks(dbpath, SQL_COMMAND, CSVname):
    """
    Parameters
    ----------
    dbpath : string
        Path to db to query
    SQL_COMMAND : string
        User input, being the SQL command to execute
    Returns
    -------
    None.
    """
    if CSVname == "":
        CSVname = "sqloutput"

    print("dbpath: ", dbpath)
    conn = sqlite3.connect(f'{dbpath}')
    dbCursor = conn.cursor()
    nRows = getNRows(dbCursor, SQL_COMMAND)

    try:
        if nRows < 600000:
            dbCursor.execute(f"{SQL_COMMAND}")

            colNames = list(map(lambda x: x[0], dbCursor.description))
            result = dbCursor.fetchmany(chunk_size)

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
                result = dbCursor.fetchmany(chunk_size)
        else:
            print("Number exceeds reasonnable capacity for a CSV format (>600000 rows). Please refine the query (group the data or aggregate it) \
                  to obtain a lighter output.")
    except PermissionError:
        print(
            "File used by another person, yourself, or simply not authorized to overwrite")

    conn.commit()
    conn.close()


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

    # Getting the name of the table in the query, to be able to place it in
    # the coming select count(*) n rows

    nRows = getNRows(dbCursor, SQL_COMMAND)
    print("nRows: ", nRows)

    dbCursor.execute(f"{SQL_COMMAND}")

    if nRows > 500:
        sql_output = dbCursor.fetchmany(500)
    else:
        sql_output = dbCursor.fetchall()

    colNames = list(map(lambda x: x[0], dbCursor.description))

    conn.commit()
    conn.close()

    return sql_output, colNames, nRows


@app.route('/')
def home():
    return render_template('mainpage.html',
                           currentWD=currentWD,
                           DB_USER_INPUT=DB_USER_INPUT,
                           valid=True)
# to do: init command to avoid red arrow


def STD_FUNC_TRUE(dbpath):
    """
    Set of standard variables to be passed inside each render_template()
    func, to be displayed through HTML
    """
    global INIT_DB_USER_INPUT

    sql_output, colNames, nRows = readSqlite(dbpath, SQL_COMMAND=SQL_COMMAND)
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
    global sql_output
    global DB_USER_INPUT
    global DB_USER_INPUT_previous
    global INIT_DB_USER_INPUT

    print('-------------------------')
    print("INIT 1: ", INIT_DB_USER_INPUT)

    if INIT_DB_USER_INPUT == False:
        print("INIT 2: ", INIT_DB_USER_INPUT)
        DB_USER_INPUT_previous = DB_USER_INPUT

    SQL_COMMAND = request.form['textarea']

    if INIT_DB_USER_INPUT == False:
        if request.form.get('dbPathForm') != "":
            DB_USER_INPUT = request.form.get('dbPathForm')
            print("new DB user input used")
        else:
            print("user input empty, hence using old one")

    if INIT_DB_USER_INPUT == True:
        print("very first: ")
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
    std_args, df = STD_FUNC_TRUE(dbpath=DB_USER_INPUT)
    toCSVinChunks(DB_USER_INPUT, SQL_COMMAND, CSVname=CSVname_user)

    return render_template('mainpage.html',
                           SQL_COMMAND=SQL_COMMAND,
                           **std_args)


if __name__ == '__main__':
    app.run(debug=True)
