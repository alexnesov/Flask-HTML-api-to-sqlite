# import the Flask class from the flask module
from flask import Flask, render_template, request, Response, redirect, url_for
import sqlite3
import pandas as pd
import os


app = Flask(__name__, static_url_path='/static')

sql_output = []
SQL_COMMAND = ""
DB_USER_INPUT = ""
INIT = True
currentWD = os.path.dirname(__file__)  # WD = working directory
#pathToDB = os.path.join(currentWD, "utils\\marketdataSQL.db")


def readSqlite(dbpath, SQL_COMMAND):

    conn = sqlite3.connect(f'{dbpath}')
    mycur = conn.cursor()
    mycur.execute(f"{SQL_COMMAND}")
    sql_output = (mycur.fetchall())
    colNames = list(map(lambda x: x[0], mycur.description))

    conn.close()

    return sql_output, colNames


@app.route('/')
def home():
    return render_template('mainpage.html',
                           currentWD=currentWD,
                           DB_USER_INPUT=DB_USER_INPUT,
                           valid=True)
# to do: init command to avoid red arrow


def STD_FUNC_TRUE(dbpath):
    """
    """

    sql_output, colNames = readSqlite(dbpath, SQL_COMMAND=SQL_COMMAND)
    widthDF = list(range(len(colNames)))
    df = pd.DataFrame(sql_output)

    standard_args = dict(
        sql_output=sql_output,
        colNames=colNames,
        widthDF=widthDF,
        valid=True,
        currentWD=currentWD,
        DB_USER_INPUT=DB_USER_INPUT
    )

    return standard_args, df


def STD_FUNC_FALSE():

    standard_args = dict(
        currentWD=currentWD,
        valid=False,
        DB_USER_INPUT=DB_USER_INPUT
    )

    return standard_args


@app.route('/executed', methods=['POST'])
def executeSQL():
    global SQL_COMMAND
    global sql_output
    global DB_USER_INPUT
    global INIT

    SQL_COMMAND = request.form['textarea']

    print('user input:', DB_USER_INPUT)
    print("cmd:", SQL_COMMAND)

    if request.method == "POST":
        if INIT == True:
            print('init true')
            DB_USER_INPUT = request.form.get('dbPathForm')

        if SQL_COMMAND:
            try:
                std_args, df = STD_FUNC_TRUE(dbpath=DB_USER_INPUT)
                INIT = False

                return render_template(
                    'mainpage.html',
                    SQL_COMMAND=SQL_COMMAND,
                    **std_args)
            except sqlite3.OperationalError:
                # If error in sql comand
                print("error in the SQL command")
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
    else:
        return render_template('mainpage.html',
                               currentWD=currentWD,
                               DB_USER_INPUT=DB_USER_INPUT,
                               valid=True)


@app.route('/executed')
def generateCSV():

    print("HERE")
    std_args, df = STD_FUNC_TRUE(dbpath=DB_USER_INPUT)
    print(df)
    try:
        df.to_csv(f"{currentWD}\\test.csv")
    except PermissionError:
        print(
            'File used by another person, yourself, or simply not authorized to overwrite')

    return render_template('mainpage.html',
                           SQL_COMMAND=SQL_COMMAND,
                           **std_args)


if __name__ == '__main__':
    app.run(debug=True)
