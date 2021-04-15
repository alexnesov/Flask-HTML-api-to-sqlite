# import the Flask class from the flask module
from flask import Flask, render_template, request, Response
import sqlite3
import pandas as pd
import os


app = Flask(__name__, static_url_path='/static')

sql_output = []
SQL_COMMAND = ""
currentWD = os.path.dirname(__file__)  # WD = working directory
pathToDB = os.path.join(currentWD, "utils\\marketdataSQL.db")
print("here: ", pathToDB)


def readSqlite(SQL_COMMAND, path=pathToDB):
    # conn = sqlite3.connect('utils/marketdataSQL.db')
    conn = sqlite3.connect(f'{pathToDB}')

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
                           pathToDB=pathToDB,
                           valid=True)


# to do: init command to avoid red arrow


@app.route('/', methods=['POST'])
def executeSQL():
    global SQL_COMMAND
    global sql_output

    SQL_COMMAND = request.form['textarea']

    if SQL_COMMAND:
        try:
            sql_output, colNames = readSqlite(SQL_COMMAND=SQL_COMMAND)
            valid = True
            widthDF = list(range(len(colNames)))
            return render_template(
                'mainpage.html',
                SQL_COMMAND=SQL_COMMAND,
                sql_output=sql_output,
                colNames=colNames,
                widthDF=widthDF,
                valid=valid,
                currentWD=currentWD,
                pathToDB=pathToDB)
        except sqlite3.OperationalError:
            # If error in sql comand
            print("error in the SQL command")
            valid = False
            return render_template('mainpage.html',
                                   valid=valid,
                                   currentWD=currentWD,
                                   pathToDB=pathToDB,
                                   SQL_COMMAND=SQL_COMMAND)
    else:
        print("Empty command")
        valid = False
        return render_template('mainpage.html',
                               valid=valid,
                               currentWD=currentWD,
                               pathToDB=pathToDB,
                               SQL_COMMAND=SQL_COMMAND)


@app.route('/getCSV')
def test():

    print("HERE")
    sql_output, colNames = readSqlite(SQL_COMMAND=SQL_COMMAND)
    df = pd.DataFrame(sql_output)
    widthDF = list(range(len(colNames)))

    print("command: ", SQL_COMMAND)

    print(df)
    try:
        df.to_csv(f"{currentWD}\\test.csv")
    except PermissionError:
        print(
            'File used by another person, yourself, or simply not authorized to overwrite')

    return render_template('mainpage.html',
                           currentWD=currentWD,
                           pathToDB=pathToDB,
                           valid=True,
                           SQL_COMMAND=SQL_COMMAND,
                           sql_output=sql_output,
                           colNames=colNames,
                           widthDF=widthDF)


@app.route("/getCSV", methods=['POST'])
def getCSV():
    global sql_output
    global SQL_COMMAND
    print('NOW')
    SQL_COMMAND = request.form['textarea']

    if SQL_COMMAND:
        try:
            sql_output, colNames = readSqlite(SQL_COMMAND=SQL_COMMAND)
            valid = True
            widthDF = list(range(len(colNames)))
            return render_template(
                'mainpage.html',
                SQL_COMMAND=SQL_COMMAND,
                sql_output=sql_output,
                colNames=colNames,
                widthDF=widthDF,
                valid=valid,
                currentWD=currentWD,
                pathToDB=pathToDB)
        except sqlite3.OperationalError:
            # If error in sql comand
            print("error in the SQL command")
            valid = False
            return render_template('mainpage.html',
                                   valid=valid,
                                   currentWD=currentWD,
                                   pathToDB=pathToDB,
                                   SQL_COMMAND=SQL_COMMAND)
    else:
        print("Empty command")
        valid = False
        return render_template('mainpage.html',
                               valid=valid,
                               currentWD=currentWD,
                               pathToDB=pathToDB,
                               SQL_COMMAND=SQL_COMMAND)


if __name__ == '__main__':
    app.run(debug=True)
