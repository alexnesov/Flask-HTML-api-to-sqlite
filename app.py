# import the Flask class from the flask module
from flask import Flask, render_template, request, Response
import sqlite3
import pandas as pd
import os

from utils import csv_util

app = Flask(__name__, static_url_path='/static')

sql_output = []
currentWD = os.path.dirname(__file__)


def readSqlite(SQL_COMMAND, path='utils/marketdataSQL.db'):
    conn = sqlite3.connect('utils/marketdataSQL.db')
    mycur = conn.cursor()
    mycur.execute(f"{SQL_COMMAND}")
    sql_output = (mycur.fetchall())
    colNames = list(map(lambda x: x[0], mycur.description))

    conn.close()

    return sql_output, colNames


@app.route('/')
def home():
    return render_template('mainpage.html')


@app.route("/getCSV", methods=['GET'])
def getCSV():

    df = pd.DataFrame(sql_output)
    print(df)

    try:
        df.to_csv(f"{currentWD}\\test.csv")
    except PermissionError:
        print(
            'File used by another person, yourself, or simply not authorized to overwrite')

    return Response(
        df,
        mimetype="text/csv",
        headers={"Content-disposition":
                 "attachment; filename=output.csv"})


# to do: init command to avoid red arrow

@app.route('/', methods=['POST'])
def executeSQL():
    SQL_COMMAND = request.form['textarea']

    if SQL_COMMAND:
        try:
            global sql_output
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
                currentWD=currentWD)
        except sqlite3.OperationalError:
            # If error in sql comand
            print("error")
            valid = False
            return render_template('mainpage.html',
                                   valid=valid,
                                   SQL_COMMAND=SQL_COMMAND)
    else:
        print("Empty command")
        valid = False
        return render_template('mainpage.html',
                               valid=valid,
                               SQL_COMMAND=SQL_COMMAND)


if __name__ == '__main__':
    app.run(debug=True)
