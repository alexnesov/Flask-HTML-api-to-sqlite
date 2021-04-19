"""
Author: Alexandre Nesovic
"""
from flask import Flask, render_template, request
import sqlite3
import pandas as pd
import os
import time

app = Flask(__name__, static_url_path='/static')

sql_output = []
SQL_COMMAND = ""
DB_USER_INPUT = ""
INIT = True
currentWD = os.path.dirname(__file__)  # WD = working directory


chunk_size = 300000


# To: Add traceback to sql error

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


def toCSVinChunks(dbpath, SQL_COMMAND, ):
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
    fileNameforSave = "sqliteoutput"
    
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
                if init==True:
                    print("Saving as CSV. . .")
                    print(df_result)
                    df_result.to_csv(f'{currentWD}/{fileNameforSave}.csv', index=False)
                    init = False
                else:
                    print("Saving as CSV. . .")
                    df_result.to_csv(f'{currentWD}/{fileNameforSave}.csv', mode='a', header=False,
                              index=False)
                
                del df_result # liberating memory because of potentially big data
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
    Set of standard variables to be passed inside each render_template()
    func, to be displayed through HTML
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


@app.route('/executed', methods=['POST'])
def executeSQL():
    global SQL_COMMAND
    global sql_output
    global DB_USER_INPUT
    global INIT

    SQL_COMMAND = request.form['textarea']

    if INIT == True:
        print('init true')
        DB_USER_INPUT = request.form.get('dbPathForm')

    print('DB user input:', DB_USER_INPUT)
    print("cmd:", SQL_COMMAND)

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


@app.route('/executed')
def generateCSV():

    std_args, df = STD_FUNC_TRUE(dbpath=DB_USER_INPUT)
    toCSVinChunks(DB_USER_INPUT, SQL_COMMAND)


    return render_template('mainpage.html',
                           SQL_COMMAND=SQL_COMMAND,
                           **std_args)




if __name__ == '__main__':
    app.run(debug=True)