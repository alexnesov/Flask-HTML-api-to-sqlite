# import the Flask class from the flask module
from flask import Flask, render_template, request


"""
# Minimum reproductible example

# Two questions:

1. Should we call function outside route functions? so that we have them available 
mumtiple route function?
If no, what is the other better solution?

2. How to use the same arguments without repeting them?
(i. e passing same argument to multile function --> render_template())
"""
app = Flask(__name__,static_url_path='/static')


def dotest():
    one = 'one'
    print('one')
    return one


def dotest2():
    two = 'two'
    print('two')
    return two

def dotest3():
    three = 'three'
    print('three')
    return three

one = str(dotest())
two = dotest2()
three = dotest3()

args=dict(one=one, two=two, three=three)
SQL_COMMAND = ''

@app.route('/')
def home():
    print(SQL_COMMAND)
    return render_template('welcome.html', SQL_COMMAND=SQL_COMMAND, **args)  


######---Get text from textbox---######
@app.route('/', methods=['POST'])
def my_form_post():
    global SQL_COMMAND
    SQL_COMMAND = request.form['text']
    print(SQL_COMMAND)
    return render_template('welcome.html', SQL_COMMAND=SQL_COMMAND, **args) 
######--------------------######


@app.route('/test_one')
def welcome():
    print('bar')
    return render_template('test_one.html',**args)

if __name__ == '__main__':
    app.run(debug=True)