""" Idea: simple web interface to

- upload files
- upload file batches
- update database
- get iventory
- update inventory"""
from flask import Flask, render_template

app = Flask(__name__)


@app.route('/')
def main():
    return render_template('main.html', title='foo')


if __name__ == '__main__':
    app.run(port=7770, debug=True)
