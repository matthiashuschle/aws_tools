""" Idea: simple web interface to

- upload files
- upload file batches
- update database
- get iventory
- update inventory

may be of use: https://ourcodeworld.com/articles/read/146/top-5-best-tree-view-jquery-and-javascript-plugins
"""
from flask import Flask, render_template

app = Flask(__name__)


@app.route('/')
def main():
    return render_template('main.html', title='foo')


if __name__ == '__main__':
    app.run(port=7770, debug=True)
