""" Idea: simple web interface to

- upload files
- upload file batches
- update database
- get iventory
- update inventory

may be of use: https://ourcodeworld.com/articles/read/146/top-5-best-tree-view-jquery-and-javascript-plugins
"""
import os
import logging
from collections import namedtuple
import urllib.parse
from flask import Flask, render_template, request, url_for
from jinja2 import Template

app = Flask(__name__)


PathElement = namedtuple('PathElement', ['name', 'isdir', 'fullpath', 'extension'])


TPL_JQUERYFILETREE = Template('''\
<ul class="jqueryFileTree" style="display: none;">
{% for element in elements %}
{% if element.isdir %}
<li class="directory collapsed">{{checkbox}}<a rel="{{element.fullpath}}/">{{element.name}}</a></li>
{% else %}
<li class="file ext_{{element.extension}}">{{checkbox}}<a rel="{{element.fullpath}}">{{element.name}}</a></li>
{% endif %}
{% endfor %}
{% if alert %}
{{ alert }}
{% endif %}
</ul>
''')


def render_nav(template, **kwargs):
    if 'nav' not in kwargs:
        kwargs['nav'] = [
            {'target': url_for('main'), 'text': 'Projects'},
            {'target': 'new_project', 'text': 'New Project'},
            {'target': 'inventory', 'text': 'Inventory'},
        ]
    return render_template(template, **kwargs)


@app.route('/')
def main():
    return render_nav('main.html', title='foo')


@app.route('/jqueryfiletree', methods=['POST'])
def handle_jqueryfiletree():
    elements = []
    alert = None
    folder_raw = request.form.get('dir', 'c:\\temp')
    folder = None
    checkbox = '<input type="checkbox">' if request.form.get('multiSelect') == 'true' else ''
    only_files = request.form.get('onlyFiles') == 'true'
    only_folders = request.form.get('onlyFolders') == 'true'
    try:
        folder = urllib.parse.unquote(folder_raw)
        for f in os.listdir(folder):
            ff = os.path.join(folder, f)
            if os.path.isdir(ff) and not only_files:
                elements.append(PathElement(f, True, ff, None))
            elif not (os.path.isdir(ff) or only_folders):
                e = os.path.splitext(f)[1][1:]  # extension w/o dot
                elements.append(PathElement(f, False, ff, e))
    except OSError as exc:
        logging.exception(repr(exc))
        alert = 'Could not load directory: %s' % str(folder or folder_raw)
    return TPL_JQUERYFILETREE.render(elements=elements, alert=alert, checkbox=checkbox)


if __name__ == '__main__':
    app.run(port=7770, debug=True)
