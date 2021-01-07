import markdown
import os
import shelve
import logging
import json
import uuid

from flask import Flask, g, send_file, redirect, render_template, url_for
from flask_restful import Resource, Api, reqparse
from flask import request
from src import job_handler
from werkzeug.utils import secure_filename

logging.basicConfig(level=logging.DEBUG)
app = Flask(__name__)
api = Api(app)

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = shelve.open("jobs.db")
    return db

@app.teardown_appcontext
def teardown_db(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

@app.route("/")
def index():
    """Present some documentation"""
    # Open the README file
    with open(os.path.dirname(app.root_path) + '/README.md', 'r') as markdown_file:
        # Read the content of the file
        content = markdown_file.read()
        # Convert to HTML
        return markdown.markdown(content)


# receives an uploaded file with json body
# filename - unique name for saving jars, saved into /usr/src/app/jars relative path inside docker
# parse uploaded_data into another local request to Flink JM
# and save to DB
@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        uploaded_file = request.files['jar']
        uploaded_data = json.load(request.files['data'])
        filename = str(uuid.uuid4())+'.jpg'
        uploaded_file.save(os.path.join('/usr/src/app/jars', filename))
        app.logger.info(filename)
        app.logger.info(uploaded_data)
        return '200'
    else:
        return '404'

