import markdown
import os
import shelve
import logging
import json
import uuid

from flask import Flask, g, send_file, redirect, render_template, url_for
from flask_restful import Resource, Api, reqparse
from flask import request
from src import spe_handler
from src import db_handler

logging.basicConfig(level=logging.DEBUG)
app = Flask(__name__)
api = Api(app)

job_path = '/usr/src/app/jars'
spe_port = '8081'
broker_port = '1883'

 # app.logger.warning('testing warning log')
 # app.logger.error('testing error log')
 # app.logger.info('testing info log')

@app.route("/")
def index():
    """Present some documentation"""
    with open(os.path.dirname(app.root_path) + '/README.md', 'r') as markdown_file:
        content = markdown_file.read()
        return markdown.markdown(content)


# receives an uploaded file with json body
# json body consisnts of pipeline_name, job_name, agent_address, source_broker, sink_broker, source_topic, sink_topic, entry_class
# filename - unique name for saving jars, saved into /usr/src/app/jars relative path inside docker container
# parse uploaded_data into another local request to Flink JM and save to DB
@app.route('/upload', methods=['GET', 'POST'])
def receive_file():
    if request.method == 'POST':
        uploaded_file = request.files['jar']
        uploaded_data = json.load(request.files['data'])
        filename = str(uuid.uuid4())+'.jar'
        uploaded_file.save(os.path.join(job_path, filename))
        # start the job
        msg = start_job(uploaded_data, filename)
        return msg
    else:
        return '404'


daats = {'pipeline_name': 'first_pipe', 
        'job_name': 'A_job', 
        'agent_address': 'http://10.188.166.99', 
        'source_broker': 'tcp://10.188.166.99', 
        'sink_broker': 'tcp://10.188.166.99', 
        'source_topic': 'T-1', 
        'sink_topic': 'T-2', 
        'entry_class': 'flinkpackage.OperatorStreamOne'
        }


@app.route('/show', methods=['GET'])
def list_job():
    stuff = db_handler.list_db('jobs.db')
    return {'message': 'Success', 'data': stuff}, 200


@app.route('/delete', methods=['GET'])
def delete_job():
    shelf = db_handler.get_db('jobs.db')
    key = 'B_job'
    if not (key in shelf):
        return {'message': 'Job not found', 'data': {}}, 404
    host = shelf[key]['agent_address'] + ':' + spe_port
    spe_handler.delete_jar(host, shelf[key]['jarid'])
    spe_handler.stop_job(host, shelf[key]['jobid'])
    if os.path.exists(job_path+'/'+shelf[key]['filename']):
        os.remove(job_path+'/'+shelf[key]['filename'])
    del shelf[key]
    shelf.close()
    return '200'


# integrate port numbers with corresponding addresses
# flink address with 8081, mqtt with 1883
@app.route('/start', methods=['GET'])
def start_job(args, filename):
    spe_addr = args['agent_address'] + ':' + spe_port
    full_path = job_path + '/' + filename
    entry_class = args['entry_class']
    job_name = args['job_name']
    source_broker = args['source_broker'] + ':' + broker_port
    sink_broker = args['sink_broker'] + ':' + broker_port
    source_topic = args['source_topic']
    sink_topic = args['sink_topic']

    jarid = spe_handler.upload_jar(spe_addr, full_path)
    jobid = spe_handler.start_jar(spe_addr, jarid, entry_class, source_broker, sink_broker, source_topic, sink_topic, job_name)
    args['filename'] = filename
    args['jarid'] = jarid
    args['jobid'] = jobid
    app.logger.info(args)
    # save to db
    shelf = db_handler.get_db('jobs.db')
    shelf[args['job_name']] = args
    shelf.close()
    return '200'

