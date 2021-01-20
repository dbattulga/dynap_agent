import markdown
import os
import shelve
import logging
import json
import uuid
import requests
import socket
import time
from threading import Lock

from flask import Flask, g, redirect, render_template, url_for
from flask_restful import Resource, Api, reqparse
from flask import request
from src import spe_handler, db_handler, metrics_handler

logging.basicConfig(level=logging.DEBUG)
app = Flask(__name__)
api = Api(app)

#mutex = Lock()

job_path = '/usr/src/app/jars'
spe_port = '8081'
broker_port = '1883'

# app.logger.warning('testing warning log')
# app.logger.error('testing error log')
# app.logger.info('testing info log')

# example data format
#  daats = {'pipeline_name': 'first_pipe', 
#         'job_name': 'A_job', 
#         'agent_address': 'http://10.188.166.99', 
#         'source_broker': 'tcp://10.188.166.99', 
#         'sink_broker': 'tcp://10.188.166.99', 
#         'source_topic': 'T-1', 
#         'sink_topic': 'T-2', 
#         'entry_class': 'flinkpackage.OperatorStreamOne'
#         }

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


# WARNING: Do not send already running job to its own instance!
# it will delete the running job with a same name
@app.route('/send', methods=['GET'])
def send_file():
    shelf = db_handler.get_db('jobs.db')
    key = 'A_job'
    #url = shelf[key]['agent_address']
    url = 'http://10.188.150.130'
    source_broker = shelf[key]['source_broker']
    sink_broker = shelf[key]['sink_broker']

    body = {'pipeline_name': shelf[key]['pipeline_name'],
            'job_name': shelf[key]['job_name'],
            'agent_address': url,
            'source_broker': source_broker,
            'sink_broker': sink_broker,
            'source_topic': shelf[key]['source_topic'],
            'sink_topic': shelf[key]['sink_topic'],
            'entry_class': shelf[key]['entry_class']
        }
        
    files = [
            ('jar', ('test.jar', open(shelf[key]['job_path'], 'rb'), 'application/octet')),
            ('data', ('data', json.dumps(body), 'application/json')),
        ]
    req = requests.post(url + ":5001/upload", files=files)
    if req == '200':
        delete_job(key)
    return '200'


# show list of jobs
@app.route('/jobs', methods=['GET'])
def list_job():
    stuff = db_handler.list_db('jobs.db')
    return {'message': 'Success', 'data': stuff}, 200


# delete job, jobname = key
@app.route('/delete/<jobname>', methods=['GET'])
def delete_job(jobname):
    shelf = db_handler.get_db('jobs.db')
    #jobname = 'A_job'
    if not (jobname in shelf):
        return {'message': 'Job not found', 'data': {}}, 404
    host = shelf[jobname]['agent_address'] + ':' + spe_port
    spe_handler.delete_jar(host, shelf[jobname]['jarid'])
    spe_handler.stop_job(host, shelf[jobname]['jobid'])
    if os.path.exists(shelf[jobname]['job_path']):
        os.remove(shelf[jobname]['job_path'])
    del shelf[jobname]
    shelf.close()
    return 'deleted'


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
    args['job_path'] = full_path
    app.logger.info(args)
    # save to db
    shelf = db_handler.get_db('jobs.db')
    shelf[args['job_name']] = args
    shelf.close()
    return '200'


# handshake response
@app.route('/check_available', methods=['GET'])
def check_available():
    url = request.remote_addr
    base_url = 'http://'+url
    available_taskslots = int(metrics_handler.get_available_task_slots(base_url))
    #mutex.acquire()
    #app.logger.info(mutex)
    shelf = db_handler.get_db('state.db')
    if not ('state' in shelf):
        shelf['state'] = 0
    state = shelf['state']
    if available_taskslots - state > 0:
        #app.logger.info('SLEEPING BEFORE MODIFICATION')
        #time.sleep(10)
        #app.logger.info('FINISHED SLEEPING MODIFICATION')
        shelf['state'] = state + 1
        shelf.close()
        #mutex.release()
        return '200' #available for connection

    shelf.close()
    #mutex.release()
    return '500' #not available for more jobs


# check number of current connections
@app.route('/check_state', methods=['GET'])
def check_state():
    shelf = db_handler.get_db('state.db')
    if not ('state' in shelf):
        shelf['state'] = 0
    state = shelf['state']
    shelf.close()
    return 'current_connections:' + str(state)


# clear current connections
@app.route('/clear_state', methods=['GET'])
def clear_state():
    shelf = db_handler.get_db('state.db')
    shelf['state'] = 0
    shelf.close()
    return 'state cleared'


# handshake request
@app.route('/hs_request/<key>', methods=['GET'])
def hs_request(key):
    url = 'http://'+key
    res = requests.get(url + ":5001/check_available")
    if str(res.status_code) == '200':
        app.logger.info('DEPLOYING PSEUDO DEPLOY FUNCTION')
        deploy = requests.get(url + ":5001/pseudo_deploy")
        app.logger.info(deploy)
        return 'she said yes and migrated'
    return 'she said no :('


# handshake request
@app.route('/pseudo_deploy', methods=['GET'])
def pseudo_deploy():
    app.logger.info('STARTED SLEEPING')
    time.sleep(30)
    app.logger.info('FINISHED SLEEPING')
    shelf = db_handler.get_db('state.db')
    state = shelf['state']
    if state != 0:
        shelf['state'] = state - 1
    shelf.close()
    return 'deployed'


# handshake response
@app.route('/pseudo_handshake', methods=['GET'])
def pseudo_handshake():
    url = request.remote_addr
    base_url = 'http://'+url
    available_taskslots = int(metrics_handler.get_available_task_slots(base_url))
    
    shelf = db_handler.get_db('state.db')
    if not ('state' in shelf):
        shelf['state'] = 0
    state = shelf['state']

    if available_taskslots - state > 0:
        state += 1
        shelf['state'] = state
        shelf.close()
        return {'message': 'Success', 'data': state}, 200

    shelf.close()
    return {'message': 'Failed', 'data': state}, 500