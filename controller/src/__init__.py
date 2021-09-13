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
from flask.logging import create_logger
from src import spe_handler, db_handler, metrics_handler
from flask import jsonify

logging.basicConfig(level=logging.DEBUG)
app = Flask(__name__)
api = Api(app)
log = create_logger(app)

mutex = Lock()
job_path = '/usr/src/app/jars'
spe_port = '8081'
broker_port = '1883'

# log.debug('A debug message')
# log.error('An error message')

@app.route("/")
def index():
    """Present some documentation"""
    with open(os.path.dirname(app.root_path) + '/README.md', 'r') as markdown_file:
        content = markdown_file.read()
        return markdown.markdown(content)


# receives an uploaded file with json body
# json body consists of pipeline_name, job_name, agent_address, source_broker, sink_broker, source_topic, sink_topic, entry_class
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
        log.debug(msg)
        return msg
    else:
        return {'message': 'Failed'}, 500


# client request for uploading and starting a SPE job
# WARNING: Do not send already running job to its own instance!
# it will delete the running job with a same name
@app.route('/send/<url>/<job>', methods=['GET'])
def send_file(url, job):
    shelf = db_handler.get_db('jobs.db')
    key = job
    base_url = "http://"+url
    source_broker = shelf[key]['source_broker']
    sink_broker = "tcp://"+url+":"+broker_port

    body = {'pipeline_name': shelf[key]['pipeline_name'],
            'job_name': shelf[key]['job_name'],
            'agent_address': base_url,
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
    req = requests.post(base_url + ":5001/upload", files=files)
    if req.status_code == 200:
    #if req == '200':
        delete_job(key)
    return {'message': 'Success'}, 200


# show list of jobs
# ONLY FOR DEBUGGING
@app.route('/jobs', methods=['GET'])
def list_job():
    stuff = db_handler.list_db('jobs.db')
    return {'message': 'Success', 'data': stuff}, 200


# delete job, jobname = key
@app.route('/delete/<job>', methods=['GET'])
def delete_job(job):
    shelf = db_handler.get_db('jobs.db')
    jobname = job
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


# server side function to start job by submitting a jar to local SPE
# integrate port number with corresponding address
# flink address with 8081
@app.route('/start', methods=['GET'])
def start_job(args, filename):
    spe_addr = args['agent_address'] + ':' + spe_port
    full_path = job_path + '/' + filename
    entry_class = args['entry_class']
    job_name = args['job_name']
    source_broker = args['source_broker']
    sink_broker = args['sink_broker']
    source_topic = args['source_topic']
    sink_topic = args['sink_topic']

    jarid = spe_handler.upload_jar(spe_addr, full_path)
    jobid = spe_handler.start_jar(spe_addr, jarid, entry_class, source_broker, sink_broker, source_topic, sink_topic, job_name)
    args['filename'] = filename
    args['jarid'] = jarid
    args['jobid'] = jobid
    args['job_path'] = full_path
    log.debug(jobid)
    # save to db
    # send downstream_job info to source_addr
    shelf = db_handler.get_db('jobs.db')
    shelf[args['job_name']] = args
    shelf.close()
    return {'message': 'Job Started'}, 200

# @app.route('/save-ds/<url>/<job>', methods=['GET'])
# def save_ds(url, job):
#     # save to db
#     shelf = db_handler.get_db('jobs.db')
#     shelf[args['job_name']] = args
#     shelf.close()
#     return {'message': 'Job Started'}, 200


# handshake response
@app.route('/syn_response', methods=['GET'])
def syn_response():
    url = request.remote_addr
    base_url = 'http://'+url
    available_taskslots = int(metrics_handler.get_available_task_slots(base_url))

    #mutex.acquire()
    shelf = db_handler.get_db('state.db')
    if not ('state' in shelf):
        shelf['state'] = 0
    state = shelf['state']

    if available_taskslots - state > 0:
        #time.sleep(10)
        state += 1
        shelf['state'] = state
        shelf.close()
        #mutex.release()
        return {'message': 'Success', 'data': state}, 200

    shelf.close()
    #mutex.release()
    return {'message': 'Failed', 'data': state}, 500


# check number of current connections
@app.route('/check_connections', methods=['GET'])
def check_connections():
    shelf = db_handler.get_db('state.db')
    if not ('state' in shelf):
        shelf['state'] = 0
    state = shelf['state']
    shelf.close()
    return 'current_connections:' + str(state)


# clear current connections
# ONLY FOR DEBUGGING
@app.route('/clear_connections', methods=['GET'])
def clear_connections():
    shelf = db_handler.get_db('state.db')
    shelf['state'] = 0
    shelf.close()
    return 'state cleared'


# remember, the key id is just an interface
# handshake request
@app.route('/syn_request/<url>/<job>', methods=['GET'])
def syn_request(targeturl, job):
    res = requests.get("http://"+targeturl+ ":5001/syn_response")
    #log.debug('RES STATUS CODE: '+str(res.status_code))
    if res.status_code == 200:
        # send restart request also to the downstream
        log.debug('DEPLOYING migration')
        deploy = send_file(targeturl, job)
        log.debug(deploy)
        # if successful: order a restart
        # restart client will check the downstream topic is empty
        return {'message': 'Success'}, 200
    return {'message': 'Failed'}, 500


# handshake request
@app.route('/restart/<job>', methods=['GET'])
def restart_job(job):
    # log.debug('STARTED SLEEPING ' + job)
    # time.sleep(10)
    # log.debug('FINISHED SLEEPING ' + job)
    # shelf = db_handler.get_db('state.db')
    # state = shelf['state']
    # if state != 0:
    #     state = state - 1
    #     shelf['state'] = state
    # shelf.close()

    # get the specific job with id, it will request restart from local SPE
    url = request.remote_addr
    base_url = 'http://'+url+":"+spe_port
    # shelf = db_handler.get_db('jobs.db')
    # key = job
    # base_url = "http://"+url
    # source_broker = shelf[key]['source_broker']
    # sink_broker = "tcp://"+url+":"+broker_port

    # body = {'pipeline_name': shelf[key]['pipeline_name'],
    #         'job_name': shelf[key]['job_name'],
    #         'agent_address': base_url,
    #         'source_broker': source_broker,
    #         'sink_broker': sink_broker,
    #         'source_topic': shelf[key]['source_topic'],
    #         'sink_topic': shelf[key]['sink_topic'],
    #         'entry_class': shelf[key]['entry_class']
    #     }
    # shelf.close()
    #spe_handler.start_job(base_url, jobid, jarid, entryclass, sourcemqtt, sinkmqtt, sourcetopic, sinktopic, jobname)
    #spe_handler.restart_job(base_url, jobid, jarid, entryclass, sourcemqtt, sinkmqtt, sourcetopic, sinktopic, jobname)

    return {'message': 'Deployed'}, 200


# request_stat response
@app.route('/stat_response', methods=['GET'])
def stat_response():
    return jsonify(
        message='Success', 
        data='coolcoolcoolnodoubtnodoubt'
    )
    #return {'message': 'Success', 'data': 'coolcoolcoolnodoubtnodoubt'}, 200