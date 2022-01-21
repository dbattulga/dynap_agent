import json
import logging
import os
import uuid
from threading import Lock

import markdown
import requests
from flask import Flask, request, jsonify
from flask.logging import create_logger
from flask_restful import Api

from src import db_handler
from src import metrics_handler
from src import spe_handler
import paho.mqtt.client as mqtt


logging.basicConfig(level=logging.DEBUG)
app = Flask(__name__)
api = Api(app)
log = create_logger(app)

from src import test_routes


mutex = Lock()
job_path = '/usr/src/app/jars'
spe_port = '8081'
broker_port = '1883'
shelf = {}
clients = {}


@app.route("/")
def index():
    with open(os.path.dirname(app.root_path) + '/README.md', 'r') as markdown_file:
        content = markdown_file.read()
        return markdown.markdown(content)


@app.route('/upload', methods=['GET', 'POST'])
def receive_job():
    if request.method == 'POST':
        uploaded_file = request.files['jar']
        uploaded_data = json.load(request.files['data'])
        filename = str(uuid.uuid4()) + '.jar'
        uploaded_file.save(os.path.join(job_path, filename))
        # start the job
        msg = start_job(uploaded_data, filename)
        log.debug(msg)
        return msg
    else:
        return {'message': 'Failed'}, 500


@app.route('/start', methods=['GET'])
def start_job(args, filename):
    spe_addr = 'http://' + args['agent_address'] + ':' + spe_port
    full_path = job_path + '/' + filename
    entry_class = args['entry_class']
    job_name = args['job_name']
    broker = args['agent_address']
    source_topic = args['source_topic']
    sink_topic = args['sink_topic']
    jarid = spe_handler.upload_jar(spe_addr, full_path)
    jobid = spe_handler.start_jar(spe_addr, jarid, entry_class, broker, source_topic, sink_topic, job_name)
    args['filename'] = filename
    args['jarid'] = jarid
    args['jobid'] = jobid
    args['job_path'] = full_path
    log.debug(jobid)
    # send request to upstream (clients to start)
    upstreams = args['source_broker']
    for i in range(len(upstreams)):
        client_id = args['job_name']+"_source_"+args['source_topic'][i]
        log.debug("starting "+client_id+" on "+args['source_broker'][i])
        json_data = {
            "job_name": args['job_name'],
            "source_broker": args['source_broker'][i],
            "topic": args['source_topic'][i],
            "sink_broker": args['sink_broker'][i]
        }
        req = requests.get("http://" + args['source_broker'][i] + ":5001/create_client", json=json_data)
        log.debug(req.text)
    #shelf = db_handler.get_db('jobs.db')
    shelf[args['job_name']] = args
    #shelf.close()
    return {'message': 'Job Started'}, 200


@app.route('/send/<url>/<job>', methods=['GET'])
def send_job(url, job):
    #shelf = db_handler.get_db('jobs.db')
    if not (job in shelf):
        return {'message': 'Job not found', 'data': {}}, 404
    upstreams = shelf[job]['source_broker']
    for i in range(len(upstreams)):
        client_id = shelf[job]['job_name']+"_source_"+shelf[job]['source_topic'][i]
        log.debug("deleting "+client_id+" on "+shelf[job]['source_broker'][i])
        req = requests.get("http://" + shelf[job]['source_broker'][i] + ":5001/delete_client/"+client_id)
        log.debug(req.text)

    stop_job(job) #stop request to flink
    body = {'pipeline_name': shelf[job]['pipeline_name'],
            'job_name': shelf[job]['job_name'],
            'agent_address': url,
            'source_broker': shelf[job]['source_broker'],
            'sink_broker': shelf[job]['sink_broker'],
            'source_topic': shelf[job]['source_topic'],
            'sink_topic': shelf[job]['sink_topic'],
            'entry_class': shelf[job]['entry_class']
            }

    files = [
        ('jar', ('test.jar', open(shelf[job]['job_path'], 'rb'), 'application/octet')),
        ('data', ('data', json.dumps(body), 'application/json')),
    ]
    req = requests.post("http://" + url + ":5001/upload", files=files)
    if req.status_code == 200:
        downstreams = shelf[job]['sink_broker']
        for i in range(len(downstreams)):
            json_data = {
                "job_name": shelf[job]['job_name'],
                "update_source_broker": url
            }
            req = requests.get("http://" + shelf[job]['sink_broker'][i] + ":5001/update_downstream", json=json_data)
            log.debug(req.text)
        delete_job(job) #delete from DB
    return {'message': 'Success'}, 200


@app.route('/jobs', methods=['GET'])
def list_job():
    #stuff = db_handler.list_db('jobs.db')
    stuff = shelf
    return {'message': 'Success', 'data': stuff}, 200


@app.route('/list_upstream/<job>', methods=['GET'])
def list_upstream(job):
    #stuff = db_handler.get_db('jobs.db')
    stuff = shelf
    if not (job in stuff):
        return {'message': 'Job not found', 'data': {}}, 404
    upstreams = stuff[job]['source_broker']
    return {'message': 'Success', 'data': upstreams}, 200


@app.route('/update_downstream', methods=['GET'])
def update_downstream():
    json_data = request.json
    job_name = json_data['job_name']
    topic = json_data['topic']
    # TODO send topic and address
    topic = "T-1"
    updated_address = json_data['update_source_broker']
    # receive TOPIC and upstream ADDRESS
    # look through the db,
    # if any of the source topics match with TOPIC
    # edit the source broker address with ADDRESS
    #stuff = db_handler.get_db('jobs.db')
    for job in shelf:
        for i in range(len(shelf[job]['source_topic'])):
            if topic == shelf[job]['source_topic'][i]:
                shelf[job]['source_broker'][i] = updated_address
    for job in shelf:
        log.debug(shelf[job]['source_broker'])
    return {'message': 'Success'}, 200


@app.route('/list_downstream/<job>', methods=['GET'])
def list_downstream(job):
    #stuff = db_handler.get_db('jobs.db')
    stuff = shelf
    if not (job in stuff):
        return {'message': 'Job not found', 'data': {}}, 404
    downstreams = stuff[job]['sink_broker']
    for downstream in downstreams:
        log.debug(downstream)
    return {'message': 'Success', 'data': downstreams}, 200


@app.route('/delete/<job>', methods=['GET'])
def delete_job(job):
    #shelf = db_handler.get_db('jobs.db')
    if not (job in shelf):
        return {'message': 'Job not found', 'data': {}}, 404
    if os.path.exists(shelf[job]['job_path']):
        os.remove(shelf[job]['job_path'])
    del shelf[job] #check if it's deleting all db
    #shelf.close()
    return {'message': 'Success'}, 200


@app.route('/stop/<job>', methods=['GET'])
def stop_job(job):
    #shelf = db_handler.get_db('jobs.db')
    if not (job in shelf):
        return {'message': 'Job not found', 'data': {}}, 404
    host = 'http://' + shelf[job]['agent_address'] + ':' + spe_port
    spe_handler.delete_jar(host, shelf[job]['jarid'])
    spe_handler.stop_job(host, shelf[job]['jobid'])
    return {'message': 'Success'}, 200


def on_message(client, userdata, message):
    msg = "message received: " + str(message.payload.decode("utf-8"))
    pub_client = mqtt.Client("pub_"+userdata["client_id"], clean_session=True)
    pub_client.connect(userdata["sink_broker"])
    pub_client.publish(topic=userdata["topic"], payload=str(message.payload.decode("utf-8")))
    #pub_client.disconnect()
    log.debug(msg)


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.connected_flag = True
    else:
        log.debug("Bad connection Returned code=" + rc)
        client.loop_stop()


def on_disconnect(client, userdata, rc):
    log.debug("client disconnected ok")
    client.loop_stop()


# create mqtt client
@app.route('/create_client', methods=['GET'])
def create_client():
    json_data = request.json
    job_name = json_data['job_name']
    source_broker = json_data['source_broker']
    topic = json_data['topic']
    sink_broker = json_data['sink_broker']
    client_id = job_name+"_source_"+topic

    args = {'client_id': client_id,
            'source_broker': source_broker,
            'topic': topic,
            'sink_broker': sink_broker
        }

    client = mqtt.Client(client_id, userdata=args, clean_session=False)
    client.connect(source_broker)
    client.subscribe(topic, qos=1)
    #shelf = db_handler.get_db('clients.db')
    #shelf[client_id] = args
    clients[client_id] = args
    #shelf.close()

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    client.loop_start()
    return {'message': 'Success'}, 200


# show list of clients
@app.route('/clients', methods=['GET'])
def list_client():
    #stuff = db_handler.list_db('clients.db')
    stuff = clients
    return {'message': 'Success', 'data': stuff}, 200


@app.route('/delete_client/<client_id>', methods=['GET'])
def delete_client(client_id):
    #shelf = db_handler.get_db('clients.db')
    if not (client_id in clients):
        return {'message': 'Client not found', 'data': {}}, 404
    client = mqtt.Client(client_id, clean_session=False)
    broker = clients[client_id]['source_broker']
    client.connect(broker)
    client.on_disconnect = on_disconnect
    client.loop_stop()
    del clients[client_id]
    #shelf.close()
    return {'message': 'Success'}, 200


############################################################################################################
# Not using functions beyond this point
# handshake response
@app.route('/syn_response', methods=['GET'])
def syn_response():
    url = request.remote_addr
    base_url = 'http://' + url
    available_taskslots = int(metrics_handler.get_available_task_slots(base_url))

    # mutex.acquire()
    shelf = db_handler.get_db('state.db')
    if not ('state' in shelf):
        shelf['state'] = 0
    state = shelf['state']

    if available_taskslots - state > 0:
        # time.sleep(10)
        state += 1
        shelf['state'] = state
        shelf.close()
        # mutex.release()
        return {'message': 'Success', 'data': state}, 200

    shelf.close()
    # mutex.release()
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
    res = requests.get("http://" + targeturl + ":5001/syn_response")
    # log.debug('RES STATUS CODE: '+str(res.status_code))
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
    base_url = 'http://' + url + ":" + spe_port
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
    # spe_handler.start_job(base_url, jobid, jarid, entryclass, sourcemqtt, sinkmqtt, sourcetopic, sinktopic, jobname)
    # spe_handler.restart_job(base_url, jobid, jarid, entryclass, sourcemqtt, sinkmqtt, sourcetopic, sinktopic, jobname)

    return {'message': 'Deployed'}, 200


# response to request_stat
@app.route('/stat_response', methods=['GET'])
def stat_response():
    return jsonify(
        message='Success',
        data='cool cool cool no doubt no doubt'
    )
