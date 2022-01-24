from flask.logging import create_logger
from src import app
from src import db_handler
from flask import request
import logging
import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.DEBUG)
log = create_logger(app)


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
    client_id = json_data['job_name']
    source_broker = json_data['source_broker']
    topic = json_data['topic']
    sink_broker = json_data['sink_broker']

    args = {'client_id': client_id,
            'source_broker': source_broker,
            'topic': topic,
            'sink_broker': sink_broker
            }

    client = mqtt.Client(client_id, userdata=args, clean_session=False)
    client.connect(source_broker)
    client.subscribe(topic, qos=1)
    clients = db_handler.get_db('clients.db')
    clients[client_id] = args
    clients.close()

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    client.loop_start()
    return {'message': 'Success'}, 200


# show list of clients
@app.route('/clients', methods=['GET'])
def list_client():
    stuff = db_handler.list_db('clients.db')
    return {'message': 'Success', 'data': stuff}, 200


@app.route('/delete_client/<client_id>', methods=['GET'])
def delete_client(client_id):
    clients = db_handler.get_db('clients.db')
    if not (client_id in clients):
        return {'message': 'Client not found', 'data': {}}, 404
    client = mqtt.Client(client_id, clean_session=False)
    broker = clients[client_id]['source_broker']
    client.connect(broker)
    client.on_disconnect = on_disconnect
    client.loop_stop()
    del clients[client_id]
    clients.close()
    return {'message': 'Success'}, 200
