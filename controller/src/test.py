import json
import requests
import os
import shelve
import datetime
import time
#from flask import g
import socket
#from netifaces import interfaces, ifaddresses, AF_INET

#from flask import Flask, g, send_file, redirect, render_template, url_for
#from flask_restful import Resource, Api, reqparse
#from flask import request
#import spe_handler, db_handler, metrics_handler

def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
        IP = '172.22.0.1'
    finally:
        s.close()
    return IP

#s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
#s.connect(("8.8.8.8", 80))
#print(s.getsockname()[0])
#s.close()

#for ifaceName in interfaces():
#    addresses = [i['addr'] for i in ifaddresses(ifaceName).setdefault(AF_INET, [{'addr':'No IP addr'}] )]
#    print ('%s: %s' % (ifaceName, ', '.join(addresses)))
#print(get_ip())

def request():
    key = '192.168.2.147'
    url = 'http://'+key
    res = requests.get(url + ":5001/hs_request/" + key)
    return res

def requestt():
    key = '192.168.2.147'
    url = 'http://'+key
    res = requests.get(url + ':5001/pseudo_handshake')
    return res._content

def start_jar():
    jobname = "A_job"
    sourcemqtt = ["tcp://192.168.2.147:1883", "tcp://192.168.2.147:1883", "tcp://192.168.2.147:1883"]
    sinkmqtt = "tcp://192.168.2.147:1883"
    sinktopic = ["T1", "T2"]
    sourcetopic = ["T3", "T4"]
    
    sourcebrokers = ','.join(sourcemqtt)
    sourcetopics = ','.join(sourcetopic)
    sinktopics = ','.join(sinktopic)
    sinkbroker = sinkmqtt
    programArgs = "--jobname "+jobname+" --sourcemqtt "+sourcebrokers+" --sinkmqtt "+sinkbroker+" --sourcetopic "+sourcetopics+" --sinktopic "+sinktopics
    return programArgs

def start_jarr():
    
    base_url = "http://192.168.2.147:8081"
    jarid = "29166309-6b43-4555-8e67-326f497a9dfc_aa6024f5-9b11-407b-b414-bea3bc847b05.jar"
    entryclass = "flinkpackage.MultiSourceTest"
    jobname = "A_job"
    sourcemqtt = ["tcp://192.168.2.147:1883", "tcp://192.168.2.147:1883", "tcp://192.168.2.147:1883"]
    sinkmqtt = "tcp://192.168.2.147:1883"
    sinktopic = ["T1", "T2"]
    sourcetopic = ["T3", "T4"]

    sourcebrokers = ','.join(sourcemqtt)
    sourcetopics = ','.join(sourcetopic)
    sinktopics = ','.join(sinktopic)
    sinkbroker = sinkmqtt
    programArgs = "--jobname '"+jobname+"' --sourcemqtt '"+sourcebrokers+"' --sinkmqtt '"+sinkbroker+"' --sourcetopic '"+sourcetopics+"' --sinktopic '"+sinktopics+"' "
    propertiess = {
        "entryClass": entryclass,
        "programArgs": programArgs
    }
    start = requests.post(base_url + "/jars/"+jarid+"/run", json=propertiess)

    if (start.ok):
        response = json.loads(start.content)
        if "jobid" in response:
            return response["jobid"]
    start.raise_for_status()
    return start.status_code

def test_clients():
    downstreams = shelf[job]['sink_broker']
    clients = db_handler.get_db('clients.db')
    for i in range(len(downstreams)):
        job_topic = shelf[job]['sink_topic'][i]
        for client in clients:
            client_topic = client['topic']
            if client_topic == job_topic:
                client_id = client['client_id']
                log.debug("deleting "+client_id+" on "+shelf[job]['agent_address'])
                req = requests.get("http://" + shelf[job]['agent_address'] + ":5001/delete_client/"+client_id)
                log.debug("starting "+client_id+" on "+url)
                json_data = {
                    "client_id": client_id,
                    "source_broker": url,
                    "topic": client_topic,
                    "sink_broker": shelf[job]['sink_broker'][i]
                }
                req = requests.get("http://" + url + ":5001/create_client", json=json_data)
                log.debug(req.text)

print( start_jarr() )