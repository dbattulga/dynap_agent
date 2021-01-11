import json
import requests
import os
import shelve

from flask import g

# jobs.db structure: 
## 'pipeline_name': 'first_pipe', 
## 'job_name': 'A_job', 
## 'agent_address': 'http://10.188.166.99', 
## 'source_broker': 'tcp://10.188.166.99', 
## 'sink_broker': 'tcp://10.188.166.99', 
## 'source_topic': 'T-1', 
## 'sink_topic': 'T-2', 
## 'entry_class': 'flinkpackage.OperatorStreamOne'
## 'job_path': internal docker cntainer path with job name
## 'jarid': gibberish id returned from SPE
## 'jobid': gibberish id returned from SPE



def get_db(dbname):
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = shelve.open(dbname)
    return db


def teardown_db(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()


def list_db(dbname):
    db = get_db(dbname)
    keys = list(db.keys())
    stuff = []
    for key in keys:
        stuff.append(db[key])
    db.close()
    return stuff
