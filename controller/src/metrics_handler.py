import json
import requests
import os
import shelve

from flask import g


def get_metrics():
    response = requests.get('http://localhost:9090/api/v1/query', params={'query': "query=container_cpu_load_average_10s{container_name=POD}"})

    print(response.json()['data']['result'])