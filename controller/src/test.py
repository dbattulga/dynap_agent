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
    res = requests.get(url + ":5001/pseudo_deploy")
    return res

print( request())