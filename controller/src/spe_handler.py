import json
import requests
import os


#if successful: returns jarid, else returns "failed"
def upload_jar(base_url, jarpath):
    files = {"jarfile": ( os.path.basename(jarpath), open(jarpath, "rb"), "application/x-java-archive")}
    upload = requests.post(base_url + "/jars/upload", files=files)
    if (upload.ok):
        response = json.loads(upload.content)
        if "filename" in response:
            return get_upload_id(response["filename"])

    upload.raise_for_status()
    return upload.status_code

def find_all(a_str, sub):
    start = 1
    while True:
        start = a_str.find(sub, start)
        if start == -1: return
        yield start
        start += len(sub) # use start += 1 to find overlapping matches

def get_upload_id(str):
    lst = list(find_all(str, '/'))
    return (str[lst[-1]+1:])

# starts job with parameters, successful: returns jobid else "failed"
def start_jar(base_url, jarid, entryclass, sourcemqtt, sinkmqtt, sourcetopic, sinktopic, jobname):
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


def delete_jar(base_url, jarid):
    delete = requests.delete(base_url + "/jars/" +jarid)
    if (delete.ok):
        response = json.loads(delete.content)
    else:
        delete.raise_for_status()
    return delete.status_code


def stop_job(base_url, jobid):
    stop = requests.patch(base_url + "/jobs/" + jobid)
    if (stop.ok):
        response = json.loads(stop.content)
    else:
        stop.raise_for_status()
    return stop.status_code